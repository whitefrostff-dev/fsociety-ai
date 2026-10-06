from fastapi import FastAPI, Form, File, UploadFile, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
from starlette.middleware.sessions import SessionMiddleware
from typing import Optional, List
import uuid
import os
import re
import io
import httpx
import json
import time
import asyncio
from datetime import datetime
from pydantic import BaseModel
from groq import Groq
from google import genai
from google.genai import types
from authlib.integrations.starlette_client import OAuth
import psycopg2
import psycopg2.extras

try:
    from pypdf import PdfReader
    HAS_PYPDF = True
except ImportError:
    HAS_PYPDF = False

try:
    from ddgs import DDGS
    HAS_DDGS = True
except ImportError:
    HAS_DDGS = False

app = FastAPI()

# --- SEARCH CACHE ---
search_cache = {}
CACHE_TTL = 3600
search_lock = asyncio.Lock()

def get_cached_search(query: str, search_type: str = "text"):
    cache_key = f"{search_type}:{query.strip().lower()}"
    if cache_key in search_cache:
        data, timestamp = search_cache[cache_key]
        if time.time() - timestamp < CACHE_TTL:
            return data
    return None

def set_cached_search(query: str, results, search_type: str = "text"):
    cache_key = f"{search_type}:{query.strip().lower()}"
    search_cache[cache_key] = (results, time.time())

# --- POSTGRESQL ---
DATABASE_URL = os.environ.get("DATABASE_URL")

def get_db_connection():
    if DATABASE_URL:
        try:
            return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
        except Exception as e:
            print(f"Database connection error: {e}")
    return None

def init_db():
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS user_chats (
                        user_email TEXT,
                        chat_id TEXT,
                        title TEXT,
                        messages JSONB,
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        PRIMARY KEY (user_email, chat_id)
                    );
                    CREATE TABLE IF NOT EXISTS gems (
                        id SERIAL PRIMARY KEY,
                        user_email TEXT,
                        name TEXT,
                        description TEXT,
                        system_prompt TEXT,
                        icon TEXT DEFAULT 'fa-robot'
                    );
                    CREATE TABLE IF NOT EXISTS assets (
                        id SERIAL PRIMARY KEY,
                        user_email TEXT,
                        file_name TEXT,
                        file_path TEXT,
                        file_type TEXT
                    );
                    CREATE TABLE IF NOT EXISTS usage_limits (
                        identifier TEXT,
                        usage_date DATE,
                        message_count INTEGER DEFAULT 0,
                        PRIMARY KEY (identifier, usage_date)
                    );
                """)
                conn.commit()
            conn.close()
            print("PostgreSQL tables initialized successfully.")
        except Exception as e:
            print(f"Error initializing PostgreSQL tables: {e}")
            if conn:
                conn.close()

init_db()

# --- FILE STORAGE ---
DATA_DIR = os.getenv("DATABASE_DIR", "/data" if os.path.exists("/data") else "./data")
os.makedirs(DATA_DIR, exist_ok=True)
CHATS_FILE = os.path.join(DATA_DIR, "chats.json")

def load_local_chats():
    if os.path.exists(CHATS_FILE):
        try:
            with open(CHATS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_local_chats(data):
    # Atomic write so a crash mid-write can't corrupt chats.json
    try:
        tmp_path = CHATS_FILE + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f)
        os.replace(tmp_path, CHATS_FILE)
    except Exception as e:
        print(f"Error saving to disk: {e}")

UPLOAD_DIR = os.path.join(DATA_DIR, "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)
app.mount("/uploads", StaticFiles(directory=UPLOAD_DIR), name="uploads")

# --- FAVICON ---
def _find_favicon_path():
    for path in [os.path.join("..", "app", "favicon.png"), "favicon.png"]:
        if os.path.exists(path):
            return path
    return None

@app.get("/favicon.png")
async def favicon():
    path = _find_favicon_path()
    if path:
        return Response(content=open(path, "rb").read(), media_type="image/png")
    return Response(status_code=204)

@app.get("/favicon.ico")
async def favicon_ico():
    path = _find_favicon_path()
    if path:
        return Response(content=open(path, "rb").read(), media_type="image/png")
    return Response(status_code=204)

# --- SESSION / PROXY ---
# SECURITY: set SESSION_SECRET as an env var on your host (any long random string).
SESSION_SECRET = os.getenv("SESSION_SECRET", "ranen_super_secret_session_string")
app.add_middleware(ProxyHeadersMiddleware, trusted_hosts="*")
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET, https_only=False, same_site="lax")

@app.middleware("http")
async def ensure_guest_cookie(request: Request, call_next):
    existing_guest_id = request.cookies.get("guest_id")
    new_guest_id = None
    if not existing_guest_id:
        new_guest_id = str(uuid.uuid4())
        request.state.guest_id = new_guest_id
    else:
        request.state.guest_id = existing_guest_id

    response = await call_next(request)
    if new_guest_id:
        response.set_cookie(key="guest_id", value=new_guest_id, max_age=31536000, httponly=True)
    return response

# --- ENV ---
# .strip() fixes the classic bug of a stray space/quote/newline pasted into the key
GROQ_API_KEY = (os.getenv("GROQ_API_KEY") or "").strip().strip('"').strip("'")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
GOOGLE_API_KEY = (os.getenv("GOOGLE_API_KEY") or "").strip().strip('"').strip("'")
GITHUB_CLIENT_ID = os.getenv("GITHUB_CLIENT_ID", "")
GITHUB_CLIENT_SECRET = os.getenv("GITHUB_CLIENT_SECRET", "")

groq_client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None
genai_client = genai.Client(api_key=GOOGLE_API_KEY) if GOOGLE_API_KEY else None

print(f"[STARTUP] Groq key set: {bool(GROQ_API_KEY)} | Google key set: {bool(GOOGLE_API_KEY)}")

# --- MODEL CHAINS ---
# GROQ IS PRIMARY for every text request. Each Groq model has its own quota,
# so if one is rate-limited the next is tried before Gemini is ever touched.
# Override with env vars (comma-separated) without editing code.
GROQ_MODELS = [m.strip() for m in os.getenv(
    "GROQ_MODELS", "openai/gpt-oss-120b,llama-3.3-70b-versatile,openai/gpt-oss-20b,llama-3.1-8b-instant"
).split(",") if m.strip()]

# Gemini: images (vision) + last-resort fallback for text.
GEMINI_MODELS = [m.strip() for m in os.getenv(
    "GEMINI_MODELS", "gemini-3.5-flash,gemini-2.5-flash-lite"
).split(",") if m.strip()]

def _model_chain(provider: str):
    return GROQ_MODELS if provider == "groq" else GEMINI_MODELS

# --- OAUTH ---
oauth = OAuth()
oauth.register(
    name='google',
    client_id=GOOGLE_CLIENT_ID,
    client_secret=GOOGLE_CLIENT_SECRET,
    server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
    client_kwargs={'scope': 'openid email profile'}
)

class StepSyncRequest(BaseModel):
    steps: int

def get_identifier(request: Request):
    user = request.session.get('user')
    if user and user.get('email'):
        return ("user_email", user['email'])
    guest_id = getattr(request.state, "guest_id", None) or request.cookies.get("guest_id")
    return ("guest_id", guest_id if guest_id else "unknown_guest")

def get_identifier_ws(websocket) -> str:
    try:
        session_user = websocket.session.get('user') if hasattr(websocket, 'session') else None
    except Exception:
        session_user = None
    if session_user and session_user.get('email'):
        return session_user['email']
    guest_id = websocket.cookies.get("guest_id")
    return guest_id if guest_id else "unknown_guest"

# --- DAILY MESSAGE LIMIT ---
DAILY_MESSAGE_LIMIT = int(os.getenv("DAILY_MESSAGE_LIMIT", "20"))
_local_usage_counts = {}

def _today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")

def get_today_usage(identifier: str) -> int:
    today = _today_str()
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT message_count FROM usage_limits WHERE identifier = %s AND usage_date = %s",
                    (identifier, today)
                )
                row = cur.fetchone()
                conn.close()
                return row["message_count"] if row else 0
        except Exception as e:
            print(f"USAGE FETCH ERROR: {e}")
            if conn:
                conn.close()
    return _local_usage_counts.get((identifier, today), 0)

def increment_today_usage(identifier: str):
    today = _today_str()
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO usage_limits (identifier, usage_date, message_count)
                    VALUES (%s, %s, 1)
                    ON CONFLICT (identifier, usage_date)
                    DO UPDATE SET message_count = usage_limits.message_count + 1;
                """, (identifier, today))
                conn.commit()
            conn.close()
            return
        except Exception as e:
            print(f"USAGE INCREMENT ERROR: {e}")
            if conn:
                conn.close()
    key = (identifier, today)
    _local_usage_counts[key] = _local_usage_counts.get(key, 0) + 1

def under_daily_limit(identifier: str) -> bool:
    return get_today_usage(identifier) < DAILY_MESSAGE_LIMIT

def check_and_increment_usage(identifier: str) -> bool:
    # Used by Live Call (one call = one unit)
    if not under_daily_limit(identifier):
        return False
    increment_today_usage(identifier)
    return True

# --- FILE TEXT EXTRACTION ---
MAX_FILE_CHARS = 12000

def extract_pdf_text(file_bytes: bytes) -> str:
    if not HAS_PYPDF:
        return "[PDF text extraction unavailable — `pypdf` is not installed on the server]"
    try:
        reader = PdfReader(io.BytesIO(file_bytes))
        pages_text = []
        for page in reader.pages:
            try:
                pages_text.append(page.extract_text() or "")
            except Exception:
                continue
        text = "\n".join(pages_text).strip()
        if not text:
            return "[PDF appears to be scanned/image-based — no extractable text found. OCR would be needed.]"
        return text
    except Exception as e:
        print(f"PDF extraction error: {e}")
        return "[Could not extract text from this PDF — it may be corrupted or encrypted]"

def extract_file_text(file_bytes: bytes, filename: str, mime_type: str) -> str:
    is_pdf = (mime_type == "application/pdf") or filename.lower().endswith(".pdf")
    if is_pdf:
        text = extract_pdf_text(file_bytes)
    else:
        try:
            text = file_bytes.decode('utf-8', errors='ignore')
        except Exception:
            text = "[Binary or unreadable file content]"
    if len(text) > MAX_FILE_CHARS:
        text = text[:MAX_FILE_CHARS] + f"\n\n[... truncated — file was longer than {MAX_FILE_CHARS} characters ...]"
    return text

# --- DB HELPERS ---
def save_chat_history(user_email: str, chat_id: str, title: str, messages: list):
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO user_chats (user_email, chat_id, title, messages)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (user_email, chat_id)
                    DO UPDATE SET title = EXCLUDED.title, messages = EXCLUDED.messages;
                """, (user_email, str(chat_id), title, json.dumps(messages)))
                conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"DATABASE UPSERT ERROR: {e}")
            if conn:
                conn.close()

    local_chats = load_local_chats()
    if user_email not in local_chats:
        local_chats[user_email] = {}
    local_chats[user_email][str(chat_id)] = {"title": title, "messages": messages}
    save_local_chats(local_chats)
    return True

# --- ROUTES ---
@app.get("/google0b211ab21a1539ad.html", response_class=HTMLResponse)
async def google_verification():
    return "google-site-verification: google0b211ab21a1539ad.html"

@app.get("/sitemap.xml", response_class=Response)
async def sitemap():
    sitemap_content = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
    <url><loc>https://ranen.duckdns.org/</loc><changefreq>daily</changefreq><priority>1.0</priority></url>
    <url><loc>https://ranen.duckdns.org/terms</loc><changefreq>monthly</changefreq><priority>0.3</priority></url>
    <url><loc>https://ranen.duckdns.org/privacy</loc><changefreq>monthly</changefreq><priority>0.3</priority></url>
    <url><loc>https://ranen.duckdns.org/about</loc><changefreq>monthly</changefreq><priority>0.6</priority></url>
</urlset>"""
    return Response(content=sitemap_content, media_type="application/xml")

@app.get("/", response_class=HTMLResponse)
async def serve_frontend(request: Request):
    html_path = os.path.join("..", "app", "index.html")
    if not os.path.exists(html_path):
        html_path = "index.html"

    content = "<h3>index.html not found.</h3>"
    if os.path.exists(html_path):
        with open(html_path, "r", encoding="utf-8") as f:
            content = f.read()

    response = HTMLResponse(content=content)
    if not request.cookies.get("guest_id"):
        guest_id = str(uuid.uuid4())
        response.set_cookie(key="guest_id", value=guest_id, max_age=31536000, httponly=True)
    return response

LEGAL_PAGE_STYLE = """
    <style>
        body{background:#050505;color:#e5e7eb;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;padding:0;margin:0;line-height:1.7;}
        .wrap{max-width:760px;margin:0 auto;padding:48px 24px 80px;}
        h1{font-size:1.8rem;margin-bottom:4px;}
        .updated{color:#9ca3af;font-size:0.8rem;margin-bottom:32px;}
        h2{font-size:1.15rem;margin-top:32px;margin-bottom:10px;color:#f9fafb;border-bottom:1px solid #262626;padding-bottom:6px;}
        p, li{color:#d1d5db;font-size:0.92rem;}
        ul{padding-left:20px;}
        a{color:#60a5fa;text-decoration:none;}
        a:hover{text-decoration:underline;}
        .contact-box{background:#111827;border:1px solid #262626;border-radius:12px;padding:18px 20px;margin-top:8px;}
        .contact-box p{margin:4px 0;}
        .back{display:inline-block;margin-bottom:24px;color:#9ca3af;font-size:0.85rem;}
    </style>
"""

CONTACT_BLOCK = """
    <div class="contact-box">
        <p><b>Email:</b> <a href="mailto:whitefrostff@gmail.com">whitefrostff@gmail.com</a></p>
        <p><b>Phone / WhatsApp:</b> <a href="tel:+2347077187114">+234 707 718 7114</a></p>
    </div>
"""

@app.get("/terms", response_class=HTMLResponse)
async def terms_page():
    return f"""
    <html><head><title>Terms of Service - Ranen</title>{LEGAL_PAGE_STYLE}</head>
    <body><div class="wrap">
    <a href="/" class="back">&larr; Back to Ranen</a>
    <h1>Terms of Service</h1>
    <div class="updated">Last updated: 2026</div>

    <p>These Terms govern your use of Ranen ("the Service"), an AI assistant platform built and operated by Nwodili Yaemerie Convenant. By using Ranen, you agree to these Terms. If you do not agree, please do not use the Service.</p>

    <h2>1. Acceptable Use</h2>
    <ul>
        <li>Do not use the Service for malicious activity, including malware creation, prompt injection attacks, or attempts to bypass safety systems.</li>
        <li>Do not use the Service to generate illegal content, including content that exploits or endangers minors, facilitates violence, or violates the rights of others.</li>
        <li>Do not attempt to reverse-engineer, scrape, or overload the Service's infrastructure.</li>
        <li>Do not use the Service to impersonate real people or organizations in a misleading or harmful way.</li>
    </ul>

    <h2>2. AI Output Disclaimer</h2>
    <p>Ranen is powered by third-party large language models (Groq and Google Gemini). Outputs may contain errors, outdated information, or hallucinations. Do not rely on Ranen as a substitute for professional medical, legal, financial, or safety-critical advice. Always verify important information independently.</p>

    <h2>3. Accounts &amp; Guest Access</h2>
    <p>You may use Ranen as a signed-in user (via Google OAuth) or as an anonymous guest. Guest sessions are tied to a browser cookie and are not guaranteed to persist indefinitely. You are responsible for safeguarding access to your account.</p>

    <h2>4. Content Ownership</h2>
    <p>You retain ownership of the messages, files, and content you submit to Ranen. We retain ownership of the platform itself — its design, code, branding, and user interface. By uploading files or content, you confirm you have the right to share that content and to have it processed by the third-party AI providers listed above.</p>

    <h2>5. File Uploads</h2>
    <p>Uploaded files are stored to provide chat continuity and may be processed by third-party AI providers to generate responses. Do not upload sensitive personal documents (IDs, financial statements, medical records) unless necessary, as Ranen is not a certified secure storage system.</p>

    <h2>6. Service Availability</h2>
    <p>The Service is provided "as is" and "as available," without warranties of any kind. We do not guarantee uninterrupted uptime, and third-party AI providers may experience outages, rate limits, or degraded performance outside of our control.</p>

    <h2>7. Limitation of Liability</h2>
    <p>To the maximum extent permitted by law, Nwodili Yaemerie Convenant shall not be liable for any indirect, incidental, or consequential damages arising from your use of the Service, including but not limited to data loss, service downtime, or reliance on AI-generated content.</p>

    <h2>8. Changes to These Terms</h2>
    <p>These Terms may be updated periodically as the Service evolves. Continued use of Ranen after changes are posted constitutes acceptance of the revised Terms.</p>

    <h2>9. Termination</h2>
    <p>We reserve the right to suspend or terminate access to the Service for any user found violating these Terms, without prior notice.</p>

    <h2>Contact</h2>
    <p>Questions about these Terms? Reach out directly:</p>
    {CONTACT_BLOCK}
    </div></body></html>
    """

@app.get("/privacy", response_class=HTMLResponse)
async def privacy_page():
    return f"""
    <html><head><title>Privacy Policy - Ranen</title>{LEGAL_PAGE_STYLE}</head>
    <body><div class="wrap">
    <a href="/" class="back">&larr; Back to Ranen</a>
    <h1>Privacy Policy</h1>
    <div class="updated">Last updated: 2026</div>

    <p>This Privacy Policy explains what data Ranen collects, how it is used, and your choices regarding that data.</p>

    <h2>1. Data We Collect</h2>
    <ul>
        <li><b>Chat content:</b> messages you send, and any files or images you upload.</li>
        <li><b>Account info:</b> if you sign in with Google, we receive your name and email address via OAuth.</li>
        <li><b>Guest identifiers:</b> a random cookie ID for anonymous sessions, with no personal info attached.</li>
        <li><b>Basic technical logs:</b> connection metadata (e.g., timestamps, error logs) used for debugging and abuse prevention.</li>
    </ul>

    <h2>2. How We Use Your Data</h2>
    <ul>
        <li>To generate AI responses to your messages.</li>
        <li>To maintain chat history so conversations persist across sessions.</li>
        <li>To improve reliability and fix bugs.</li>
    </ul>

    <h2>3. Third-Party AI Providers</h2>
    <p>Your prompts, uploaded files, and images are sent to third-party AI providers (Groq and Google Gemini) to generate responses. Each provider processes this data under its own privacy and data-handling terms. We do not control how these providers internally process requests beyond what their APIs document.</p>

    <h2>4. Data Storage</h2>
    <p>Chat history and uploaded files are stored on our servers (PostgreSQL database and file storage) to provide session continuity. This data persists until you delete a chat or request deletion of your account data.</p>

    <h2>5. Data We Do Not Sell</h2>
    <p>We do not sell your personal data, chat histories, or uploaded files to advertisers or data brokers.</p>

    <h2>6. Your Choices</h2>
    <ul>
        <li>You can delete individual chats at any time from the sidebar.</li>
        <li>You can use Ranen as a guest without creating an account.</li>
        <li>You can request full deletion of your stored data by contacting us (see below).</li>
    </ul>

    <h2>7. Children's Privacy</h2>
    <p>Ranen is not directed at children under 13, and we do not knowingly collect personal data from children under 13.</p>

    <h2>8. Changes to This Policy</h2>
    <p>This Privacy Policy may be updated as the Service evolves. Material changes will be reflected by updating the "Last updated" date above.</p>

    <h2>Contact</h2>
    <p>For privacy questions, data deletion requests, or anything else:</p>
    {CONTACT_BLOCK}
    </div></body></html>
    """

@app.get("/about", response_class=HTMLResponse)
async def about_page():
    direct_answer = (
        "Nwodili Yaemerie Convenant is an 18-year-old cybersecurity student and web developer "
        "from Anambra State, Nigeria, studying at Abia State University. He is the creator of "
        "Ranen, an AI assistant platform."
    )
    did_make_answer = (
        "Yes. Nwodili Yaemerie Convenant is the creator of Ranen, an AI assistant platform that "
        "lets users chat, generate code, analyze images and documents, and search the web in real time."
    )
    what_is_ranen_answer = (
        "Ranen is an AI assistant platform built by Nwodili Yaemerie Convenant. It lets users chat "
        "with multiple AI models, generate and edit code, analyze uploaded images and documents, "
        "search the web for current information, and make live voice calls to the assistant."
    )
    return f"""
    <html><head>
    <title>Who is Nwodili Yaemerie Convenant? — Creator of Ranen</title>
    <meta name="description" content="{direct_answer}">
    <link rel="canonical" href="https://ranen.duckdns.org/about">
    <script type="application/ld+json">
    {{
      "@context": "https://schema.org",
      "@type": "FAQPage",
      "mainEntity": [
        {{
          "@type": "Question",
          "name": "Who is Nwodili Yaemerie Convenant?",
          "acceptedAnswer": {{ "@type": "Answer", "text": "{direct_answer}" }}
        }},
        {{
          "@type": "Question",
          "name": "Did Nwodili Yaemerie Convenant make Ranen?",
          "acceptedAnswer": {{ "@type": "Answer", "text": "{did_make_answer}" }}
        }},
        {{
          "@type": "Question",
          "name": "What is Ranen?",
          "acceptedAnswer": {{ "@type": "Answer", "text": "{what_is_ranen_answer}" }}
        }}
      ]
    }}
    </script>
    {LEGAL_PAGE_STYLE}
    <style>
        .profile-header {{ display:flex; align-items:center; gap:16px; margin-bottom:24px; }}
        .profile-avatar {{ width:64px; height:64px; border-radius:16px; background:linear-gradient(135deg,#0f172a,#334155); display:flex; align-items:center; justify-content:center; font-size:1.5rem; font-weight:800; color:#fff; flex-shrink:0; }}
        .fact-grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(140px,1fr)); gap:12px; margin:20px 0; }}
        .fact-item {{ background:#111827; border:1px solid #262626; border-radius:10px; padding:10px 14px; }}
        .fact-label {{ font-size:0.7rem; color:#9ca3af; text-transform:uppercase; letter-spacing:0.05em; }}
        .fact-value {{ font-size:0.95rem; color:#f9fafb; font-weight:600; margin-top:2px; }}
        .direct-answer {{ font-size:1rem; line-height:1.7; background:#111827; border:1px solid #262626; border-radius:12px; padding:16px 18px; margin:18px 0; }}
    </style>
    </head>
    <body><div class="wrap">
    <a href="/" class="back">&larr; Back to Ranen</a>

    <div class="profile-header">
        <div class="profile-avatar">NC</div>
        <div>
            <h1 style="margin-bottom:2px;">Nwodili Yaemerie Convenant</h1>
            <p style="color:#9ca3af; font-size:0.9rem; margin:0;">Creator of Ranen</p>
        </div>
    </div>

    <h2>Who is Nwodili Yaemerie Convenant?</h2>
    <p class="direct-answer">{direct_answer}</p>

    <div class="fact-grid">
        <div class="fact-item"><div class="fact-label">Age</div><div class="fact-value">18</div></div>
        <div class="fact-item"><div class="fact-label">Location</div><div class="fact-value">Anambra State, Nigeria</div></div>
        <div class="fact-item"><div class="fact-label">School</div><div class="fact-value">Abia State University</div></div>
        <div class="fact-item"><div class="fact-label">Religion</div><div class="fact-value">Judaism</div></div>
    </div>

    <h2>Did Nwodili Yaemerie Convenant make Ranen?</h2>
    <p class="direct-answer">{did_make_answer}</p>

    <h2>What does he work on?</h2>
    <p>
        He builds full-stack web applications and AI-powered tools, with a particular interest in
        ethical hacking and vulnerability research alongside his development work.
    </p>
    <ul>
        <li>Cybersecurity &amp; ethical hacking fundamentals</li>
        <li>Full-stack web development (Python/FastAPI, JavaScript)</li>
        <li>Building AI-powered tools and assistants — including Ranen</li>
        <li>Linux system administration</li>
    </ul>

    <h2>What is Ranen?</h2>
    <p class="direct-answer">{what_is_ranen_answer}</p>

    <h2>Links</h2>
    <ul>
        <li><a href="https://github.com/whitefrostff-dev" target="_blank">GitHub — whitefrostff-dev</a></li>
    </ul>

    <h2>Contact</h2>
    {CONTACT_BLOCK}
    </div></body></html>
    """

@app.get("/api/user")
async def get_current_user(request: Request):
    user = request.session.get('user')
    if user:
        return {"logged_in": True, "name": user.get('name'), "email": user.get('email')}
    return {"logged_in": False, "name": "Guest User"}

@app.get("/api/debug/storage")
async def debug_storage():
    conn = get_db_connection()
    db_connected = conn is not None
    if conn:
        conn.close()
    return {
        "database_url_configured": bool(DATABASE_URL),
        "database_currently_reachable": db_connected,
        "using": "postgres" if db_connected else "local_file_fallback",
        "local_fallback_path": CHATS_FILE,
        "warning": None if db_connected else
            "Chat history is on ephemeral local disk and will be LOST on every redeploy/restart unless DATABASE_URL is set."
    }

@app.get("/api/debug/providers")
async def debug_providers():
    """Open /api/debug/providers in your browser to see EXACTLY why Groq or
    Gemini is failing. Never reveals the keys. Set DEBUG_ENDPOINTS=0 on your
    host to disable it once everything works."""
    if os.getenv("DEBUG_ENDPOINTS", "1") == "0":
        return {"disabled": True}

    out = {
        "groq_key_set": bool(GROQ_API_KEY),
        "groq_key_length": len(GROQ_API_KEY),
        "google_key_set": bool(GOOGLE_API_KEY),
        "groq_models": GROQ_MODELS,
        "gemini_models": GEMINI_MODELS,
        "groq_results": {},
        "gemini_results": {},
    }

    if not groq_client:
        out["groq_results"] = "NO CLIENT — GROQ_API_KEY is missing from your host's environment variables."
    else:
        for model in GROQ_MODELS:
            try:
                await asyncio.to_thread(
                    groq_client.chat.completions.create,
                    model=model, messages=[{"role": "user", "content": "say hi"}], max_tokens=30,
                )
                out["groq_results"][model] = "OK"
            except Exception as e:
                out["groq_results"][model] = str(e)[:300]

    if not genai_client:
        out["gemini_results"] = "NO CLIENT — GOOGLE_API_KEY is missing."
    else:
        for model in GEMINI_MODELS:
            try:
                await asyncio.to_thread(genai_client.models.generate_content, model=model, contents="say hi")
                out["gemini_results"][model] = "OK"
            except Exception as e:
                out["gemini_results"][model] = str(e)[:300]

    return out

@app.get('/auth/login')
async def login(request: Request):
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("x-forwarded-host", request.url.netloc)
    redirect_uri = f"{scheme}://{host}/auth/callback"
    return await oauth.google.authorize_redirect(request, redirect_uri)

@app.get('/auth/callback')
async def auth(request: Request):
    try:
        token = await oauth.google.authorize_access_token(request)
        user_info = token.get('userinfo')
        if user_info:
            request.session['user'] = {'name': user_info.get('name'), 'email': user_info.get('email')}
    except Exception as e:
        print(f"Auth error: {e}")
    return RedirectResponse(url="/")

@app.get('/auth/logout')
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/")

@app.post("/api/guest-mode")
async def switch_to_guest_mode(request: Request):
    request.session.pop('user', None)
    response = JSONResponse(content={"status": "success"})
    if not request.cookies.get("guest_id"):
        guest_id = str(uuid.uuid4())
        response.set_cookie(key="guest_id", value=guest_id, max_age=31536000, httponly=True)
    return response

@app.get("/api/sessions")
async def get_user_sessions(request: Request):
    col, val = get_identifier(request)
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT chat_id, title, created_at FROM user_chats WHERE user_email = %s ORDER BY created_at DESC", (val,))
                rows = cur.fetchall()
                conn.close()
                if rows:
                    return [{"id": r["chat_id"], "title": r.get("title", "Untitled Chat"), "is_pinned": 0} for r in rows]
        except Exception as e:
            print(f"DATABASE FETCH SESSIONS ERROR: {e}")
            if conn:
                conn.close()

    local_chats = load_local_chats()
    user_data = local_chats.get(val, {})
    return [{"id": cid, "title": info["title"], "is_pinned": 0} for cid, info in user_data.items()]

def _make_search_snippet(content: str, query_lower: str, context_chars: int = 40) -> str:
    idx = content.lower().find(query_lower)
    if idx == -1:
        return content[:80]
    start = max(0, idx - context_chars)
    end = min(len(content), idx + len(query_lower) + context_chars)
    prefix = "..." if start > 0 else ""
    suffix = "..." if end < len(content) else ""
    return f"{prefix}{content[start:end]}{suffix}"

@app.get("/api/search-messages")
async def search_messages(request: Request, q: str = ""):
    col, val = get_identifier(request)
    query_lower = q.strip().lower()
    if not query_lower:
        return []

    results = []
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT chat_id, title, messages FROM user_chats WHERE user_email = %s", (val,))
                rows = cur.fetchall()
                conn.close()
                for row in rows:
                    msgs = row.get("messages") or []
                    if isinstance(msgs, str):
                        msgs = json.loads(msgs)
                    for idx, m in enumerate(msgs):
                        content = m.get("content", "") or ""
                        if query_lower in content.lower():
                            results.append({
                                "chat_id": row["chat_id"],
                                "chat_title": row.get("title") or "Untitled Chat",
                                "message_index": idx,
                                "role": m.get("role"),
                                "snippet": _make_search_snippet(content, query_lower),
                            })
        except Exception as e:
            print(f"SEARCH MESSAGES ERROR: {e}")
            if conn:
                conn.close()
    else:
        local_chats = load_local_chats()
        user_data = local_chats.get(val, {})
        for chat_id, info in user_data.items():
            for idx, m in enumerate(info.get("messages", [])):
                content = m.get("content", "") or ""
                if query_lower in content.lower():
                    results.append({
                        "chat_id": chat_id,
                        "chat_title": info.get("title") or "Untitled Chat",
                        "message_index": idx,
                        "role": m.get("role"),
                        "snippet": _make_search_snippet(content, query_lower),
                    })

    return results[:50]

@app.get("/api/news-digest")
async def news_digest():
    cache_key = "daily_news_digest"
    cached = get_cached_search(cache_key, search_type="news")
    if cached is not None:
        return cached
    if not HAS_DDGS:
        return []
    try:
        async with search_lock:
            with DDGS() as ddgs:
                results = list(ddgs.news("world news technology", max_results=8))
        items = [
            {
                "title": r.get("title"),
                "url": r.get("url") or r.get("href"),
                "source": r.get("source"),
                "image": r.get("image") or "",
                "excerpt": (r.get("body") or "")[:220],
                "date": r.get("date") or "",
            }
            for r in results if r.get("title")
        ]
        set_cached_search(cache_key, items, search_type="news")
        return items
    except Exception as e:
        print(f"NEWS DIGEST ERROR: {e}")
        return []

@app.get("/api/history/{session_id}")
async def get_session_history(request: Request, session_id: str):
    col, val = get_identifier(request)
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT messages FROM user_chats WHERE user_email = %s AND chat_id = %s", (val, str(session_id)))
                row = cur.fetchone()
                conn.close()
                if row and row.get("messages"):
                    msgs = row["messages"]
                    return msgs if isinstance(msgs, list) else json.loads(msgs)
        except Exception as e:
            print(f"DATABASE FETCH HISTORY ERROR: {e}")
            if conn:
                conn.close()

    local_chats = load_local_chats()
    user_data = local_chats.get(val, {})
    if str(session_id) in user_data:
        return user_data[str(session_id)].get("messages", [])
    return []

@app.post("/api/new-session")
async def create_new_session(request: Request):
    col, val = get_identifier(request)
    new_chat_id = str(uuid.uuid4())
    save_chat_history(user_email=val, chat_id=new_chat_id, title="New Chat", messages=[])
    return {"session_id": new_chat_id}

@app.delete("/api/delete-session/{session_id}")
async def delete_session(request: Request, session_id: str):
    col, val = get_identifier(request)
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM user_chats WHERE user_email = %s AND chat_id = %s", (val, str(session_id)))
                conn.commit()
            conn.close()
        except Exception as e:
            print(f"DATABASE DELETE ERROR: {e}")
            if conn:
                conn.close()

    local_chats = load_local_chats()
    if val in local_chats and str(session_id) in local_chats[val]:
        del local_chats[val][str(session_id)]
        save_local_chats(local_chats)
    return {"status": "success"}

@app.get("/api/gems")
async def get_gems(request: Request):
    col, val = get_identifier(request)
    default_gems = [
        {
            "id": 1,
            "name": "Ranen Core",
            "description": "Standard elite assistant created by Nwodili Yaemerie Convenant",
            "system_prompt": (
                "You are Ranen, an elite AI assistant created by Nwodili Yaemerie Convenant. "
                "Reason carefully like a top-tier frontier model: think through problems step by step "
                "internally, catch your own mistakes before answering, and give precise, well-structured "
                "answers. Be direct and concise — no filler, no repeating the question, no unnecessary "
                "caveats. Match your depth to the question: quick questions get quick answers; hard "
                "technical or reasoning questions get a real, careful breakdown."
            ),
            "icon": "fa-terminal"
        }
    ]
    conn = get_db_connection()
    if not conn:
        return default_gems

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name, description, system_prompt, icon FROM gems WHERE user_email = 'system' OR user_email = %s", (val,))
            rows = cur.fetchall()
            conn.close()
            if rows:
                custom_gems = [{"id": r["id"], "name": r["name"], "description": r["description"], "system_prompt": r["system_prompt"], "icon": r.get("icon", "fa-robot")} for r in rows]
                return default_gems + custom_gems
    except Exception as e:
        print(f"DATABASE GEMS ERROR: {e}")
        if conn:
            conn.close()
    return default_gems

@app.post("/api/gems")
async def create_gem(
    request: Request,
    name: str = Form(...),
    description: str = Form(...),
    system_prompt: str = Form(...),
    icon: str = Form("fa-robot")
):
    col, val = get_identifier(request)
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO gems (user_email, name, description, system_prompt, icon) VALUES (%s, %s, %s, %s, %s)", (val, name, description, system_prompt, icon))
                conn.commit()
            conn.close()
        except Exception as e:
            print(f"Error creating gem: {e}")
            if conn:
                conn.close()
    return {"status": "success"}

@app.get("/api/assets")
async def get_user_assets(request: Request):
    col, val = get_identifier(request)
    conn = get_db_connection()
    if not conn:
        return []
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, file_name, file_path, file_type FROM assets WHERE user_email = %s ORDER BY id DESC", (val,))
            rows = cur.fetchall()
            conn.close()
            if rows:
                return [{"id": r["id"], "file_name": r["file_name"], "file_path": r["file_path"], "file_type": r["file_type"]} for r in rows]
    except Exception as e:
        print(f"DATABASE ASSETS ERROR: {e}")
        if conn:
            conn.close()
    return []

@app.post("/api/plugins/steps/save")
async def save_user_steps(request: Request, payload: StepSyncRequest):
    return {"status": "success", "steps_saved": payload.steps}

@app.get("/auth/github-plugin")
async def github_plugin_login():
    if not GITHUB_CLIENT_ID:
        return RedirectResponse(url="/?error=github_keys_missing")
    github_url = f"https://github.com/login/oauth/authorize?client_id={GITHUB_CLIENT_ID}&scope=repo,user"
    return RedirectResponse(github_url)

@app.get("/auth/github/callback")
async def github_plugin_callback(request: Request, code: str):
    async with httpx.AsyncClient() as client:
        res = await client.post(
            "https://github.com/login/oauth/access_token",
            headers={"Accept": "application/json"},
            data={"client_id": GITHUB_CLIENT_ID, "client_secret": GITHUB_CLIENT_SECRET, "code": code},
        )
        data = res.json()
        access_token = data.get("access_token")
        if access_token:
            request.session['github_token'] = access_token
            return RedirectResponse(url="/?plugin=github&status=connected")
        return RedirectResponse(url="/?plugin=github&status=failed")

# --- WEB SEARCH TOOL ---
RATE_LIMIT_MARKERS = ["429", "rate limit", "rate_limit", "quota", "resource_exhausted", "too many requests", "capacity"]

def _is_rate_limit_error(err: Exception) -> bool:
    msg = str(err).lower()
    return any(marker in msg for marker in RATE_LIMIT_MARKERS)

def _perform_web_search(query: str, max_results: int = 5) -> str:
    if not HAS_DDGS:
        return "Web search is unavailable — the `ddgs` package isn't installed on the server."
    cache_key = f"{query}:{max_results}"
    cached = get_cached_search(cache_key, search_type="text")
    if cached is not None:
        results = cached
    else:
        try:
            with DDGS() as ddgs:
                results = list(ddgs.text(query, max_results=max_results))
            set_cached_search(cache_key, results, search_type="text")
        except Exception as e:
            return f"Web search failed: {e}"
    if not results:
        return "No results found for that search."
    return "\n".join(f"- {r.get('title')}: {(r.get('body') or '')[:250]} (Source: {r.get('href')})" for r in results[:5])

# Set ENABLE_WEB_SEARCH=0 on your host to turn the search tool off (for testing)
ENABLE_WEB_SEARCH = os.getenv("ENABLE_WEB_SEARCH", "1") != "0"

WEB_SEARCH_TOOL_GROQ = {
    "type": "function",
    "function": {
        "name": "web_search",
        "description": (
            "Search the live web for current information — news, prices, recent events, or "
            "anything that may have changed since training or requires real-time knowledge. "
            "Use this whenever the answer could depend on up-to-date information."
        ),
        "parameters": {
            "type": "object",
            "properties": {"query": {"type": "string", "description": "The search query"}},
            "required": ["query"],
        },
    },
}

def _clean_tool_call(tc) -> dict:
    # Send back ONLY the fields Groq accepts. model_dump() can include extra
    # None fields that trigger a 400 on the follow-up request.
    return {
        "id": tc.id,
        "type": "function",
        "function": {"name": tc.function.name, "arguments": tc.function.arguments or "{}"},
    }

def _groq_text(message) -> str:
    return (getattr(message, "content", None) or "").strip()

async def _call_groq_with_tools(model, base_messages_payload):
    """Tool-calling first. If the tool round-trip breaks for a NON-quota reason,
    fall back to a plain call on the same model. Rate-limit/auth errors are
    re-raised so the model chain can move on to the next model."""
    if not ENABLE_WEB_SEARCH:
        plain = await asyncio.to_thread(
            groq_client.chat.completions.create,
            model=model, messages=base_messages_payload, temperature=0.75, max_tokens=2048,
        )
        return _groq_text(plain.choices[0].message)
    try:
        messages_payload = list(base_messages_payload)
        first = await asyncio.to_thread(
            groq_client.chat.completions.create,
            model=model, messages=messages_payload, temperature=0.75, max_tokens=2048,
            tools=[WEB_SEARCH_TOOL_GROQ], tool_choice="auto",
        )
        choice = first.choices[0]
        tool_calls = getattr(choice.message, "tool_calls", None)

        if not tool_calls:
            return _groq_text(choice.message)

        messages_payload.append({
            "role": "assistant",
            "content": choice.message.content or "",
            "tool_calls": [_clean_tool_call(tc) for tc in tool_calls],
        })

        for tc in tool_calls:
            try:
                args = json.loads(tc.function.arguments)
            except Exception:
                args = {}
            result_text = await asyncio.to_thread(_perform_web_search, args.get("query", ""))
            messages_payload.append({"role": "tool", "tool_call_id": tc.id, "content": result_text})

        second = await asyncio.to_thread(
            groq_client.chat.completions.create,
            model=model, messages=messages_payload, temperature=0.75, max_tokens=2048,
        )
        return _groq_text(second.choices[0].message)

    except Exception as e:
        low = str(e).lower()
        if "api key" in low or "401" in low or "invalid_api_key" in low:
            raise
        # Includes rate limits: the tool round-trip is the heavy part, so a
        # plain call (no search results) often still fits under the limit.
        print(f"[GROQ TOOL-CALLING FALLBACK on {model}]: {e}")
        plain = await asyncio.to_thread(
            groq_client.chat.completions.create,
            model=model, messages=base_messages_payload, temperature=0.75, max_tokens=2048,
        )
        return _groq_text(plain.choices[0].message)

async def _call_google_with_tools(model, system_prompt, base_contents):
    try:
        contents = list(base_contents)
        tool = types.Tool(function_declarations=[
            types.FunctionDeclaration(
                name="web_search",
                description="Search the live web for current, up-to-date information — news, prices, recent events.",
                parameters=types.Schema(
                    type="OBJECT",
                    properties={"query": types.Schema(type="STRING", description="The search query")},
                    required=["query"],
                ),
            )
        ])
        config = types.GenerateContentConfig(system_instruction=system_prompt, temperature=0.7, tools=[tool])
        resp = await asyncio.to_thread(genai_client.models.generate_content, model=model, contents=contents, config=config)

        candidate = resp.candidates[0]
        function_call_part = None
        for part in candidate.content.parts:
            if getattr(part, "function_call", None):
                function_call_part = part.function_call
                break

        if not function_call_part:
            return resp.text

        query = dict(function_call_part.args).get("query", "") if function_call_part.args else ""
        result_text = await asyncio.to_thread(_perform_web_search, query)

        contents.append(candidate.content)
        contents.append(types.Content(
            role="user",
            parts=[types.Part.from_function_response(name="web_search", response={"result": result_text})],
        ))
        resp2 = await asyncio.to_thread(genai_client.models.generate_content, model=model, contents=contents, config=config)
        return resp2.text

    except Exception as e:
        if _is_rate_limit_error(e):
            raise  # don't burn another request on a quota error
        print(f"[GOOGLE TOOL-CALLING FALLBACK on {model}]: {e}")
        config = types.GenerateContentConfig(system_instruction=system_prompt, temperature=0.7)
        resp = await asyncio.to_thread(genai_client.models.generate_content, model=model, contents=base_contents, config=config)
        return resp.text

async def _call_provider(provider, model, system_prompt, recent_history, effective_message, is_image, file_bytes, mime_type):
    if provider == "google":
        if not genai_client:
            raise RuntimeError("GOOGLE_API_KEY is missing.")
        contents = []
        for msg in recent_history[:-1]:
            role_prefix = "User" if msg["role"] == "user" else "Model"
            contents.append(f"{role_prefix}: {msg['content']}")

        if is_image and file_bytes:
            contents.append(types.Part.from_bytes(data=file_bytes, mime_type=mime_type))
            contents.append(effective_message)
            config = types.GenerateContentConfig(system_instruction=system_prompt, temperature=0.7)
            resp = await asyncio.to_thread(genai_client.models.generate_content, model=model, contents=contents, config=config)
            return resp.text

        contents.append(effective_message)
        return await _call_google_with_tools(model, system_prompt, contents)

    # groq
    if not groq_client:
        raise RuntimeError("GROQ_API_KEY is missing from environment variables.")
    messages_payload = [{"role": "system", "content": system_prompt}]
    for msg in recent_history[:-1]:
        messages_payload.append({"role": msg["role"], "content": msg["content"]})
    messages_payload.append({"role": "user", "content": effective_message})
    return await _call_groq_with_tools(model, messages_payload)

async def _generate_smart_title(user_message: str, ai_response: str) -> Optional[str]:
    if not groq_client:
        return None
    try:
        result = await asyncio.to_thread(
            groq_client.chat.completions.create,
            model="llama-3.1-8b-instant",
            messages=[
                {"role": "system", "content": (
                    "Generate a short chat title (3-6 words, no quotes, no punctuation at the "
                    "end) summarizing what this conversation is about. Reply with ONLY the title."
                )},
                {"role": "user", "content": f"User: {user_message[:300]}\nAssistant: {ai_response[:300]}"}
            ],
            temperature=0.3,
            max_tokens=20,
        )
        title = result.choices[0].message.content.strip().strip('"').strip("'")
        if title and len(title) <= 60:
            return title
        return None
    except Exception as e:
        print(f"[SMART TITLE ERROR]: {e}")
        return None

DEFAULT_SYSTEM_PROMPT = (
    "You are Ranen, an elite AI assistant created by Nwodili Yaemerie Convenant. "
    "CORE DIRECTIVES:\n"
    "1. Reason like a top-tier frontier model (Sonnet/Fable-class): think through problems "
    "carefully and internally before answering, break down hard problems step by step, catch "
    "your own errors, and never guess when you can reason it out.\n"
    "2. Act natural and humanoid. Speak like a real developer/peer, not a machine.\n"
    "3. BE CONCISE by default. No filler, no repeating the question, no unnecessary preamble. "
    "But when a question is genuinely technical or multi-step, give it the depth it needs — "
    "concise does not mean shallow.\n"
    "4. Eliminate robotic filler, meta-commentary, and empty politeness.\n"
    "5. Identity: If asked who made you or what your name is, state clearly: 'I am Ranen, created by Nwodili Yaemerie Convenant.'\n"
    "6. You have a web_search tool. Use it whenever a question depends on current events, "
    "prices, recent releases, or anything that could have changed since your training — don't "
    "guess or say you can't access the internet, just call the tool. Don't use it for timeless "
    "facts, math, or general knowledge you already know.\n"
    "7. When asked to build a website, app, or any code project: pick the best approach "
    "yourself and build it directly — never respond with a list of design options or ask which "
    "style the person wants before writing code. State any assumption in one line if needed, "
    "then deliver complete, working, production-quality output in a single response — no "
    "placeholders, no 'add your logic here' stubs, no half-finished sections.\n"
    "8. Never use inline style=\"...\" attributes on HTML elements. Put CSS in a single "
    "organized <style> block (single-file output) or one shared external stylesheet (multi-file "
    "output, e.g. Flask templates) — never duplicate a <style> block across multiple pages of the "
    "same project. Use semantic HTML and make it visually polished by default (real layout, "
    "spacing, and color choices) even if the person didn't specify a design — never ship "
    "something plain or unfinished unless they explicitly asked for bare-bones."
)

async def _generate_with_fallback(system_prompt, recent_history, effective_message, is_image, file_bytes, mime_type):
    """GROQ FIRST for every text request, whatever model is picked in the UI.
    Walks every Groq model in GROQ_MODELS, and only if ALL of them fail does it
    try Gemini. Image requests go to Gemini only (no Groq vision here). Every
    failure is collected, so you see the real Groq error, not just Gemini's."""
    order = ["google"] if is_image else ["groq", "google"]

    errors = []
    for p in order:
        for model in _model_chain(p):
            try:
                resp = await _call_provider(
                    p, model, system_prompt, recent_history,
                    effective_message, is_image, file_bytes, mime_type
                )
                if resp and resp.strip():
                    print(f"[SERVED BY] {p}/{model}")
                    return resp
                errors.append(f"{p}/{model}: empty response")
            except Exception as e:
                msg = str(e)
                print(f"[{p.upper()} ERROR on {model}]: {msg[:400]}")
                errors.append(f"{p}/{model}: {msg[:200]}")
                if "is missing" in msg.lower():
                    break  # no key for this provider — skip its remaining models

    raise RuntimeError(" || ".join(errors))

BUSY_MESSAGE = "Ranen is a bit overloaded right now. Please try again in a minute."

@app.post("/api/chat")
async def chat_with_assistant(
    request: Request,
    session_id: str = Form(...),
    message: str = Form(""),
    files: List[UploadFile] = File(default=[]),
    model_choice: str = Form("groq:openai/gpt-oss-120b"),
    gem_prompt: Optional[str] = Form(None)
):
    col, val = get_identifier(request)

    if not under_daily_limit(val):
        return {"response": f"You've reached today's limit of {DAILY_MESSAGE_LIMIT} messages — it resets at midnight. Thanks for using Ranen!"}

    existing_messages = []
    chat_title = "New Chat"

    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT messages, title FROM user_chats WHERE user_email = %s AND chat_id = %s", (val, str(session_id)))
                row = cur.fetchone()
                conn.close()
                if row:
                    msgs = row.get("messages", [])
                    existing_messages = msgs if isinstance(msgs, list) else json.loads(msgs) if msgs else []
                    chat_title = row.get("title", "New Chat")
        except Exception as e:
            print(f"DATABASE FETCH ERROR IN /api/chat: {e}")
            if conn:
                conn.close()
    else:
        local_chats = load_local_chats()
        user_data = local_chats.get(val, {}).get(str(session_id), {})
        existing_messages = user_data.get("messages", [])
        chat_title = user_data.get("title", "New Chat")

    file_bytes = None
    mime_type = ""
    is_image = False
    file_text_content = ""
    display_message = message
    attached_names = []

    for f in files:
        if not f or not f.filename:
            continue

        f_bytes = await f.read()
        f_mime = f.content_type or "application/octet-stream"
        f_is_image = f_mime.startswith("image/")

        stored_filename = f"{uuid.uuid4().hex}_{f.filename}"
        filepath = os.path.join(UPLOAD_DIR, stored_filename)
        with open(filepath, "wb") as fh:
            fh.write(f_bytes)

        rel_path = f"/uploads/{stored_filename}"
        conn_asset = get_db_connection()
        if conn_asset:
            try:
                with conn_asset.cursor() as cur:
                    cur.execute("INSERT INTO assets (user_email, file_name, file_path, file_type) VALUES (%s, %s, %s, %s)", (val, f.filename, rel_path, f_mime))
                    conn_asset.commit()
                conn_asset.close()
            except Exception as e:
                print(f"Error saving asset to DB: {e}")
                if conn_asset:
                    conn_asset.close()

        attached_names.append(f.filename)

        if f_is_image:
            if file_bytes is None:
                file_bytes = f_bytes
                mime_type = f_mime
                is_image = True
        else:
            extracted = extract_file_text(f_bytes, f.filename, f_mime)
            file_text_content += (
                f"\n\n--- Contents of uploaded file '{f.filename}' ---\n"
                f"```\n{extracted}\n```\n"
                f"--- End of file contents ---"
            )

    if attached_names:
        display_message += f" [Attached Files: {', '.join(attached_names)}]"

    existing_messages.append({"role": "user", "content": display_message})

    is_first_message = chat_title in ["New Chat", ""] and bool(message)
    if is_first_message:
        chat_title = (message[:28] + '...') if len(message) > 28 else message

    recent_history = existing_messages[-12:]
    lower_prompt = message.lower()

    # --- Image search interceptor (whole-word match so "image" inside other words doesn't trigger) ---
    image_trigger_words = ["pic", "pics", "picture", "pictures", "image", "images", "photo", "photos"]
    has_image_intent = any(re.search(r'\b' + w + r'\b', lower_prompt) for w in image_trigger_words)

    if has_image_intent and not attached_names:
        clean_prompt = message
        match = re.search(r'(?:picture|pic|image|photo)s?\s+(?:of\s+)?(.*)', message, re.IGNORECASE)
        if match and match.group(1).strip():
            clean_prompt = match.group(1).strip()

        noise_words = ["search", "find", "get", "show", "me", "generate", "create",
                       "duckduckgo", "can you", "please", "i want", "a", "an", "the", "some"]
        for noise in noise_words:
            clean_prompt = re.sub(r'\b' + noise + r'\b', '', clean_prompt, flags=re.IGNORECASE)
        clean_prompt = ' '.join(clean_prompt.split()).strip() or message

        ai_response = f"Ah, my bad bro. I tried pulling up a picture of **\"{clean_prompt}\"**, but my search is acting up. Give it another try in a bit!"
        if HAS_DDGS:
            cached_img_results = get_cached_search(clean_prompt, search_type="image")
            if cached_img_results is not None:
                results = cached_img_results
            else:
                async with search_lock:
                    try:
                        await asyncio.sleep(0.5)
                        with DDGS() as ddgs:
                            results = list(ddgs.images(clean_prompt, max_results=1))
                            set_cached_search(clean_prompt, results, search_type="image")
                    except Exception as e:
                        results = []
                        print(f"Image search throttle/error: {e}")

            if results:
                image_url = results[0].get('image')
                title = results[0].get('title', 'DuckDuckGo Image')
                ai_response = f'Here is the image I found for **"{clean_prompt}"**:<br><br><img src="{image_url}" alt="{title}" style="max-width:100%; border-radius:8px; margin-top:10px;" />'
            else:
                ai_response = f"Man, I scoured the web for **\"{clean_prompt}\"** but couldn't grab a good image right now. Try asking me again later!"
        else:
            ai_response = "I'd love to show you a picture, but my image search engine isn't wired up right now. We need the `ddgs` package!"

        existing_messages.append({"role": "assistant", "content": ai_response})
        save_chat_history(user_email=val, chat_id=str(session_id), title=chat_title, messages=existing_messages)
        return {"response": ai_response}

    current_date_str = datetime.now().strftime("%A, %B %d, %Y")
    effective_message = f"{message}\n\n[Current date: {current_date_str}]"
    if file_text_content:
        effective_message = f"{effective_message}{file_text_content}"

    system_prompt = (gem_prompt.strip() if (gem_prompt and gem_prompt.strip()) else None) or DEFAULT_SYSTEM_PROMPT

    succeeded = True
    try:
        ai_response = await _generate_with_fallback(
            system_prompt, recent_history, effective_message, is_image, file_bytes, mime_type
        )
    except Exception as e:
        print(f"ALL PROVIDERS FAILED: {e}")
        ai_response = BUSY_MESSAGE
        succeeded = False

    # Only charge the daily limit for replies that actually worked
    if succeeded:
        increment_today_usage(val)

    if is_first_message and succeeded:
        smart_title = await _generate_smart_title(message, ai_response)
        if smart_title:
            chat_title = smart_title

    existing_messages.append({"role": "assistant", "content": ai_response})
    save_chat_history(user_email=val, chat_id=str(session_id), title=chat_title, messages=existing_messages)
    return {"response": ai_response}

@app.post("/api/regenerate")
async def regenerate_response(
    request: Request,
    session_id: str = Form(...),
    model_choice: str = Form("groq:openai/gpt-oss-120b"),
    gem_prompt: Optional[str] = Form(None)
):
    col, val = get_identifier(request)

    if not under_daily_limit(val):
        return JSONResponse(status_code=200, content={"error": f"You've reached today's limit of {DAILY_MESSAGE_LIMIT} messages — it resets at midnight."})

    existing_messages = []
    chat_title = "New Chat"

    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT messages, title FROM user_chats WHERE user_email = %s AND chat_id = %s", (val, str(session_id)))
                row = cur.fetchone()
                conn.close()
                if row:
                    msgs = row.get("messages", [])
                    existing_messages = msgs if isinstance(msgs, list) else json.loads(msgs) if msgs else []
                    chat_title = row.get("title", "New Chat")
        except Exception as e:
            print(f"DATABASE FETCH ERROR IN /api/regenerate: {e}")
            if conn:
                conn.close()
    else:
        local_chats = load_local_chats()
        user_data = local_chats.get(val, {}).get(str(session_id), {})
        existing_messages = user_data.get("messages", [])
        chat_title = user_data.get("title", "New Chat")

    if not existing_messages or existing_messages[-1]["role"] != "assistant":
        return JSONResponse(status_code=400, content={"error": "No response to regenerate."})

    existing_messages.pop()

    last_user_message = None
    for m in reversed(existing_messages):
        if m["role"] == "user":
            last_user_message = m["content"]
            break

    if not last_user_message:
        return JSONResponse(status_code=400, content={"error": "No user message found to regenerate from."})

    recent_history = existing_messages[-12:]
    system_prompt = (gem_prompt.strip() if (gem_prompt and gem_prompt.strip()) else None) or DEFAULT_SYSTEM_PROMPT

    succeeded = True
    try:
        ai_response = await _generate_with_fallback(
            system_prompt, recent_history, last_user_message, False, None, ""
        )
    except Exception as e:
        print(f"ALL PROVIDERS FAILED (regenerate): {e}")
        ai_response = BUSY_MESSAGE
        succeeded = False

    if succeeded:
        increment_today_usage(val)

    existing_messages.append({"role": "assistant", "content": ai_response})
    save_chat_history(user_email=val, chat_id=str(session_id), title=chat_title, messages=existing_messages)
    return {"response": ai_response}

# --- LIVE CALL (Gemini Live API relay) ---
LIVE_CALL_MODEL = os.getenv("LIVE_CALL_MODEL", "gemini-3.1-flash-live-preview")

@app.websocket("/ws/live-call")
async def live_call_websocket(websocket: WebSocket):
    await websocket.accept()

    if not genai_client:
        await websocket.send_json({"type": "error", "message": "GOOGLE_API_KEY is missing — Live Call needs Gemini configured."})
        await websocket.close()
        return

    ws_identifier = get_identifier_ws(websocket)
    if not check_and_increment_usage(ws_identifier):
        await websocket.send_json({"type": "error", "message": f"You've reached today's limit of {DAILY_MESSAGE_LIMIT} messages — it resets at midnight."})
        await websocket.close()
        return

    live_config = {"response_modalities": ["AUDIO"], "system_instruction": DEFAULT_SYSTEM_PROMPT}

    try:
        async with genai_client.aio.live.connect(model=LIVE_CALL_MODEL, config=live_config) as session:
            await websocket.send_json({"type": "ready"})

            async def relay_browser_to_gemini():
                try:
                    while True:
                        message = await websocket.receive()
                        if message.get("type") == "websocket.disconnect":
                            break
                        try:
                            if message.get("bytes") is not None:
                                await session.send_realtime_input(
                                    audio=types.Blob(data=message["bytes"], mime_type="audio/pcm;rate=16000")
                                )
                            elif message.get("text") is not None:
                                payload = json.loads(message["text"])
                                if payload.get("type") == "end":
                                    break
                        except Exception as inner_e:
                            print(f"[LIVE CALL] browser->gemini frame skipped: {inner_e}")
                            continue
                except WebSocketDisconnect:
                    pass
                except Exception as e:
                    print(f"[LIVE CALL] browser->gemini relay ended: {e}")

            async def relay_gemini_to_browser():
                try:
                    async for response in session.receive():
                        try:
                            audio_data = getattr(response, "data", None)
                            if audio_data:
                                await websocket.send_bytes(audio_data)
                            server_content = getattr(response, "server_content", None)
                            if server_content is not None:
                                if getattr(server_content, "interrupted", False):
                                    await websocket.send_json({"type": "interrupted"})
                                if getattr(server_content, "turn_complete", False):
                                    await websocket.send_json({"type": "turn_complete"})
                        except Exception as inner_e:
                            print(f"[LIVE CALL] gemini message skipped: {inner_e}")
                            continue
                except Exception as e:
                    print(f"[LIVE CALL] gemini->browser relay ended: {e}")

            browser_task = asyncio.create_task(relay_browser_to_gemini())
            gemini_task = asyncio.create_task(relay_gemini_to_browser())
            done, pending = await asyncio.wait([browser_task, gemini_task], return_when=asyncio.FIRST_COMPLETED)
            for task in pending:
                task.cancel()

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"[LIVE CALL] session error: {e}")
        try:
            await websocket.send_json({"type": "error", "message": "Live call session failed. Please try again."})
        except Exception:
            pass
    finally:
        try:
            await websocket.close()
        except Exception:
            pass

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), reload=False)
