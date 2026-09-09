from fastapi import FastAPI, Form, File, UploadFile, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
from starlette.middleware.sessions import SessionMiddleware
from typing import Optional
import uuid
import os
import re
import io
import base64
import urllib.parse
import shutil
import httpx
import json
import time
import asyncio
from datetime import datetime
from pydantic import BaseModel
from groq import Groq
from google import genai
from google.genai import types
from openai import AsyncOpenAI
from authlib.integrations.starlette_client import OAuth
import psycopg2
import psycopg2.extras

# PDF text extraction dependency check
try:
    from pypdf import PdfReader
    HAS_PYPDF = True
except ImportError:
    HAS_PYPDF = False

# DuckDuckGo Web & Image Search Dependency Check
try:
    from ddgs import DDGS
    HAS_DDGS = True
except ImportError:
    HAS_DDGS = False

app = FastAPI()

# --- SEARCH CACHING & RATE-LIMIT PREVENTION SETUP ---
search_cache = {}
CACHE_TTL = 3600  # Cache search results for 1 hour
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

# --- POSTGRESQL DATABASE SETUP ---
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
                """)
                conn.commit()
            conn.close()
            print("PostgreSQL tables initialized successfully.")
        except Exception as e:
            print(f"Error initializing PostgreSQL tables: {e}")
            if conn:
                conn.close()

init_db()

# --- PERSISTENT FILE STORAGE & UPLOADS ---
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
    try:
        with open(CHATS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f)
    except Exception as e:
        print(f"Error saving to disk: {e}")

UPLOAD_DIR = os.path.join(DATA_DIR, "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)
app.mount("/uploads", StaticFiles(directory=UPLOAD_DIR), name="uploads")

# --- FAVICON ---
# Root-level files aren't served by the /uploads StaticFiles mount, so this
# needs its own route. Looks for favicon.png next to index.html; falls back
# to a 204 (no icon) if it's not there yet so the route never 500s.
def _find_favicon_path():
    candidates = [
        os.path.join("..", "app", "favicon.png"),
        "favicon.png",
    ]
    for path in candidates:
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
    # Browsers request this by default even when a PNG <link> is set; point it
    # at the same PNG so there's no broken-icon request in the network tab.
    path = _find_favicon_path()
    if path:
        return Response(content=open(path, "rb").read(), media_type="image/png")
    return Response(status_code=204)

# --- PROXY & SECURE SESSION FIX ---
app.add_middleware(ProxyHeadersMiddleware, trusted_hosts="*")
app.add_middleware(
    SessionMiddleware, 
    secret_key="ranen_super_secret_session_string",
    https_only=False,
    same_site="lax"
)

# --- ENVIRONMENT VARIABLES ---
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
SILICONFLOW_API_KEY = os.getenv("SILICONFLOW_API_KEY")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
GITHUB_CLIENT_ID = os.getenv("GITHUB_CLIENT_ID", "")
GITHUB_CLIENT_SECRET = os.getenv("GITHUB_CLIENT_SECRET", "")

# Init API Clients
groq_client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None
genai_client = genai.Client(api_key=GOOGLE_API_KEY) if GOOGLE_API_KEY else None

silicon_client = AsyncOpenAI(
    api_key=SILICONFLOW_API_KEY,
    base_url="https://api.siliconflow.cn/v1"
) if SILICONFLOW_API_KEY else None

openrouter_client = AsyncOpenAI(
    api_key=OPENROUTER_API_KEY,
    base_url="https://openrouter.ai/api/v1"
) if OPENROUTER_API_KEY else None

# --- GOOGLE OAUTH SETUP ---
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
    
    guest_id = request.cookies.get("guest_id")
    return ("guest_id", guest_id if guest_id else "unknown_guest")

# --- FILE TEXT EXTRACTION HELPER ---
# Caps how much extracted text we stuff into the model context so a huge
# PDF doesn't blow past the model's context window / your max_tokens budget.
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
    """Return best-effort plain text for a non-image upload, truncated to a safe size."""
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

# --- DATABASE HELPER FUNCTIONS ---
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
    local_chats[user_email][str(chat_id)] = {
        "title": title,
        "messages": messages
    }
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
    <url>
        <loc>https://ranen.duckdns.org/</loc>
        <changefreq>daily</changefreq>
        <priority>1.0</priority>
    </url>
    <url>
        <loc>https://ranen.duckdns.org/terms</loc>
        <changefreq>monthly</changefreq>
        <priority>0.3</priority>
    </url>
    <url>
        <loc>https://ranen.duckdns.org/privacy</loc>
        <changefreq>monthly</changefreq>
        <priority>0.3</priority>
    </url>
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
    <p>Ranen is powered by third-party large language models (including but not limited to Groq, Google Gemini, OpenRouter, and SiliconFlow). Outputs may contain errors, outdated information, or hallucinations. Do not rely on Ranen as a substitute for professional medical, legal, financial, or safety-critical advice. Always verify important information independently.</p>

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
    <p>Your prompts, uploaded files, and images are sent to third-party AI providers (Groq, Google Gemini, OpenRouter, SiliconFlow) to generate responses. Each provider processes this data under its own privacy and data-handling terms. We do not control how these providers internally process requests beyond what their APIs document.</p>

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

@app.get("/api/user")
async def get_current_user(request: Request):
    user = request.session.get('user')
    if user:
        return {"logged_in": True, "name": user.get('name'), "email": user.get('email')}
    return {"logged_in": False, "name": "Guest User"}

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
            request.session['user'] = {
                'name': user_info.get('name'),
                'email': user_info.get('email')
            }
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
            data={
                "client_id": GITHUB_CLIENT_ID,
                "client_secret": GITHUB_CLIENT_SECRET,
                "code": code,
            },
        )
        data = res.json()
        access_token = data.get("access_token")
        
        if access_token:
            request.session['github_token'] = access_token
            return RedirectResponse(url="/?plugin=github&status=connected")
        return RedirectResponse(url="/?plugin=github&status=failed")

RATE_LIMIT_MARKERS = ["429", "rate limit", "rate_limit", "quota", "resource_exhausted", "too many requests", "capacity"]

def _is_rate_limit_error(err: Exception) -> bool:
    msg = str(err).lower()
    return any(marker in msg for marker in RATE_LIMIT_MARKERS)

PROVIDER_DEFAULT_MODEL = {
    "groq": "openai/gpt-oss-120b",
    "openrouter": "meta-llama/llama-3.3-70b-instruct",
    "google": "gemini-1.5-flash",
    "silicon": "Qwen/Qwen2.5-7B-Instruct",
}

async def _call_provider(provider, actual_model, system_prompt, recent_history, effective_message, is_image, file_bytes, mime_type):
    """Dispatches one AI call to the given provider. Raises on failure —
    callers handle retries/fallback."""
    if provider == "openrouter":
        if not openrouter_client:
            raise RuntimeError("OPENROUTER_API_KEY is missing.")
        messages_payload = [{"role": "system", "content": system_prompt}]
        for msg in recent_history[:-1]:
            messages_payload.append({"role": msg["role"], "content": msg["content"]})

        if is_image and file_bytes:
            b64_img = base64.b64encode(file_bytes).decode('utf-8')
            messages_payload.append({
                "role": "user",
                "content": [
                    {"type": "text", "text": effective_message or "Analyze this file."},
                    {"type": "image_url", "image_url": {"url": f"data:{mime_type};base64,{b64_img}"}}
                ]
            })
        else:
            messages_payload.append({"role": "user", "content": effective_message})

        res = await openrouter_client.chat.completions.create(
            model=actual_model, messages=messages_payload, max_tokens=2048
        )
        return res.choices[0].message.content

    elif provider == "google":
        if not genai_client:
            raise RuntimeError("GOOGLE_API_KEY is missing.")
        contents = []
        for msg in recent_history[:-1]:
            role_prefix = "User" if msg["role"] == "user" else "Model"
            contents.append(f"{role_prefix}: {msg['content']}")

        if is_image and file_bytes:
            image_part = types.Part.from_bytes(data=file_bytes, mime_type=mime_type)
            contents.append(image_part)

        contents.append(effective_message)

        config = types.GenerateContentConfig(system_instruction=system_prompt, temperature=0.7)
        resp = genai_client.models.generate_content(model=actual_model, contents=contents, config=config)
        return resp.text

    elif provider == "silicon":
        if not silicon_client:
            raise RuntimeError("SILICONFLOW_API_KEY is missing.")
        messages_payload = [{"role": "system", "content": system_prompt}]
        for msg in recent_history[:-1]:
            messages_payload.append({"role": msg["role"], "content": msg["content"]})
        messages_payload.append({"role": "user", "content": effective_message})

        response = await silicon_client.chat.completions.create(
            model=actual_model, messages=messages_payload, max_tokens=2048
        )
        return response.choices[0].message.content

    else:  # groq
        if not groq_client:
            raise RuntimeError("GROQ_API_KEY is missing from environment variables.")
        if "70b" in actual_model or "versatile" in actual_model:
            actual_model = "openai/gpt-oss-120b"
        elif "8b" in actual_model or "instant" in actual_model or not actual_model:
            actual_model = "openai/gpt-oss-20b"
        else:
            actual_model = "openai/gpt-oss-120b"

        messages_payload = [{"role": "system", "content": system_prompt}]
        for msg in recent_history[:-1]:
            messages_payload.append({"role": msg["role"], "content": msg["content"]})
        messages_payload.append({"role": "user", "content": effective_message})

        chat_completion = groq_client.chat.completions.create(
            model=actual_model, messages=messages_payload, temperature=0.75, max_tokens=2048
        )
        return chat_completion.choices[0].message.content


@app.post("/api/chat")
async def chat_with_assistant(
    request: Request,
    session_id: str = Form(...), 
    message: str = Form(""), 
    file: Optional[UploadFile] = File(None),
    model_choice: str = Form("groq:openai/gpt-oss-120b"), 
    gem_prompt: Optional[str] = Form(None)
):
    col, val = get_identifier(request)

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

    if file and file.filename:
        file_bytes = await file.read()
        mime_type = file.content_type or "application/octet-stream"
        is_image = mime_type.startswith("image/")
        
        filename = f"{uuid.uuid4().hex}_{file.filename}"
        filepath = os.path.join(UPLOAD_DIR, filename)
        with open(filepath, "wb") as f:
            f.write(file_bytes)
            
        rel_path = f"/uploads/{filename}"
        conn_asset = get_db_connection()
        if conn_asset:
            try:
                with conn_asset.cursor() as cur:
                    cur.execute("INSERT INTO assets (user_email, file_name, file_path, file_type) VALUES (%s, %s, %s, %s)", (val, file.filename, rel_path, mime_type))
                    conn_asset.commit()
                conn_asset.close()
            except Exception as e:
                print(f"Error saving asset to DB: {e}")
                if conn_asset:
                    conn_asset.close()

        # --- Extract text from non-image files (PDF-aware) ---
        if not is_image:
            file_text_content = extract_file_text(file_bytes, file.filename, mime_type)

        display_message += f" [Attached File: {file.filename}]"

    existing_messages.append({"role": "user", "content": display_message})
    
    if chat_title in ["New Chat", ""] and message:
        chat_title = (message[:28] + '...') if len(message) > 28 else message

    recent_history = existing_messages[-12:]
    lower_prompt = message.lower()
    
    # --- Robust Image Search Interceptor ---
    image_trigger_words = ["pic", "pics", "picture", "pictures", "image", "images", "photo", "photos"]
    has_image_intent = any(w in lower_prompt for w in image_trigger_words)
    
    if has_image_intent and not file:
        clean_prompt = message
        match = re.search(r'(?:picture|pic|image|photo)s?\s+(?:of\s+)?(.*)', message, re.IGNORECASE)
        if match and match.group(1).strip():
            clean_prompt = match.group(1).strip()
            
        noise_words = [
            "search", "find", "get", "show", "me", "generate", "create", 
            "duckduckgo", "can you", "please", "i want", "a", "an", "the", "some"
        ]
        
        for noise in noise_words:
            clean_prompt = re.sub(r'\b' + noise + r'\b', '', clean_prompt, flags=re.IGNORECASE)
            
        clean_prompt = ' '.join(clean_prompt.split()).strip()
        clean_prompt = clean_prompt or message

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

    # --- Live Web Search Interceptor ---
    web_search_keywords = ["news", "latest", "current", "what is happening", "today", "price", "update", "exchange rate"]
    research_keywords = ["research", "deep dive", "look into", "investigate", "in-depth"]
    effective_message = message

    is_research_mode = any(kw in lower_prompt for kw in research_keywords)
    should_search = is_research_mode or any(kw in lower_prompt for kw in web_search_keywords)
    # Research mode pulls more sources for a deeper synthesis. This is
    # user-triggered on-demand depth, not autonomous background research —
    # a true "AI researches on its own schedule" feature needs a background
    # worker/scheduler process, which this request-response backend doesn't have.
    search_result_count = 6 if is_research_mode else 3

    if HAS_DDGS and should_search:
        cached_text_results = get_cached_search(f"{message}:{search_result_count}", search_type="text")
        if cached_text_results is not None:
            results = cached_text_results
        else:
            async with search_lock:
                try:
                    await asyncio.sleep(0.5) 
                    with DDGS() as ddgs:
                        results = list(ddgs.text(message, max_results=search_result_count))
                        set_cached_search(f"{message}:{search_result_count}", results, search_type="text")
                except Exception as e:
                    results = []
                    print(f"Web search execution error: {e}")

        if results:
            snippets = "\n".join([f"- Title: {r.get('title')}\n  Snippet: {r.get('body')}\n  Source URL: {r.get('href')}" for r in results])
            current_date_str = datetime.now().strftime("%A, %B %d, %Y")
            research_instruction = (
                "Instructions: This is a research request. Synthesize the sources above into a "
                "structured, well-organized answer — cover multiple angles, note any disagreement "
                "between sources, and cite source URLs inline where relevant."
                if is_research_mode else
                "Instructions: Use the real-time search context above to answer the user's question directly."
            )
            effective_message = (
                f"{message}\n\n"
                f"[Live Web Search Context - Current Date: {current_date_str}]:\n"
                f"{snippets}\n\n"
                f"{research_instruction}"
            )

    # --- Inject File Text Content into Message Payload if Present ---
    if file_text_content and not is_image:
        effective_message = (
            f"{effective_message}\n\n"
            f"--- Contents of uploaded file '{file.filename}' ---\n"
            f"```\n{file_text_content}\n```\n"
            f"--- End of file contents ---"
        )

    # --- Standard AI Chat Processing ---
    system_prompt = (gem_prompt.strip() if (gem_prompt and gem_prompt.strip()) else None) or (
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
        "5. Identity: If asked who made you or what your name is, state clearly: 'I am Ranen, created by Nwodili Yaemerie Convenant.'"
    )

    provider, actual_model = model_choice.split(":", 1) if ":" in model_choice else ("groq", model_choice)

    if provider == "google":
        actual_model = "gemini-3.6-flash"
    elif provider == "openrouter" and actual_model.endswith(":free"):
        actual_model = actual_model.replace(":free", "")

    # Automatic provider fallback: if the chosen provider is rate-limited,
    # retry on another provider you already have configured via env vars.
    # This does NOT rotate API keys or accounts to dodge quotas (that would
    # violate provider ToS) — it just uses redundancy across the different,
    # legitimately-configured providers this app already supports.
    vision_capable = {"openrouter", "google"}
    all_providers = ["groq", "openrouter", "google", "silicon"]
    fallback_order = [provider] + [p for p in all_providers if p != provider and (not is_image or p in vision_capable)]

    ai_response = None
    last_error = None

    for attempt_provider in fallback_order:
        attempt_model = actual_model if attempt_provider == provider else PROVIDER_DEFAULT_MODEL.get(attempt_provider, actual_model)
        try:
            ai_response = await _call_provider(
                attempt_provider, attempt_model, system_prompt, recent_history,
                effective_message, is_image, file_bytes, mime_type
            )
            if attempt_provider != provider:
                print(f"[FALLBACK] {provider} unavailable, served by {attempt_provider} instead.")
            break
        except Exception as e:
            last_error = e
            print(f"[{attempt_provider.upper()} API ERROR]: {str(e)}")
            if not _is_rate_limit_error(e):
                # Non-rate-limit errors (bad key, malformed request) won't be
                # fixed by switching providers — stop trying immediately.
                break
            continue

    if ai_response is None:
        ai_response = f"Whoops, looks like the AI provider(s) hit a snag: {str(last_error)}"

    existing_messages.append({"role": "assistant", "content": ai_response})
    save_chat_history(user_email=val, chat_id=str(session_id), title=chat_title, messages=existing_messages)

    return {"response": ai_response}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), reload=False)

