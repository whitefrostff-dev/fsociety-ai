from fastapi import FastAPI, Form, File, UploadFile, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
from starlette.middleware.sessions import SessionMiddleware
from typing import Optional, List
from urllib.parse import urlparse, quote
import html as html_lib
import threading
import uuid
import os
import re
import io
import httpx
import json
import time
import asyncio
import secrets
from datetime import datetime
from pydantic import BaseModel
from groq import Groq
from google import genai
from google.genai import types
from authlib.integrations.starlette_client import OAuth
import psycopg2
import psycopg2.extras

# [ADMIN] admin panel (admin.py must sit next to this file)
from admin import create_admin_router, require_admin

# MongoDB Atlas mirror + Firebase login (integrations.py must sit next to this file)
from integrations import (
    mongo_save_chat, mongo_get_chat, mongo_list_chats, mongo_delete_chat,
    firebase_ready, verify_firebase_token, MONGODB_URI,
)

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

# --- POSTGRESQL (Neon) ---
# [FIXED] Set DATABASE_URL on Render to your Neon connection string (the pooled one).
# Common Neon problems this block now handles automatically:
#   1. The string was pasted with quotes, spaces, or a leading "psql " -> cleaned.
#   2. "channel_binding=require" (Neon adds it by default) breaks some psycopg2 builds -> removed.
#   3. sslmode missing -> added (Neon only accepts SSL).
#   4. Neon sleeps when idle, so the first connect can time out -> retried.
#   5. Tables were only created once at startup; if Neon was asleep then, they were NEVER
#      created and every query failed -> tables are now created on the first good connection.
#   6. If Neon is truly down, requests no longer freeze for 10s each -> 20s cool-down, then
#      the app falls back to MongoDB Atlas / local file.
def _clean_database_url(raw):
    if not raw:
        return None
    url = raw.strip().strip('"').strip("'").strip()
    if url.lower().startswith("psql "):
        url = url[5:].strip().strip('"').strip("'").strip()
    url = re.sub(r"([?&])channel_binding=[^&]*&?", r"\1", url).rstrip("?&")
    if "sslmode=" not in url:
        url += ("&" if "?" in url else "?") + "sslmode=require"
    return url or None

DATABASE_URL = _clean_database_url(os.environ.get("DATABASE_URL"))

_last_db_error = None
_db_tables_ready = False
_db_down_until = 0.0

DB_SCHEMA_SQL = """
    CREATE TABLE IF NOT EXISTS user_chats (
        user_email TEXT,
        chat_id TEXT,
        title TEXT,
        messages JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (user_email, chat_id)
    );
    ALTER TABLE user_chats ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();
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
    CREATE TABLE IF NOT EXISTS user_settings (
        user_email TEXT PRIMARY KEY,
        data JSONB,
        updated_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS shared_chats (
        share_id TEXT PRIMARY KEY,
        owner TEXT,
        chat_id TEXT,
        title TEXT,
        messages JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
"""

def _ensure_tables(conn):
    global _db_tables_ready
    if _db_tables_ready:
        return
    try:
        with conn.cursor() as cur:
            cur.execute(DB_SCHEMA_SQL)
        conn.commit()
        _db_tables_ready = True
        print("PostgreSQL tables ready.")
    except Exception as e:
        print(f"Error creating PostgreSQL tables: {e}")
        try:
            conn.rollback()
        except Exception:
            pass

def get_db_connection():
    global _last_db_error, _db_down_until
    if not DATABASE_URL:
        return None
    if time.time() < _db_down_until:
        return None

    last_err = None
    for attempt in range(2):
        try:
            conn = psycopg2.connect(
                DATABASE_URL,
                connect_timeout=10,
                cursor_factory=psycopg2.extras.RealDictCursor,
                keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5,
            )
            _last_db_error = None
            _db_down_until = 0.0
            _ensure_tables(conn)
            return conn
        except Exception as e:
            last_err = e
            if attempt == 0:
                time.sleep(1.5)  # give a sleeping Neon database a moment to wake up

    _last_db_error = str(last_err)[:300]
    _db_down_until = time.time() + 20
    print(f"Database connection error: {last_err}")
    return None

def init_db():
    conn = get_db_connection()
    if conn:
        conn.close()
        print("PostgreSQL connected.")

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

# [SECURITY] Uploaded files are served by this route, never as a raw static folder.
# Only images/PDF open inline; everything else (html, svg, js...) is forced to DOWNLOAD,
# so an uploaded page can never run scripts on your domain.
_INLINE_UPLOAD_TYPES = {".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
                        ".gif": "image/gif", ".webp": "image/webp", ".pdf": "application/pdf"}

@app.get("/uploads/{filename}")
async def serve_upload(filename: str):
    safe = os.path.basename(filename)
    path = os.path.join(UPLOAD_DIR, safe)
    if safe != filename or not os.path.isfile(path):
        return Response(status_code=404)
    ext = os.path.splitext(safe)[1].lower()
    headers = {"X-Content-Type-Options": "nosniff", "Cache-Control": "private, max-age=3600"}
    if ext in _INLINE_UPLOAD_TYPES:
        return FileResponse(path, media_type=_INLINE_UPLOAD_TYPES[ext], headers=headers)
    return FileResponse(path, media_type="application/octet-stream", filename=safe, headers=headers)

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
# The old default secret was public on GitHub, which would let anyone forge
# an admin cookie. Set SESSION_SECRET on your host. If it's missing we generate a
# random one at startup (safe, but everyone gets logged out on each restart).
SESSION_SECRET = os.getenv("SESSION_SECRET")
if not SESSION_SECRET:
    print("[WARNING] SESSION_SECRET is not set — using a random one. Set it in your host's env vars so logins survive restarts.")
    SESSION_SECRET = secrets.token_urlsafe(48)
app.add_middleware(ProxyHeadersMiddleware, trusted_hosts="*")
# Secure cookies when running on Render (HTTPS); stays off for local http testing.
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET, https_only=bool(os.getenv("RENDER")), same_site="lax")

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
        response.set_cookie(key="guest_id", value=new_guest_id, max_age=31536000, httponly=True, samesite="lax", secure=bool(os.getenv("RENDER")))
    return response

# [SECURITY] Headers that stop clickjacking, MIME sniffing and referrer leaks
@app.middleware("http")
async def security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "SAMEORIGIN")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    response.headers.setdefault("Permissions-Policy", "camera=(self), microphone=(self), geolocation=()")
    if request.url.scheme == "https":
        response.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
    return response

# [SECURITY] Simple in-memory rate limiter (per IP). Resets when the server restarts.
_rate_buckets = {}

def rate_limited(key: str, limit: int, window: int) -> bool:
    now = time.time()
    if len(_rate_buckets) > 5000:  # keep memory small
        for k in [k for k, q in _rate_buckets.items() if not q or q[-1] < now - 3600]:
            _rate_buckets.pop(k, None)
    q = _rate_buckets.setdefault(key, [])
    while q and q[0] < now - window:
        q.pop(0)
    if len(q) >= limit:
        return True
    q.append(now)
    return False

def client_ip(request: Request) -> str:
    return request.client.host if request.client else "unknown"

# Upload / message limits
MAX_MESSAGE_CHARS = int(os.getenv("MAX_MESSAGE_CHARS", "8000"))
MAX_UPLOAD_BYTES = int(os.getenv("MAX_UPLOAD_MB", "10")) * 1024 * 1024
MAX_FILES_PER_MESSAGE = 5
ALLOWED_UPLOAD_EXT = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".heic", ".pdf", ".txt", ".md",
                      ".py", ".js", ".html", ".css", ".json", ".csv"}

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

print(f"[STARTUP] Groq key set: {bool(GROQ_API_KEY)} | Google key set: {bool(GOOGLE_API_KEY)} | Neon URL set: {bool(DATABASE_URL)}")

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

# --- OAUTH (old Google login via authlib; kept so /auth/callback still works) ---
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
    # Always mirror to MongoDB Atlas (background thread, never blocks the reply)
    mongo_save_chat(user_email, chat_id, title, messages)

    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO user_chats (user_email, chat_id, title, messages, updated_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (user_email, chat_id)
                    DO UPDATE SET title = EXCLUDED.title, messages = EXCLUDED.messages, updated_at = NOW();
                """, (user_email, str(chat_id), title, psycopg2.extras.Json(messages)))
                conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"DATABASE UPSERT ERROR: {e}")
            try:
                conn.close()
            except Exception:
                pass

    local_chats = load_local_chats()
    if user_email not in local_chats:
        local_chats[user_email] = {}
    local_chats[user_email][str(chat_id)] = {"title": title, "messages": messages}
    save_local_chats(local_chats)
    return True

def get_chat_record(val: str, session_id: str):
    """Read a chat: Neon first, then MongoDB Atlas, then the local file.
    Returns (messages, title). A failed Neon read no longer silently becomes an
    empty chat (which used to get saved over the real history)."""
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT messages, title FROM user_chats WHERE user_email = %s AND chat_id = %s",
                    (val, str(session_id))
                )
                row = cur.fetchone()
            conn.close()
            if row:
                msgs = row.get("messages") or []
                if isinstance(msgs, str):
                    msgs = json.loads(msgs)
                return msgs, (row.get("title") or "New Chat")
        except Exception as e:
            print(f"NEON READ ERROR: {e}")
            try:
                conn.close()
            except Exception:
                pass

    doc = mongo_get_chat(val, str(session_id))
    if doc:
        return doc.get("messages", []), (doc.get("title") or "New Chat")

    local = load_local_chats().get(val, {}).get(str(session_id), {})
    return local.get("messages", []), local.get("title", "New Chat")


# =====================================================================
# [NEW] SETTINGS (custom instructions, memory, pinned chats), SHARING,
#       IMAGE GENERATION, EXPORT / DELETE ACCOUNT, STREAMING HELPERS
# =====================================================================
SETTINGS_FILE = os.path.join(DATA_DIR, "settings.json")
SHARES_FILE = os.path.join(DATA_DIR, "shares.json")
DEFAULT_SETTINGS = {"custom_instructions": "", "memory_enabled": True, "memories": [], "pinned": []}
MAX_MEMORIES = 50

def _load_json_file(path):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def _save_json_file(path, data):
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f)
        os.replace(tmp, path)
    except Exception as e:
        print(f"Error saving {path}: {e}")

def get_user_settings(val: str) -> dict:
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT data FROM user_settings WHERE user_email = %s", (val,))
                row = cur.fetchone()
            conn.close()
            data = (row or {}).get("data") or {}
            if isinstance(data, str):
                data = json.loads(data)
            return {**DEFAULT_SETTINGS, **data}
        except Exception as e:
            print(f"SETTINGS READ ERROR: {e}")
            try:
                conn.close()
            except Exception:
                pass
    return {**DEFAULT_SETTINGS, **_load_json_file(SETTINGS_FILE).get(val, {})}

def save_user_settings(val: str, data: dict):
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO user_settings (user_email, data, updated_at) VALUES (%s, %s, NOW())
                    ON CONFLICT (user_email) DO UPDATE SET data = EXCLUDED.data, updated_at = NOW();
                """, (val, psycopg2.extras.Json(data)))
                conn.commit()
            conn.close()
            return
        except Exception as e:
            print(f"SETTINGS SAVE ERROR: {e}")
            try:
                conn.close()
            except Exception:
                pass
    all_settings = _load_json_file(SETTINGS_FILE)
    all_settings[val] = data
    _save_json_file(SETTINGS_FILE, all_settings)

def add_memory(val: str, text: str) -> bool:
    text = " ".join((text or "").split())[:300]
    if len(text) < 3:
        return False
    st = get_user_settings(val)
    if any(m.get("text", "").lower() == text.lower() for m in st["memories"]):
        return True
    st["memories"].append({"id": uuid.uuid4().hex[:8], "text": text})
    st["memories"] = st["memories"][-MAX_MEMORIES:]
    save_user_settings(val, st)
    return True

def build_user_context(st: dict) -> str:
    parts = []
    ci = (st.get("custom_instructions") or "").strip()
    if ci:
        parts.append("THE USER'S CUSTOM INSTRUCTIONS (follow them in every reply unless they are unsafe):\n" + ci[:1500])
    if st.get("memory_enabled", True):
        mems = [m.get("text", "") for m in st.get("memories", []) if m.get("text")][-MAX_MEMORIES:]
        if mems:
            parts.append("THINGS THE USER ASKED YOU TO REMEMBER ABOUT THEM (use them naturally when relevant):\n" + "\n".join("- " + t for t in mems))
    return ("\n\n" + "\n\n".join(parts)) if parts else ""

def _apply_pins(val: str, rows: list) -> list:
    pins = set(get_user_settings(val).get("pinned", []))
    for r in rows:
        r["is_pinned"] = 1 if r.get("id") in pins else 0
    rows.sort(key=lambda r: -r["is_pinned"])  # stable: pinned first, newest-first order kept inside each group
    return rows

def set_pinned(val: str, chat_id: str, pinned: bool):
    st = get_user_settings(val)
    pins = [c for c in st.get("pinned", []) if c != chat_id]
    if pinned:
        pins.insert(0, chat_id)
    st["pinned"] = pins[:100]
    save_user_settings(val, st)

# ---------- shared chat links ----------
def create_or_update_share(owner: str, chat_id: str, title: str, messages: list):
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT share_id FROM shared_chats WHERE owner = %s AND chat_id = %s", (owner, chat_id))
                row = cur.fetchone()
                if row:
                    share_id = row["share_id"]
                    cur.execute("UPDATE shared_chats SET title = %s, messages = %s WHERE share_id = %s",
                                (title, psycopg2.extras.Json(messages), share_id))
                else:
                    share_id = secrets.token_urlsafe(12)
                    cur.execute("INSERT INTO shared_chats (share_id, owner, chat_id, title, messages) VALUES (%s, %s, %s, %s, %s)",
                                (share_id, owner, chat_id, title, psycopg2.extras.Json(messages)))
                conn.commit()
            conn.close()
            return share_id
        except Exception as e:
            print(f"SHARE SAVE ERROR: {e}")
            try:
                conn.close()
            except Exception:
                pass
    shares = _load_json_file(SHARES_FILE)
    for sid, rec in shares.items():
        if rec.get("owner") == owner and rec.get("chat_id") == chat_id:
            rec["title"], rec["messages"] = title, messages
            _save_json_file(SHARES_FILE, shares)
            return sid
    sid = secrets.token_urlsafe(12)
    shares[sid] = {"owner": owner, "chat_id": chat_id, "title": title, "messages": messages}
    _save_json_file(SHARES_FILE, shares)
    return sid

def get_share(share_id: str):
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT title, messages FROM shared_chats WHERE share_id = %s", (share_id,))
                row = cur.fetchone()
            conn.close()
            if row:
                msgs = row.get("messages") or []
                if isinstance(msgs, str):
                    msgs = json.loads(msgs)
                return {"title": row.get("title") or "Shared chat", "messages": msgs}
        except Exception as e:
            print(f"SHARE READ ERROR: {e}")
            try:
                conn.close()
            except Exception:
                pass
    rec = _load_json_file(SHARES_FILE).get(share_id)
    if rec:
        return {"title": rec.get("title") or "Shared chat", "messages": rec.get("messages", [])}
    return None

def delete_share_for_chat(owner: str, chat_id: str):
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM shared_chats WHERE owner = %s AND chat_id = %s", (owner, str(chat_id)))
                conn.commit()
            conn.close()
        except Exception as e:
            print(f"SHARE DELETE ERROR: {e}")
            try:
                conn.close()
            except Exception:
                pass
    shares = _load_json_file(SHARES_FILE)
    gone = [sid for sid, rec in shares.items() if rec.get("owner") == owner and rec.get("chat_id") == str(chat_id)]
    if gone:
        for sid in gone:
            shares.pop(sid, None)
        _save_json_file(SHARES_FILE, shares)

def _render_share_text(text: str) -> str:
    parts = re.split(r"```[\w.+-]*\n?(.*?)```", text or "", flags=re.S)
    out = []
    for i, part in enumerate(parts):
        if i % 2 == 1:
            out.append(f"<pre><code>{html_lib.escape(part)}</code></pre>")
        elif part.strip():
            esc = html_lib.escape(part.strip())
            esc = re.sub(r"!\[([^\]]*)\]\((/uploads/[A-Za-z0-9_.\-]+)\)", r'<img src="\2" alt="\1">', esc)
            out.append(f"<div class='t'>{esc}</div>")
    return "".join(out)

def _list_chat_rows(val: str) -> list:
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT chat_id, title FROM user_chats WHERE user_email = %s", (val,))
                rows = cur.fetchall()
            conn.close()
            return [{"id": r["chat_id"], "title": r.get("title") or "Untitled Chat"} for r in rows]
        except Exception as e:
            print(f"LIST CHATS ERROR: {e}")
            try:
                conn.close()
            except Exception:
                pass
    rows = mongo_list_chats(val)
    if rows:
        return rows
    return [{"id": cid, "title": info.get("title", "")} for cid, info in load_local_chats().get(val, {}).items()]

# ---------- image generation ----------
IMAGE_MODEL = os.getenv("IMAGE_MODEL", "gemini-2.5-flash-image")
DAILY_IMAGE_LIMIT = int(os.getenv("DAILY_IMAGE_LIMIT", "8"))

_IMAGE_GEN_RE = re.compile(
    r"\b(generate|create|make|draw|design|paint|imagine|render|produce)\b[^.\n]{0,40}\b(image|picture|photo|logo|illustration|artwork|wallpaper|poster|icon|portrait|drawing|painting|sketch)s?\b",
    re.I)
_NOT_IMAGE_RE = re.compile(
    r"\b(html|css|website|web ?page|app|code|script|function|program|react|python|javascript|gallery|component|api|bot|game|caption|alt text|dataset|diagram|chart)\b",
    re.I)

def is_image_generation_request(message: str) -> bool:
    m = (message or "").strip()
    if not m:
        return False
    if re.match(r"^/(imagine|image)\b", m, re.I):
        return True
    if _NOT_IMAGE_RE.search(m):
        return False
    return bool(_IMAGE_GEN_RE.search(m) or re.match(r"^draw\b", m, re.I))

async def generate_image(prompt: str):
    """Returns (bytes, mime) or None. Tries Gemini first, then the free Pollinations service."""
    if genai_client and IMAGE_MODEL:
        try:
            def _gemini():
                resp = genai_client.models.generate_content(
                    model=IMAGE_MODEL, contents=prompt,
                    config=types.GenerateContentConfig(response_modalities=["TEXT", "IMAGE"]),
                )
                for cand in (resp.candidates or []):
                    for part in (cand.content.parts or []):
                        inline = getattr(part, "inline_data", None)
                        if inline and inline.data:
                            return inline.data, (inline.mime_type or "image/png")
                return None
            result = await asyncio.to_thread(_gemini)
            if result:
                return result
        except Exception as e:
            print(f"[IMAGE] Gemini failed: {str(e)[:200]}")
    try:
        url = "https://image.pollinations.ai/prompt/" + quote(prompt) + "?width=1024&height=1024&nologo=true"
        async with httpx.AsyncClient(timeout=70, follow_redirects=True) as client:
            r = await client.get(url)
        ctype = (r.headers.get("content-type") or "").split(";")[0].strip()
        if r.status_code == 200 and ctype.startswith("image/") and len(r.content) > 2000:
            return r.content, ctype
    except Exception as e:
        print(f"[IMAGE] Pollinations failed: {str(e)[:200]}")
    return None

def _record_asset(val: str, file_name: str, rel_path: str, mime: str):
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO assets (user_email, file_name, file_path, file_type) VALUES (%s, %s, %s, %s)",
                            (val, file_name, rel_path, mime))
                conn.commit()
            conn.close()
        except Exception as e:
            print(f"Error saving asset to DB: {e}")
            try:
                conn.close()
            except Exception:
                pass

# =====================================================================
# [NEW] API: settings / memory / pin / rename / feedback / branch / share / export / delete
# =====================================================================
@app.get("/api/settings")
async def api_get_settings(request: Request):
    col, val = get_identifier(request)
    st = get_user_settings(val)
    return {"custom_instructions": st["custom_instructions"], "memory_enabled": bool(st["memory_enabled"]), "memories": st["memories"]}

@app.post("/api/settings")
async def api_save_settings(request: Request):
    col, val = get_identifier(request)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Bad request"}, status_code=400)
    st = get_user_settings(val)
    if "custom_instructions" in body:
        st["custom_instructions"] = str(body["custom_instructions"])[:1500]
    if "memory_enabled" in body:
        st["memory_enabled"] = bool(body["memory_enabled"])
    save_user_settings(val, st)
    return {"status": "success"}

@app.post("/api/memory")
async def api_add_memory(request: Request):
    col, val = get_identifier(request)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Bad request"}, status_code=400)
    if not add_memory(val, str(body.get("text", ""))):
        return JSONResponse({"error": "Write a little more than that."}, status_code=400)
    return {"status": "success", "memories": get_user_settings(val)["memories"]}

@app.delete("/api/memory/{memory_id}")
async def api_delete_memory(request: Request, memory_id: str):
    col, val = get_identifier(request)
    st = get_user_settings(val)
    st["memories"] = [m for m in st["memories"] if m.get("id") != memory_id]
    save_user_settings(val, st)
    return {"status": "success", "memories": st["memories"]}

@app.delete("/api/memory")
async def api_clear_memory(request: Request):
    col, val = get_identifier(request)
    st = get_user_settings(val)
    st["memories"] = []
    save_user_settings(val, st)
    return {"status": "success", "memories": []}

@app.post("/api/rename-session")
async def api_rename_session(request: Request, session_id: str = Form(...), title: str = Form(...)):
    col, val = get_identifier(request)
    title = " ".join(title.split())[:60]
    if not title:
        return JSONResponse({"error": "Title can't be empty"}, status_code=400)
    msgs, _old = get_chat_record(val, session_id)
    save_chat_history(user_email=val, chat_id=str(session_id), title=title, messages=msgs)
    return {"status": "success", "title": title}

@app.post("/api/pin-session")
async def api_pin_session(request: Request, session_id: str = Form(...), pinned: str = Form("1")):
    col, val = get_identifier(request)
    set_pinned(val, str(session_id), pinned == "1")
    return {"status": "success"}

@app.post("/api/feedback")
async def api_feedback(request: Request, session_id: str = Form(...), message_index: int = Form(...), value: str = Form(...)):
    col, val = get_identifier(request)
    msgs, title = get_chat_record(val, session_id)
    if message_index < 0 or message_index >= len(msgs) or msgs[message_index].get("role") != "assistant":
        return JSONResponse({"error": "Message not found"}, status_code=400)
    if value in ("up", "down"):
        msgs[message_index]["feedback"] = value
    else:
        msgs[message_index].pop("feedback", None)
    save_chat_history(user_email=val, chat_id=str(session_id), title=title, messages=msgs)
    return {"status": "success"}

@app.post("/api/branch-session")
async def api_branch_session(request: Request, session_id: str = Form(...), upto_index: int = Form(...)):
    """Edit-a-message: copy the chat up to (not including) that message into a NEW chat.
    The original conversation is left untouched."""
    col, val = get_identifier(request)
    msgs, title = get_chat_record(val, session_id)
    if upto_index < 0 or upto_index > len(msgs):
        return JSONResponse({"error": "Message not found"}, status_code=400)
    kept = [{k: v for k, v in m.items() if k != "feedback"} for m in msgs[:upto_index]]
    new_id = str(uuid.uuid4())
    new_title = ((title or "New Chat") + " (edited)")[:60]
    save_chat_history(user_email=val, chat_id=new_id, title=new_title, messages=kept)
    return {"session_id": new_id, "title": new_title}

@app.post("/api/share-session")
async def api_share_session(request: Request, session_id: str = Form(...)):
    col, val = get_identifier(request)
    if rate_limited(f"share:{client_ip(request)}", 20, 60):
        return JSONResponse({"error": "Too many requests. Try again in a minute."}, status_code=429)
    msgs, title = get_chat_record(val, session_id)
    if not msgs:
        return JSONResponse({"error": "Send a message first, then share the chat."}, status_code=400)
    snapshot = [{"role": m.get("role"), "content": m.get("content", "")} for m in msgs]
    share_id = create_or_update_share(val, str(session_id), title, snapshot)
    return {"url": f"/s/{share_id}"}

@app.post("/api/unshare-session")
async def api_unshare_session(request: Request, session_id: str = Form(...)):
    col, val = get_identifier(request)
    delete_share_for_chat(val, str(session_id))
    return {"status": "success"}

@app.get("/s/{share_id}", response_class=HTMLResponse)
async def view_shared_chat(share_id: str):
    if not re.fullmatch(r"[A-Za-z0-9_-]{6,40}", share_id):
        return HTMLResponse("<h3>Link not found.</h3>", status_code=404)
    data = get_share(share_id)
    if not data:
        return HTMLResponse("<h3>This shared chat doesn't exist or was removed.</h3>", status_code=404)
    blocks = []
    for m in data["messages"]:
        role = "user" if m.get("role") == "user" else "assistant"
        label = "You" if role == "user" else "Ranen"
        blocks.append(f"<div class='msg {role}'><div class='who'>{label}</div>{_render_share_text(m.get('content', ''))}</div>")
    title = html_lib.escape(data["title"])
    return HTMLResponse(f"""<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><meta name="robots" content="noindex,nofollow">
<title>{title} - Ranen</title>
<style>
body{{background:#000;color:#e5e7eb;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;margin:0;line-height:1.65}}
.wrap{{max-width:760px;margin:0 auto;padding:28px 18px 80px}}
h1{{font-size:1.25rem;margin:0 0 4px}} .sub{{color:#9ca3af;font-size:.8rem;margin-bottom:22px}}
.msg{{border:1px solid #232326;border-radius:18px;padding:14px 16px;margin-bottom:12px;background:#111113}}
.msg.user{{background:#1c1c1f}} .who{{font-size:.7rem;text-transform:uppercase;letter-spacing:.06em;color:#9ca3af;margin-bottom:6px}}
.t{{white-space:pre-wrap;word-wrap:break-word}} pre{{background:#0a0a0b;border:1px solid #232326;border-radius:12px;padding:12px;overflow:auto;font-size:.82rem}}
img{{max-width:100%;border-radius:14px;margin-top:8px}} a{{color:#60a5fa}}
.cta{{display:inline-block;margin-top:18px;padding:10px 18px;border-radius:999px;background:#fff;color:#000;text-decoration:none;font-weight:700}}
</style></head><body><div class="wrap">
<h1>{title}</h1><div class="sub">A shared conversation from Ranen (read-only)</div>
{''.join(blocks)}
<a class="cta" href="/">Try Ranen</a></div></body></html>""")

@app.get("/api/export")
async def api_export(request: Request):
    col, val = get_identifier(request)
    if rate_limited(f"export:{client_ip(request)}", 5, 60):
        return JSONResponse({"error": "Too many requests"}, status_code=429)
    chats = []
    for row in _list_chat_rows(val):
        msgs, title = get_chat_record(val, row["id"])
        chats.append({"id": row["id"], "title": title, "messages": msgs})
    st = get_user_settings(val)
    payload = {
        "exported_at": datetime.now().isoformat(),
        "app": "Ranen",
        "settings": {"custom_instructions": st["custom_instructions"], "memory_enabled": st["memory_enabled"], "memories": st["memories"]},
        "chats": chats,
    }
    return JSONResponse(payload, headers={"Content-Disposition": 'attachment; filename="ranen-export.json"'})

@app.post("/api/delete-account")
async def api_delete_account(request: Request, confirm: str = Form("")):
    col, val = get_identifier(request)
    if confirm != "DELETE":
        return JSONResponse({"error": "Type DELETE to confirm."}, status_code=400)
    rows = _list_chat_rows(val)
    for row in rows:
        mongo_delete_chat(val, row["id"])
    file_paths = []
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT file_path FROM assets WHERE user_email = %s", (val,))
                file_paths = [r["file_path"] for r in cur.fetchall()]
                for table in ("user_chats", "gems", "assets", "user_settings", "shared_chats"):
                    owner_col = "owner" if table == "shared_chats" else "user_email"
                    cur.execute(f"DELETE FROM {table} WHERE {owner_col} = %s", (val,))
                conn.commit()
            conn.close()
        except Exception as e:
            print(f"DELETE ACCOUNT ERROR: {e}")
            try:
                conn.close()
            except Exception:
                pass
    for fp in file_paths:
        try:
            path = os.path.join(UPLOAD_DIR, os.path.basename(fp or ""))
            if os.path.isfile(path):
                os.remove(path)
        except Exception:
            pass
    local = load_local_chats()
    if val in local:
        del local[val]
        save_local_chats(local)
    all_settings = _load_json_file(SETTINGS_FILE)
    if val in all_settings:
        del all_settings[val]
        _save_json_file(SETTINGS_FILE, all_settings)
    shares = _load_json_file(SHARES_FILE)
    gone = [sid for sid, rec in shares.items() if rec.get("owner") == val]
    for sid in gone:
        shares.pop(sid, None)
    if gone:
        _save_json_file(SHARES_FILE, shares)
    request.session.clear()
    return {"status": "deleted"}

# --- ROUTES ---
@app.get("/google0b211ab21a1539ad.html", response_class=HTMLResponse)
async def google_verification():
    return "google-site-verification: google0b211ab21a1539ad.html"

@app.get("/sitemap.xml", response_class=Response)
async def sitemap():
    sitemap_content = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
    <url><loc>https://ranen.duckdns.org/</loc><changefreq>daily</changefreq><priority>1.0</priority></url>
    <url><loc>https://ranen.duckdns.org/about</loc><changefreq>monthly</changefreq><priority>0.8</priority></url>
    <url><loc>https://ranen.duckdns.org/terms</loc><changefreq>monthly</changefreq><priority>0.3</priority></url>
    <url><loc>https://ranen.duckdns.org/privacy</loc><changefreq>monthly</changefreq><priority>0.3</priority></url>
</urlset>"""
    return Response(content=sitemap_content, media_type="application/xml")

# [NEW] robots.txt — helps Google find the sitemap and skip the private parts
@app.get("/robots.txt", response_class=Response)
async def robots_txt():
    body = (
        "User-agent: *\n"
        "Allow: /\n"
        "Disallow: /admin\n"
        "Disallow: /api/\n"
        "Disallow: /auth/\n"
        "Disallow: /s/\n"
        "\n"
        "Sitemap: https://ranen.duckdns.org/sitemap.xml\n"
    )
    return Response(content=body, media_type="text/plain")

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
    response.headers["Cache-Control"] = "no-store, max-age=0"
    if not request.cookies.get("guest_id"):
        guest_id = str(uuid.uuid4())
        response.set_cookie(key="guest_id", value=guest_id, max_age=31536000, httponly=True, samesite="lax", secure=bool(os.getenv("RENDER")))
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

    <p>These Terms govern your use of Ranen ("the Service"), an AI assistant platform built and operated by Nwodili Yaemerie Convenant and managed by Tinu Tom. By using Ranen, you agree to these Terms. If you do not agree, please do not use the Service.</p>

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
        <li><b>Account info:</b> if you sign in with Google, we receive your name and email address via Firebase Authentication.</li>
        <li><b>Guest identifiers:</b> a random cookie ID for anonymous sessions, with no personal info attached.</li>
        <li><b>Basic technical logs:</b> connection metadata (e.g., timestamps, error logs) used for debugging and abuse prevention.</li>
    </ul>

    <h2>2. How We Use Your Data</h2>
    <ul>
        <li>To generate AI responses to your messages.</li>
        <li>To maintain chat history so conversations persist across sessions.</li>
        <li>To improve reliability and fix bugs.</li>
    </ul>

    <h2>3. Third-Party Providers</h2>
    <p>Your prompts, uploaded files, and images are sent to third-party AI providers (Groq and Google Gemini) to generate responses. Each provider processes this data under its own privacy and data-handling terms. We do not control how these providers internally process requests beyond what their APIs document. Sign-in is handled by Firebase Authentication (Google).</p>

    <h2>4. Data Storage</h2>
    <p>Chat history is stored in a PostgreSQL database (Neon) with a backup copy in MongoDB Atlas, and uploaded files are stored on our servers, to provide session continuity. This data persists until you delete a chat or request deletion of your account data.</p>

    <h2>5. Data We Do Not Sell</h2>
    <p>We do not sell your personal data, chat histories, or uploaded files to advertisers or data brokers.</p>

    <h2>6. Your Choices</h2>
    <ul>
        <li>You can delete individual chats at any time from the sidebar.</li>
        <li>You can use Ranen as a guest without creating an account.</li>
        <li>You can export or permanently delete all your data yourself from Personalization inside Ranen, or contact us (see below).</li>
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
    # Manager (only the name and role are published — no age or location)
    manager_answer = (
        "Tinu Tom is the manager of Ranen and handles its business administration, working "
        "alongside its creator Nwodili Yaemerie Convenant."
    )
    who_manages_answer = (
        "Ranen is managed by Tinu Tom, who works alongside its creator, Nwodili Yaemerie Convenant."
    )
    return f"""
    <html><head>
    <title>Nwodili Yaemerie Convenant &amp; Tinu Tom — Creator and Manager of Ranen</title>
    <meta name="description" content="{direct_answer} Managed by Tinu Tom.">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link rel="canonical" href="https://ranen.duckdns.org/about">
    <meta property="og:type" content="profile">
    <meta property="og:title" content="Nwodili Yaemerie Convenant &amp; Tinu Tom — Creator and Manager of Ranen">
    <meta property="og:description" content="Ranen is built by Nwodili Yaemerie Convenant and managed by Tinu Tom.">
    <meta property="og:url" content="https://ranen.duckdns.org/about">
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
        }},
        {{
          "@type": "Question",
          "name": "Who is Tinu Tom?",
          "acceptedAnswer": {{ "@type": "Answer", "text": "{manager_answer}" }}
        }},
        {{
          "@type": "Question",
          "name": "Who manages Ranen?",
          "acceptedAnswer": {{ "@type": "Answer", "text": "{who_manages_answer}" }}
        }}
      ]
    }}
    </script>
    <script type="application/ld+json">
    {{
      "@context": "https://schema.org",
      "@type": "Organization",
      "name": "Ranen",
      "url": "https://ranen.duckdns.org/",
      "founder": {{
        "@type": "Person",
        "name": "Nwodili Yaemerie Convenant",
        "sameAs": ["https://github.com/whitefrostff-dev"]
      }},
      "employee": {{
        "@type": "Person",
        "name": "Tinu Tom",
        "jobTitle": "Manager"
      }}
    }}
    </script>
    <script type="application/ld+json">
    {{
      "@context": "https://schema.org",
      "@type": "Person",
      "name": "Tinu Tom",
      "jobTitle": "Manager",
      "description": "Tinu Tom is the manager of Ranen and handles its business administration.",
      "url": "https://ranen.duckdns.org/about",
      "worksFor": {{
        "@type": "Organization",
        "name": "Ranen",
        "url": "https://ranen.duckdns.org/"
      }}
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

    <h2>What does Nwodili work on?</h2>
    <p>
        Nwodili builds full-stack web applications and AI-powered tools, with a particular interest in
        ethical hacking and vulnerability research alongside his development work.
    </p>
    <ul>
        <li>Cybersecurity &amp; ethical hacking fundamentals</li>
        <li>Full-stack web development (Python/FastAPI, JavaScript)</li>
        <li>Building AI-powered tools and assistants — including Ranen</li>
        <li>Linux system administration</li>
    </ul>

    <h2>Who is Tinu Tom?</h2>
    <div class="profile-header">
        <div class="profile-avatar">TT</div>
        <div>
            <h1 style="margin-bottom:2px; font-size:1.3rem;">Tinu Tom</h1>
            <p style="color:#9ca3af; font-size:0.9rem; margin:0;">Manager of Ranen</p>
        </div>
    </div>
    <p class="direct-answer">{manager_answer}</p>

    <div class="fact-grid">
        <div class="fact-item"><div class="fact-label">Role</div><div class="fact-value">Manager</div></div>
        <div class="fact-item"><div class="fact-label">Field</div><div class="fact-value">Business Administration</div></div>
        <div class="fact-item"><div class="fact-label">Works on</div><div class="fact-value">Ranen</div></div>
    </div>

    <h2>Who manages Ranen?</h2>
    <p class="direct-answer">{who_manages_answer}</p>

    <h2>What is Ranen?</h2>
    <p class="direct-answer">{what_is_ranen_answer}</p>

    <h2>Links</h2>
    <ul>
        <li><a href="https://ranen.duckdns.org/">Ranen — the AI assistant</a></li>
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

# Debug endpoints are admin-only. /api/debug/providers fires real requests at Groq/Gemini.
@app.get("/api/debug/storage")
async def debug_storage(request: Request):
    require_admin(request)
    conn = get_db_connection()
    db_connected = conn is not None
    if conn:
        conn.close()
    db_host = None
    if DATABASE_URL:
        try:
            db_host = urlparse(DATABASE_URL).hostname
        except Exception:
            db_host = "unreadable"
    return {
        "database_url_configured": bool(DATABASE_URL),
        "database_host": db_host,
        "using_neon_pooler": bool(db_host and "-pooler" in db_host),
        "database_currently_reachable": db_connected,
        "tables_ready": _db_tables_ready,
        "last_database_error": _last_db_error,
        "using": "postgres" if db_connected else "local_file_fallback",
        "mongodb_uri_configured": bool(MONGODB_URI),
        "firebase_ready": firebase_ready,
        "local_fallback_path": CHATS_FILE,
        "warning": None if db_connected else
            "Neon is unreachable. Chats are being read from MongoDB Atlas if available, otherwise from ephemeral local disk. See last_database_error."
    }

@app.get("/api/debug/providers")
async def debug_providers(request: Request):
    """Admin-only. Open /api/debug/providers while logged in as the admin to see
    EXACTLY why Groq or Gemini is failing. Never reveals the keys."""
    require_admin(request)

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

# --- AUTH ---
# Login happens in the browser through Firebase (popup). The old /auth/login
# link just sends people back to the page and opens the login popup.
@app.get('/auth/login')
async def login():
    return RedirectResponse(url="/?login=1")

# [NEW] Firebase login helper pages, served through OUR domain.
# Some networks block ranen-11fe1.firebaseapp.com. With authDomain set to ranen.duckdns.org
# in index.html, the browser asks us for /__/auth/handler and we fetch it from Google for it.
@app.api_route("/__/{kind}/{path:path}", methods=["GET", "POST"])
async def firebase_auth_proxy(kind: str, path: str, request: Request):
    if kind not in ("auth", "firebase"):
        return Response(status_code=404)
    url = f"https://ranen-11fe1.firebaseapp.com/__/{kind}/{path}"
    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=20) as client:
            r = await client.request(
                request.method, url,
                params=dict(request.query_params),
                content=await request.body(),
            )
    except Exception as e:
        print(f"Firebase auth proxy failed: {e}")
        return Response(content="Login helper is temporarily unavailable.", status_code=502)
    return Response(content=r.content, status_code=r.status_code,
                    media_type=r.headers.get("content-type"))

@app.post("/auth/firebase")
async def firebase_login(request: Request):
    if rate_limited(f"login:{client_ip(request)}", 10, 60):
        return JSONResponse({"error": "Too many login attempts. Wait a minute."}, status_code=429)
    if not firebase_ready:
        return JSONResponse({"error": "Firebase is not configured on the server"}, status_code=500)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Bad request"}, status_code=400)
    try:
        decoded = await asyncio.to_thread(verify_firebase_token, body.get("id_token", ""))
    except Exception as e:
        print(f"Firebase token verify failed: {e}")
        return JSONResponse({"error": "Invalid token"}, status_code=401)

    email = decoded.get("email")
    # Chats are keyed by email, so an unverified email must never be accepted.
    if not email or not decoded.get("email_verified"):
        return JSONResponse({"error": "A verified email is required"}, status_code=401)

    request.session['user'] = {
        'name': decoded.get('name') or email.split('@')[0],
        'email': email,
        'email_verified': True,
        'uid': decoded.get('uid'),
    }
    return {"status": "success"}

# Old authlib callback (unused now, safe to delete later)
@app.get('/auth/callback')
async def auth(request: Request):
    try:
        token = await oauth.google.authorize_access_token(request)
        user_info = token.get('userinfo')
        if user_info:
            # [ADMIN] email_verified is stored so the admin check can reject unverified emails
            request.session['user'] = {
                'name': user_info.get('name'),
                'email': user_info.get('email'),
                'email_verified': user_info.get('email_verified', False),
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
        response.set_cookie(key="guest_id", value=guest_id, max_age=31536000, httponly=True, samesite="lax", secure=bool(os.getenv("RENDER")))
    return response

@app.get("/api/sessions")
async def get_user_sessions(request: Request):
    col, val = get_identifier(request)
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT chat_id, title FROM user_chats WHERE user_email = %s ORDER BY updated_at DESC NULLS LAST, created_at DESC", (val,))
                rows = cur.fetchall()
            conn.close()
            return _apply_pins(val, [{"id": r["chat_id"], "title": r.get("title") or "Untitled Chat", "is_pinned": 0} for r in rows])
        except Exception as e:
            print(f"DATABASE FETCH SESSIONS ERROR: {e}")
            try:
                conn.close()
            except Exception:
                pass

    # Neon unreachable -> try MongoDB Atlas, then the local file
    mongo_rows = mongo_list_chats(val)
    if mongo_rows:
        return _apply_pins(val, mongo_rows)

    local_chats = load_local_chats()
    user_data = local_chats.get(val, {})
    return _apply_pins(val, [{"id": cid, "title": info["title"], "is_pinned": 0} for cid, info in user_data.items()])

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
    msgs, _title = get_chat_record(val, session_id)
    return msgs

@app.post("/api/new-session")
async def create_new_session(request: Request):
    col, val = get_identifier(request)
    new_chat_id = str(uuid.uuid4())
    save_chat_history(user_email=val, chat_id=new_chat_id, title="New Chat", messages=[])
    return {"session_id": new_chat_id}

@app.delete("/api/delete-session/{session_id}")
async def delete_session(request: Request, session_id: str):
    col, val = get_identifier(request)

    # remove the Atlas copy too
    mongo_delete_chat(val, session_id)
    delete_share_for_chat(val, str(session_id))
    try:
        set_pinned(val, str(session_id), False)
    except Exception:
        pass

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
            "description": "Standard elite assistant created by Nwodili Yaemerie Convenant and managed by Tinu Tom",
            "system_prompt": DEFAULT_SYSTEM_PROMPT,
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

# SERVER-SIDE SEARCH ROUTER
# The old setup relied on the model choosing to call the search tool, and smaller models
# almost never did. Now the server decides: if the message looks like it needs fresh
# information, it searches first and puts the results into the prompt. This works with
# every model (Groq or Gemini). The tool-calling path below stays as a backup.
_SEARCH_PATTERNS = [re.compile(p, re.I) for p in [
    r"\b(search|google|look ?up|browse)\b",
    r"\b(latest|newest|current(ly)?|recent(ly)?|breaking|trending|up[- ]to[- ]date)\b",
    r"\b(today|tonight|yesterday|this (week|month|year)|right now|as of now)\b",
    r"\bnews\b|\bheadlines\b",
    r"\b(score|scores|fixtures?|standings|who won|match result)\b",
    r"\b(weather|forecast)\b",
    r"\b(price|prices|cost)\b.*\b(of|for|in)\b",
    r"\b(stock|stocks|crypto|bitcoin|btc|ethereum|exchange rate|naira|dollar)\b",
    r"\bwho is (the )?(current |new )?(president|prime minister|ceo|governor|minister|champion)\b",
    r"\b(release date|when (is|was|does|did).{0,40}(release|launch|come out))\b",
    r"\b202[5-9]\b",
]]

def needs_web_search(message: str, has_files: bool = False) -> bool:
    if has_files or "```" in message or len(message.strip()) < 6:
        return False
    return any(p.search(message) for p in _SEARCH_PATTERNS)

# [NEW] COMPLEX-PROBLEM DETECTOR
# Hard questions (math, logic, algorithms, debugging, design, long messages) get the
# strongest reasoning setting, a lower temperature and a bigger answer budget.
_COMPLEX_RE = re.compile(
    r"\b(solve|prove|proof|derive|calculate|compute|simplify|integrate|differentiate|optimi[sz]e|"
    r"algorithm|complexity|big-?o|debug|refactor|architecture|system design|design a|implement|"
    r"step[- ]by[- ]step|why does|why is|explain why|how would you|trade-?offs?|"
    r"integral|derivative|equation|matrix|probability|theorem|puzzle|riddle|logic|regex|sql|"
    r"statistics|physics|chemistry|calculus|algebra)\b"
    r"|[=^∫∑√]|\d\s*[+\-*/×÷]\s*\d",
    re.I,
)

def is_complex_request(message: str) -> bool:
    m = message or ""
    if "```" in m or len(m) > 350:
        return True
    return bool(_COMPLEX_RE.search(m))

async def build_effective_message(message: str, file_text_content: str = "") -> str:
    """The text the model actually sees: the user's message + today's date
    (+ live web results when needed) (+ uploaded file text). Only the plain
    message is saved in the chat history, never the search results."""
    date_str = datetime.now().strftime("%A, %B %d, %Y")
    out = f"{message}\n\n[Current date: {date_str}]"

    if is_complex_request(message):
        out += (
            "\n\n[This looks like a hard problem. Work through it carefully step by step, "
            "double-check your result (re-derive it or test it with an example), fix any mistake "
            "you find, and only then give a clear final answer.]"
        )

    if ENABLE_WEB_SEARCH and needs_web_search(message, bool(file_text_content)):
        query = re.sub(
            r"(?i)\b(please|can you|could you|search( the web| the internet)?( for)?|google|look up)\b",
            "", message
        )
        query = " ".join(query.split())[:150] or message[:150]
        results = await asyncio.to_thread(_perform_web_search, query)
        bad_starts = ("Web search failed", "Web search is unavailable", "No results")
        if results and not results.startswith(bad_starts):
            out += (
                "\n\n[LIVE WEB SEARCH RESULTS — fetched just now. Prefer these over your training "
                "data, mention the source names, and say so if they don't fully answer the question.]\n"
                f"{results}\n[END SEARCH RESULTS]"
            )

    if file_text_content:
        out += file_text_content
    return out

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

# Long code (full pages/apps) was getting cut off at 3072 tokens. Lower these on Render if Groq complains about limits.
MAX_REPLY_TOKENS = int(os.getenv("MAX_REPLY_TOKENS", "6000"))
MAX_COMPLEX_TOKENS = int(os.getenv("MAX_COMPLEX_TOKENS", "7000"))

def _groq_kwargs(model: str, complex_mode: bool) -> dict:
    """[NEW] Per-model generation settings. The gpt-oss models are reasoning models:
    their 'thinking' counts against max_tokens, so the old 2048 limit could cut them off
    before they answered. They also accept reasoning_effort (low / medium / high)."""
    kw = {
        "temperature": 0.3 if complex_mode else 0.7,
        "max_tokens": MAX_COMPLEX_TOKENS if complex_mode else MAX_REPLY_TOKENS,
    }
    if "gpt-oss" in model:
        kw["reasoning_effort"] = "high" if complex_mode else "medium"
    return kw

async def _call_groq_with_tools(model, base_messages_payload, complex_mode=False):
    """Tool-calling first. If the tool round-trip breaks for a NON-quota reason,
    fall back to a plain call on the same model. Rate-limit/auth errors are
    re-raised so the model chain can move on to the next model."""
    kw = _groq_kwargs(model, complex_mode)
    if not ENABLE_WEB_SEARCH:
        plain = await asyncio.to_thread(
            groq_client.chat.completions.create,
            model=model, messages=base_messages_payload, **kw,
        )
        return _groq_text(plain.choices[0].message)
    try:
        messages_payload = list(base_messages_payload)
        first = await asyncio.to_thread(
            groq_client.chat.completions.create,
            model=model, messages=messages_payload, **kw,
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
            model=model, messages=messages_payload, **kw,
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
            model=model, messages=base_messages_payload, **kw,
        )
        return _groq_text(plain.choices[0].message)

async def _call_google_with_tools(model, system_prompt, base_contents, complex_mode=False):
    temperature = 0.4 if complex_mode else 0.7
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
        config = types.GenerateContentConfig(system_instruction=system_prompt, temperature=temperature, tools=[tool])
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
        config = types.GenerateContentConfig(system_instruction=system_prompt, temperature=temperature)
        resp = await asyncio.to_thread(genai_client.models.generate_content, model=model, contents=base_contents, config=config)
        return resp.text

async def _call_provider(provider, model, system_prompt, recent_history, effective_message, is_image, file_bytes, mime_type, complex_mode=False):
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
            config = types.GenerateContentConfig(system_instruction=system_prompt, temperature=0.4 if complex_mode else 0.7)
            resp = await asyncio.to_thread(genai_client.models.generate_content, model=model, contents=contents, config=config)
            return resp.text

        contents.append(effective_message)
        return await _call_google_with_tools(model, system_prompt, contents, complex_mode)

    # groq
    if not groq_client:
        raise RuntimeError("GROQ_API_KEY is missing from environment variables.")
    messages_payload = [{"role": "system", "content": system_prompt}]
    for msg in recent_history[:-1]:
        messages_payload.append({"role": msg["role"], "content": msg["content"]})
    messages_payload.append({"role": "user", "content": effective_message})
    return await _call_groq_with_tools(model, messages_payload, complex_mode)

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

# [UPDATED] Facts the AI knows about its own team. Tinu Tom's age, location and other personal
# details are deliberately NOT included, so the AI can't repeat them.
ABOUT_FACTS = (
    "FACTS ABOUT RANEN (use these when someone asks about Ranen, who built it, or who runs it):\n"
    "- Ranen is an AI assistant platform created by Nwodili Yaemerie Convenant, a cybersecurity "
    "student and web developer from Anambra State, Nigeria, studying at Abia State University.\n"
    "- Ranen is managed by Tinu Tom, the manager of Ranen, who handles its business administration "
    "and works alongside the creator. If someone asks 'Who is Tinu Tom?', answer: 'Tinu Tom is the "
    "manager of Ranen, working alongside its creator, Nwodili Yaemerie Convenant.'\n"
    "- Ranen's website is https://ranen.duckdns.org/ and the about page is https://ranen.duckdns.org/about. "
    "Contact: whitefrostff@gmail.com.\n"
    "- Personal details about Tinu Tom beyond his name and role (such as his age or where he lives) "
    "are private. If asked, politely say you don't share personal details about people and point "
    "to the about page for what is public."
)

CORE_SYSTEM_PROMPT = (
    "You are Ranen, an elite AI assistant created by Nwodili Yaemerie Convenant and managed by Tinu Tom. "
    "CORE DIRECTIVES:\n"
    "1. Reason like a top-tier frontier model. For anything beyond a trivial question, work it out "
    "carefully BEFORE you answer: restate what is really being asked, break the problem into parts, "
    "consider more than one approach on hard problems, and pick the one that holds up. Then verify: "
    "re-derive the result, plug numbers back in, test edge cases, and fix any mistake you find. "
    "Never guess when you can reason it out. If something is genuinely uncertain, say exactly what "
    "you're unsure about instead of bluffing.\n"
    "2. Math, logic, science and puzzles: show the key steps clearly and in order, keep the "
    "arithmetic exact, and put the final answer on its own line so it's easy to find.\n"
    "3. Code: write complete, correct, runnable code. Mentally run it on at least one example and "
    "think about edge cases before you send it. When debugging, find the root cause, explain it in "
    "a sentence or two, then give the fixed code.\n"
    "4. Act natural and humanoid. Speak like a real developer/peer, not a machine.\n"
    "5. BE CONCISE by default. No filler, no repeating the question, no unnecessary preamble. "
    "But when a question is genuinely technical or multi-step, give it the depth it needs — "
    "concise does not mean shallow.\n"
    "6. Eliminate robotic filler, meta-commentary, and empty politeness.\n"
    "7. Identity: If asked who made you or what your name is, state clearly: 'I am Ranen, created by "
    "Nwodili Yaemerie Convenant.' If asked who manages Ranen, say it is managed by Tinu Tom.\n"
    "8. You have live web access. When a message contains a block marked LIVE WEB SEARCH RESULTS, "
    "those results were fetched just now — use them as your primary source, mention the source "
    "names, and never say you can't access the internet. You also have a web_search tool: use it "
    "whenever a question depends on current events, prices, recent releases, or anything that "
    "could have changed since your training. Don't use it for timeless facts, math, or general "
    "knowledge you already know.\n"
    "9. When asked to build a website, app, or any code project: pick the best approach "
    "yourself and build it directly — never respond with a list of design options or ask which "
    "style the person wants before writing code. State any assumption in one line if needed, "
    "then deliver complete, working, production-quality output in a single response — no "
    "placeholders, no 'add your logic here' stubs, no half-finished sections.\n"
    "10. Never use inline style=\"...\" attributes on HTML elements. Put CSS in a single "
    "organized <style> block (single-file output) or one shared external stylesheet (multi-file "
    "output, e.g. Flask templates) — never duplicate a <style> block across multiple pages of the "
    "same project. Use semantic HTML and make it visually polished by default (real layout, "
    "spacing, and color choices) even if the person didn't specify a design — never ship "
    "something plain or unfinished unless they explicitly asked for bare-bones."
)

DEFAULT_SYSTEM_PROMPT = CORE_SYSTEM_PROMPT + "\n\n" + ABOUT_FACTS

def build_system_prompt(gem_prompt: Optional[str]) -> str:
    """Custom personas (gems) keep their own personality but still get the Ranen facts."""
    gem = (gem_prompt or "").strip()
    if not gem:
        return DEFAULT_SYSTEM_PROMPT
    if "FACTS ABOUT RANEN" in gem:
        return gem
    return gem + "\n\n" + ABOUT_FACTS

async def _generate_with_fallback(system_prompt, recent_history, effective_message, is_image, file_bytes, mime_type, complex_mode=False):
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
                    effective_message, is_image, file_bytes, mime_type, complex_mode
                )
                if resp and resp.strip():
                    print(f"[SERVED BY] {p}/{model} (complex={complex_mode})")
                    return resp
                errors.append(f"{p}/{model}: empty response")
            except Exception as e:
                msg = str(e)
                print(f"[{p.upper()} ERROR on {model}]: {msg[:400]}")
                errors.append(f"{p}/{model}: {msg[:200]}")
                if "is missing" in msg.lower():
                    break  # no key for this provider — skip its remaining models

    raise RuntimeError(" || ".join(errors))

async def _stream_groq(model, messages_payload, complex_mode=False):
    """Yields text pieces from Groq as they are produced."""
    kw = _groq_kwargs(model, complex_mode)
    loop = asyncio.get_running_loop()
    q = asyncio.Queue()
    stop = threading.Event()

    def worker():
        try:
            stream = groq_client.chat.completions.create(model=model, messages=messages_payload, stream=True, **kw)
            for chunk in stream:
                if stop.is_set():
                    break
                if not chunk.choices:
                    continue
                piece = getattr(chunk.choices[0].delta, "content", None)
                if piece:
                    loop.call_soon_threadsafe(q.put_nowait, ("d", piece))
            loop.call_soon_threadsafe(q.put_nowait, ("end", None))
        except Exception as e:
            loop.call_soon_threadsafe(q.put_nowait, ("err", e))

    threading.Thread(target=worker, daemon=True).start()
    try:
        while True:
            kind, value = await q.get()
            if kind == "d":
                yield value
            elif kind == "end":
                return
            else:
                raise value
    finally:
        stop.set()

async def _stream_with_fallback(system_prompt, recent_history, effective_message, is_image, file_bytes, mime_type, complex_mode=False):
    """Streams from Groq (trying each model). Images / failures fall back to the normal non-streaming chain."""
    if not is_image and groq_client:
        payload = [{"role": "system", "content": system_prompt}]
        for msg in recent_history[:-1]:
            payload.append({"role": msg["role"], "content": msg["content"]})
        payload.append({"role": "user", "content": effective_message})
        for model in GROQ_MODELS:
            got = False
            try:
                async for piece in _stream_groq(model, payload, complex_mode):
                    got = True
                    yield piece
                if got:
                    print(f"[SERVED BY] groq/{model} (stream, complex={complex_mode})")
                    return
            except Exception as e:
                print(f"[GROQ STREAM ERROR on {model}]: {str(e)[:300]}")
                if got:
                    return  # keep the partial answer we already sent
    text = await _generate_with_fallback(system_prompt, recent_history, effective_message, is_image, file_bytes, mime_type, complex_mode)
    yield text

BUSY_MESSAGE = "Ranen is a bit overloaded right now. Please try again in a minute."

@app.post("/api/chat")
async def chat_with_assistant(
    request: Request,
    session_id: str = Form(...),
    message: str = Form(""),
    files: List[UploadFile] = File(default=[]),
    model_choice: str = Form("groq:openai/gpt-oss-120b"),
    gem_prompt: Optional[str] = Form(None),
    stream: str = Form("")
):
    col, val = get_identifier(request)

    if rate_limited(f"chat:{client_ip(request)}", 30, 60):
        return {"response": "You're sending messages too fast. Please wait a minute and try again."}
    if len(message) > MAX_MESSAGE_CHARS:
        return {"response": f"That message is too long (limit {MAX_MESSAGE_CHARS} characters). Please shorten it or attach it as a file."}
    if len([f for f in files if f and f.filename]) > MAX_FILES_PER_MESSAGE:
        return {"response": f"You can attach up to {MAX_FILES_PER_MESSAGE} files per message."}

    if not under_daily_limit(val):
        return {"response": f"You've reached today's limit of {DAILY_MESSAGE_LIMIT} messages — it resets at midnight. Thanks for using Ranen!"}

    # [NEW] user settings: custom instructions + memory ("remember that ..." saves automatically)
    user_settings = get_user_settings(val)
    if user_settings.get("memory_enabled", True):
        _mem = re.match(r"^\s*(?:please\s+)?remember(?:\s+that)?[\s:,\-]+(.{3,300})$", message or "", re.I | re.S)
        if _mem and add_memory(val, _mem.group(1)):
            user_settings = get_user_settings(val)
    user_context = build_user_context(user_settings)

    # Neon -> MongoDB Atlas -> local file (a failed read no longer wipes the chat)
    existing_messages, chat_title = get_chat_record(val, session_id)

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

        # [SECURITY] size + type checks before anything touches the disk
        if len(f_bytes) > MAX_UPLOAD_BYTES:
            return {"response": f"'{f.filename}' is too large (limit {MAX_UPLOAD_BYTES // (1024 * 1024)} MB)."}
        _ext = os.path.splitext(f.filename)[1].lower()
        _image_ok = f_is_image and f_mime in ("image/png", "image/jpeg", "image/gif", "image/webp", "image/heic")
        if _ext not in ALLOWED_UPLOAD_EXT and not _image_ok:
            return {"response": f"'{f.filename}' isn't a supported file type. Allowed: images, PDF, txt, md, py, js, html, css, json, csv."}
        if _ext == ".svg" or f_mime == "image/svg+xml":
            return {"response": "SVG files aren't accepted for security reasons. Please upload a PNG or JPG instead."}

        # Strip any path from the uploaded filename — "../../x" could previously
        # write outside the uploads folder (path traversal).
        safe_name = os.path.basename(f.filename.replace("\\", "/")).replace(" ", "_") or "file"
        stored_filename = f"{uuid.uuid4().hex}_{safe_name}"
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

    # --- [NEW] Image GENERATION (checked before the old "find me a picture" web search) ---
    if not attached_names and is_image_generation_request(message):
        if rate_limited(f"img:{client_ip(request)}", 6, 60):
            return {"response": "Easy there - wait a minute before making more images."}
        if get_today_usage(f"img:{val}") >= DAILY_IMAGE_LIMIT:
            ai_response = f"You've used today's {DAILY_IMAGE_LIMIT} image generations. They reset at midnight."
        else:
            prompt_text = re.sub(r"^\s*/(imagine|image)\s*", "", message, flags=re.I).strip()[:500]
            result = await generate_image(prompt_text)
            if result:
                img_bytes, img_mime = result
                ext = {"image/png": ".png", "image/jpeg": ".jpg", "image/webp": ".webp"}.get(img_mime, ".png")
                img_name = f"gen_{uuid.uuid4().hex}{ext}"
                with open(os.path.join(UPLOAD_DIR, img_name), "wb") as fh:
                    fh.write(img_bytes)
                _record_asset(val, img_name, f"/uploads/{img_name}", img_mime)
                increment_today_usage(f"img:{val}")
                increment_today_usage(val)
                alt = re.sub(r"[\[\]()]", "", prompt_text)[:80] or "Generated image"
                ai_response = f"Here's your image:\n\n![{alt}](/uploads/{img_name})"
            else:
                ai_response = "I couldn't create that image right now. Try again in a minute or describe it differently."
        existing_messages.append({"role": "assistant", "content": ai_response})
        save_chat_history(user_email=val, chat_id=str(session_id), title=chat_title, messages=existing_messages)
        return {"response": ai_response, "message_index": len(existing_messages) - 1}

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
                title = (results[0].get('title') or 'Image').replace('[', '').replace(']', '')
                # Return markdown instead of raw HTML. The frontend escapes raw
                # HTML in messages (XSS fix), and its markdown renderer already styles images.
                ai_response = f'Here is the image I found for **"{clean_prompt}"**:\n\n![{title}]({image_url})'
            else:
                ai_response = f"Man, I scoured the web for **\"{clean_prompt}\"** but couldn't grab a good image right now. Try asking me again later!"
        else:
            ai_response = "I'd love to show you a picture, but my image search engine isn't wired up right now. We need the `ddgs` package!"

        existing_messages.append({"role": "assistant", "content": ai_response})
        save_chat_history(user_email=val, chat_id=str(session_id), title=chat_title, messages=existing_messages)
        return {"response": ai_response, "message_index": len(existing_messages) - 1}

    # date + (live web results when the message needs them) + uploaded file text
    effective_message = await build_effective_message(message, file_text_content)

    system_prompt = build_system_prompt(gem_prompt) + user_context
    complex_mode = is_complex_request(message)

    # [NEW] Streaming: the answer is sent piece by piece (newline-delimited JSON)
    if stream:
        async def _event_stream():
            full = ""
            completed = False
            try:
                try:
                    async for piece in _stream_with_fallback(
                        system_prompt, recent_history, effective_message, is_image, file_bytes, mime_type, complex_mode
                    ):
                        full += piece
                        yield json.dumps({"type": "delta", "text": piece}) + "\n"
                    ok = bool(full.strip())
                except Exception as e:
                    print(f"ALL PROVIDERS FAILED (stream): {e}")
                    ok = bool(full.strip())
                if not ok:
                    full = BUSY_MESSAGE
                    yield json.dumps({"type": "delta", "text": BUSY_MESSAGE}) + "\n"
                else:
                    increment_today_usage(val)
                title_now = chat_title
                if is_first_message and ok:
                    smart = await _generate_smart_title(message, full)
                    if smart:
                        title_now = smart
                existing_messages.append({"role": "assistant", "content": full})
                save_chat_history(user_email=val, chat_id=str(session_id), title=title_now, messages=existing_messages)
                completed = True
                yield json.dumps({"type": "done", "response": full, "message_index": len(existing_messages) - 1, "title": title_now}) + "\n"
            finally:
                if not completed and full.strip():
                    try:
                        existing_messages.append({"role": "assistant", "content": full})
                        save_chat_history(user_email=val, chat_id=str(session_id), title=chat_title, messages=existing_messages)
                    except Exception as e:
                        print(f"STREAM SAVE ERROR: {e}")
        return StreamingResponse(_event_stream(), media_type="application/x-ndjson",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    succeeded = True
    try:
        ai_response = await _generate_with_fallback(
            system_prompt, recent_history, effective_message, is_image, file_bytes, mime_type, complex_mode
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
    return {"response": ai_response, "message_index": len(existing_messages) - 1}

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

    # Neon -> MongoDB Atlas -> local file
    existing_messages, chat_title = get_chat_record(val, session_id)

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
    system_prompt = build_system_prompt(gem_prompt) + build_user_context(get_user_settings(val))

    # regenerate also gets the date, live search and the complex-problem boost when needed
    effective_message = await build_effective_message(last_user_message)
    complex_mode = is_complex_request(last_user_message)

    succeeded = True
    try:
        ai_response = await _generate_with_fallback(
            system_prompt, recent_history, effective_message, False, None, "", complex_mode
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

    live_config = {"response_modalities": ["AUDIO"],
                   "system_instruction": DEFAULT_SYSTEM_PROMPT + build_user_context(get_user_settings(ws_identifier))}

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

# --- PWA (installable app) ---
@app.get("/manifest.webmanifest")
async def pwa_manifest():
    return JSONResponse({
        "name": "Ranen",
        "short_name": "Ranen",
        "description": "Ranen AI assistant by Nwodili Yaemerie Convenant, managed by Tinu Tom",
        "start_url": "/",
        "scope": "/",
        "display": "standalone",
        "background_color": "#080a0f",
        "theme_color": "#080a0f",
        "icons": [
            {"src": "/icon-192.png", "sizes": "192x192", "type": "image/png", "purpose": "any"},
            {"src": "/icon-512.png", "sizes": "512x512", "type": "image/png", "purpose": "any"},
        ],
    }, media_type="application/manifest+json")

def _serve_icon(name: str):
    # Uses icon-192.png / icon-512.png if you added them, otherwise falls back to favicon.png
    for path in [name, os.path.join("..", "app", name), "favicon.png", os.path.join("..", "app", "favicon.png")]:
        if os.path.exists(path):
            return Response(content=open(path, "rb").read(), media_type="image/png",
                            headers={"Cache-Control": "public, max-age=86400"})
    return Response(status_code=404)

@app.get("/icon-192.png")
async def icon_192():
    return _serve_icon("icon-192.png")

@app.get("/icon-512.png")
async def icon_512():
    return _serve_icon("icon-512.png")

SERVICE_WORKER_JS = """
const CACHE = 'ranen-shell-v1';
self.addEventListener('install', e => { self.skipWaiting(); });
self.addEventListener('activate', e => {
  e.waitUntil(caches.keys().then(keys => Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))));
  self.clients.claim();
});
self.addEventListener('fetch', e => {
  const req = e.request;
  if (req.method !== 'GET' || req.mode !== 'navigate') return;   // never touch API calls
  e.respondWith(
    fetch(req).then(res => {
      if (new URL(req.url).pathname === '/') {
        const copy = res.clone();
        caches.open(CACHE).then(c => c.put('/', copy));
      }
      return res;
    }).catch(() => caches.match('/'))
  );
});
"""

@app.get("/sw.js")
async def service_worker():
    # Must be served from the site root so it can control the whole app
    return Response(content=SERVICE_WORKER_JS, media_type="application/javascript",
                    headers={"Cache-Control": "no-cache"})

# [ADMIN] Mount the admin routes (/admin and /api/admin/*)
app.include_router(create_admin_router(
    get_db_connection, load_local_chats, save_local_chats,
    _local_usage_counts, lambda: DAILY_MESSAGE_LIMIT
))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), reload=False)
