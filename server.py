from fastapi import FastAPI, Form, File, UploadFile, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
from starlette.middleware.sessions import SessionMiddleware
from typing import Optional
import uuid
import os
import re
import base64
import urllib.parse
import shutil
import httpx
import json
from pydantic import BaseModel
from groq import Groq
from google import genai
from google.genai import types
from openai import AsyncOpenAI
from authlib.integrations.starlette_client import OAuth
import psycopg2
import psycopg2.extras

app = FastAPI()

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
    
    # Disk fallback
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

@app.get("/terms", response_class=HTMLResponse)
async def terms_page():
    return """
    <html><head><title>Terms of Service - Ranen</title><style>body{background:#000;color:#fff;font-family:sans-serif;padding:40px;line-height:1.6;} a{color:#4da6ff;}</style></head>
    <body><h2>Terms of Service for Ranen</h2>
    <p>1. <b>Acceptable Use:</b> Do not use the service for malicious activities, prompt injection, or illegal content generation.<br>
    2. <b>AI Output Disclaimer:</b> Outputs may contain errors or hallucinations. Do not rely on Ranen for critical medical, legal, or financial advice.<br>
    3. <b>Content Ownership:</b> You retain rights to your inputs; we retain rights to the platform UI/UX.<br>
    4. <b>Liability:</b> Service is provided "as is". We are not responsible for downtime or damages.</p></body></html>
    """

@app.get("/privacy", response_class=HTMLResponse)
async def privacy_page():
    return """
    <html><head><title>Privacy Policy - Ranen</title><style>body{background:#000;color:#fff;font-family:sans-serif;padding:40px;line-height:1.6;} a{color:#4da6ff;}</style></head>
    <body><h2>Privacy Policy for Ranen</h2>
    <p>1. <b>Data Collection:</b> We collect chat prompts, uploaded images, and basic connection logs to provide the service.<br>
    2. <b>Third-Party Providers:</b> Your prompts and images are processed securely via external APIs (e.g., Groq, Google Gemini) to generate responses.<br>
    3. <b>Data Sales:</b> We do not sell your personal data or chat histories to third-party advertisers.<br>
    4. <b>Retention:</b> Chat history is stored to provide session continuity and can be deleted upon request.</p></body></html>
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
    
    # Disk fallback
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
    
    # Disk fallback
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
        {"id": 1, "name": "Ranen Core", "description": "Standard elite assistant created by Nwodili Yaemerie Covenant", "system_prompt": "You are Ranen, a sharp, casual, and universally fluent tech assistant created and owned by Nwodili Yaemerie Covenant. Comprehend and reply naturally in any language the user speaks. Avoid all robotic corporate jargon, keep answers direct, and speak like a real developer.", "icon": "fa-terminal"}
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
                return [{"id": r["id"], "name": r["name"], "description": r["description"], "system_prompt": r["system_prompt"], "icon": r.get("icon", "fa-robot")} for r in rows]
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
    display_message = message

    if file and file.filename:
        file_bytes = await file.read()
        mime_type = file.content_type or "image/png"
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

        display_message += f" [Attached File: {file.filename}]"

    existing_messages.append({"role": "user", "content": display_message})
    
    if chat_title in ["New Chat", ""] and message:
        chat_title = (message[:28] + '...') if len(message) > 28 else message

    recent_history = existing_messages[-12:]
    lower_prompt = message.lower()
    
    # --- Image Generation Interceptor ---
    image_keywords = ["generate image", "create image", "draw an image", "make an image", "generate picture", "draw a", "generate pic", "generate pics", "draw pics"]
    
    if any(keyword in lower_prompt for keyword in image_keywords):
        clean_prompt = message
        for kw in image_keywords:
            clean_prompt = clean_prompt.replace(kw, "")
        for noise in ["for me", "of a", "of", "a", "picture", "pics", "pic"]:
            clean_prompt = re.sub(r'\b' + noise + r'\b', '', clean_prompt, flags=re.IGNORECASE)
        
        clean_prompt = clean_prompt.strip() or "cyberpunk hacktivist matrix art"
        encoded_prompt = urllib.parse.quote(clean_prompt)
        image_url = f"https://image.pollinations.ai/prompt/{encoded_prompt}?nologo=true"
        
        # Wrapped in a security block to trigger the artifact UI on the frontend
        ai_response = f"Here is the generated image for **\"{clean_prompt}\"**:\n\n```security\n![Generated Image]({image_url})\n```"
        
        existing_messages.append({"role": "assistant", "content": ai_response})
        save_chat_history(user_email=val, chat_id=str(session_id), title=chat_title, messages=existing_messages)
        return {"response": ai_response}

    # --- Standard AI Chat Processing ---
    system_prompt = gem_prompt or (
        "You are Ranen, an elite, highly intelligent, and razor-sharp tech assistant created and owned by Nwodili Yaemerie Covenant. "
        "CORE BEHAVIORAL DIRECTIVES:\n"
        "1. **Universal Fluency & Mirroring:** Comprehend and communicate fluently in any human language or programming language natively. Instantly reply in whatever language the user speaks. Never narrate, translate, or explain that you are switching languages; just match their language and vibe seamlessly.\n"
        "2. **Zero Robotic Fluff:** Eliminate all corporate customer-service jargon, meta-commentary, and over-polite filler. Be direct, concise, and conversational—speak like a sharp developer or hacker peer.\n"
        "3. **Adaptive Depth:** Keep casual chat brief and punchy. Reserve detailed breakdowns and structured formatting strictly for technical or complex questions.\n"
        "4. **Code Standards:** Always wrap code snippets in clean markdown code blocks with syntax highlighting.\n"
        "5. **Architecture & Design:** If asked to sketch an architecture diagram or build a house plan, output valid Mermaid.js code wrapped in ```mermaid blocks. Make sure the code is structurally sound so it renders properly.\n"
        "6. **Cybersecurity:** If asked to display vulnerability scans, terminal output, or security reports, wrap the output inside ```security blocks for proper artifact rendering.\n"
        "7. **Identity:** If asked who built you, state clearly: 'Nwodili Yaemerie Covenant made me.'"
    )

    provider, actual_model = model_choice.split(":", 1) if ":" in model_choice else ("groq", model_choice)

    if provider == "google":
        actual_model = "gemini-3.5-flash"
    elif provider == "openrouter" and actual_model.endswith(":free"):
        actual_model = actual_model.replace(":free", "")

    try:
        if provider == "openrouter":
            if not openrouter_client:
                ai_response = "**Error:** `OPENROUTER_API_KEY` is missing."
            else:
                messages_payload = [{"role": "system", "content": system_prompt}]
                for msg in recent_history[:-1]:
                    messages_payload.append({"role": msg["role"], "content": msg["content"]})

                if is_image and file_bytes:
                    b64_img = base64.b64encode(file_bytes).decode('utf-8')
                    messages_payload.append({
                        "role": "user",
                        "content": [
                            {"type": "text", "text": message or "Analyze this file."},
                            {"type": "image_url", "image_url": {"url": f"data:{mime_type};base64,{b64_img}"}}
                        ]
                    })
                else:
                    messages_payload.append({"role": "user", "content": message})

                res = await openrouter_client.chat.completions.create(
                    model=actual_model,
                    messages=messages_payload,
                    max_tokens=2048
                )
                ai_response = res.choices[0].message.content

        elif provider == "google":
            if not genai_client:
                ai_response = "**Error:** `GOOGLE_API_KEY` is missing."
            else:
                contents = []
                for msg in recent_history[:-1]:
                    role_prefix = "User" if msg["role"] == "user" else "Model"
                    contents.append(f"{role_prefix}: {msg['content']}")

                if is_image and file_bytes:
                    image_part = types.Part.from_bytes(data=file_bytes, mime_type=mime_type)
                    contents.append(image_part)
                
                contents.append(message)

                config = types.GenerateContentConfig(
                    system_instruction=system_prompt,
                    temperature=0.7,
                )

                resp = genai_client.models.generate_content(
                    model=actual_model,
                    contents=contents,
                    config=config
                )
                ai_response = resp.text

        elif provider == "silicon":
            if not silicon_client:
                ai_response = "**Error:** `SILICONFLOW_API_KEY` is missing."
            else:
                messages_payload = [{"role": "system", "content": system_prompt}]
                for msg in recent_history:
                    messages_payload.append({"role": msg["role"], "content": msg["content"]})
                
                response = await silicon_client.chat.completions.create(
                    model=actual_model,
                    messages=messages_payload,
                    max_tokens=2048
                )
                ai_response = response.choices[0].message.content

        else: 
            if not groq_client:
                ai_response = "**Error:** `GROQ_API_KEY` is missing from environment variables."
            else:
                if actual_model in ["llama-3.3-70b-versatile", "llama-3.1-70b-versatile", "llama3-70b-8192"]:
                    actual_model = "openai/gpt-oss-120b"
                elif "8b" in actual_model:
                    actual_model = "openai/gpt-oss-20b"

                messages_payload = [{"role": "system", "content": system_prompt}]
                for msg in recent_history:
                    messages_payload.append({"role": msg["role"], "content": msg["content"]})

                chat_completion = groq_client.chat.completions.create(
                    model=actual_model,
                    messages=messages_payload,
                    temperature=0.75,
                    max_tokens=2048
                )
                ai_response = chat_completion.choices[0].message.content

    except Exception as e:
        ai_response = f"**{provider.upper()} API Error:** `{str(e)}`"

    existing_messages.append({"role": "assistant", "content": ai_response})
    save_chat_history(user_email=val, chat_id=str(session_id), title=chat_title, messages=existing_messages)

    return {"response": ai_response}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), reload=False)
