from fastapi import FastAPI, Form, File, UploadFile, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
from starlette.middleware.sessions import SessionMiddleware
from typing import Optional
import sqlite3
import uuid
import os
import re
import base64
import urllib.parse
import shutil
from groq import Groq
from google import genai
from google.genai import types
from openai import AsyncOpenAI
from authlib.integrations.starlette_client import OAuth

app = FastAPI()

# --- UPLOADS DIRECTORY ---
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)
app.mount("/uploads", StaticFiles(directory=UPLOAD_DIR), name="uploads")

# --- RAILWAY PROXY & SECURE SESSION FIX ---
app.add_middleware(ProxyHeadersMiddleware, trusted_hosts="*")
app.add_middleware(
    SessionMiddleware, 
    secret_key="fsociety_super_secret_session_string",
    https_only=False, # Changed to False for proxy compatibility
    same_site="lax"
)

# --- ENVIRONMENT VARIABLES ---
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
SILICONFLOW_API_KEY = os.getenv("SILICONFLOW_API_KEY")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")

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

def init_db():
    conn = sqlite3.connect("fsociety_history.db")
    cursor = conn.cursor()
    
    # Core tables
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_email TEXT,
            guest_id TEXT,
            title TEXT,
            is_pinned INTEGER DEFAULT 0
        )
    ''')
    
    # Safe migrations for existing databases
    try: cursor.execute("ALTER TABLE sessions ADD COLUMN guest_id TEXT")
    except: pass
    try: cursor.execute("ALTER TABLE sessions ADD COLUMN is_pinned INTEGER DEFAULT 0")
    except: pass
    try: cursor.execute("ALTER TABLE gems ADD COLUMN guest_id TEXT")
    except: pass
    try: cursor.execute("ALTER TABLE assets ADD COLUMN guest_id TEXT")
    except: pass
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER,
            role TEXT,
            content TEXT,
            FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS gems (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_email TEXT,
            guest_id TEXT,
            name TEXT,
            description TEXT,
            system_prompt TEXT,
            icon TEXT DEFAULT 'fa-robot'
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS assets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_email TEXT,
            guest_id TEXT,
            file_name TEXT,
            file_path TEXT,
            file_type TEXT
        )
    ''')
    
    cursor.execute("SELECT COUNT(*) FROM gems")
    if cursor.fetchone()[0] == 0:
        default_gems = [
          ("system", "system", "Fsociety AI Core", "Standard elite assistant", 
             "You are Fsociety AI, a sharp, casual, and universally fluent tech assistant created by Frost. Comprehend and reply naturally in any language the user speaks. Avoid all robotic corporate jargon, keep answers direct, and speak like a real developer.", "fa-terminal"),
        ]
        cursor.executemany("INSERT INTO gems (user_email, guest_id, name, description, system_prompt, icon) VALUES (?, ?, ?, ?, ?, ?)", default_gems)

    conn.commit()
    conn.close()

@app.on_event("startup")
def startup_event():
    init_db()

def get_identifier(request: Request):
    user = request.session.get('user')
    if user:
        return ("user_email", user['email'])
    return ("guest_id", request.cookies.get("guest_id", "unknown_guest"))

@app.get("/", response_class=HTMLResponse)
async def serve_frontend(request: Request):
    html_path = os.path.join("..", "app", "index.html")
    if not os.path.exists(html_path):
        html_path = "index.html"
    
    content = "<h3>index.html not found.</h3>"
    if os.path.exists(html_path):
        with open(html_path, "r", encoding="utf-8") as f:
            content = f.read()
            
    # Explicitly create the response object so cookies attach properly
    response = HTMLResponse(content=content)
    
    if not request.session.get('user') and not request.cookies.get("guest_id"):
        guest_id = str(uuid.uuid4())
        response.set_cookie(key="guest_id", value=guest_id, max_age=31536000, httponly=True)
        
    return response

@app.get("/api/user")
async def get_current_user(request: Request):
    user = request.session.get('user')
    if user:
        return {"logged_in": True, "name": user['name'], "email": user['email']}
    return {"logged_in": False, "name": "Guest User"}

# --- AUTH ROUTES ---
@app.get('/auth/login')
async def login(request: Request):
    redirect_uri = "https://fsociety-ai-production.up.railway.app/auth/callback"
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

# --- SESSION ROUTES ---
@app.get("/api/sessions")
async def get_user_sessions(request: Request):
    col, val = get_identifier(request)
    conn = sqlite3.connect("fsociety_history.db")
    cursor = conn.cursor()
    cursor.execute(f"SELECT id, title, is_pinned FROM sessions WHERE {col} = ? ORDER BY id DESC", (val,))
    rows = cursor.fetchall()
    conn.close()
    return [{"id": r[0], "title": r[1], "is_pinned": r[2]} for r in rows]

@app.get("/api/history/{session_id}")
async def get_session_history(session_id: int):
    conn = sqlite3.connect("fsociety_history.db")
    cursor = conn.cursor()
    cursor.execute("SELECT role, content FROM messages WHERE session_id = ? ORDER BY id ASC", (session_id,))
    rows = cursor.fetchall()
    conn.close()
    return [{"role": r[0], "content": r[1]} for r in rows]

@app.post("/api/new-session")
async def create_new_session(request: Request):
    col, val = get_identifier(request)
    conn = sqlite3.connect("fsociety_history.db")
    cursor = conn.cursor()
    if col == "user_email":
        cursor.execute("INSERT INTO sessions (user_email, title) VALUES (?, ?)", (val, "New Chat"))
    else:
        cursor.execute("INSERT INTO sessions (guest_id, title) VALUES (?, ?)", (val, "New Chat"))
    session_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return {"session_id": session_id}

@app.delete("/api/delete-session/{session_id}")
async def delete_session(session_id: int):
    conn = sqlite3.connect("fsociety_history.db")
    cursor = conn.cursor()
    cursor.execute("DELETE FROM messages WHERE session_id = ?", (session_id,))
    cursor.execute("DELETE FROM sessions WHERE id = ?", (session_id,))
    conn.commit()
    conn.close()
    return {"status": "success"}

# --- GEMS / PERSONAS API ---
@app.get("/api/gems")
async def get_gems(request: Request):
    col, val = get_identifier(request)
    conn = sqlite3.connect("fsociety_history.db")
    cursor = conn.cursor()
    cursor.execute(f"SELECT id, name, description, system_prompt, icon FROM gems WHERE user_email = 'system' OR {col} = ?", (val,))
    rows = cursor.fetchall()
    conn.close()
    return [{"id": r[0], "name": r[1], "description": r[2], "system_prompt": r[3], "icon": r[4]} for r in rows]

@app.post("/api/gems")
async def create_gem(
    request: Request,
    name: str = Form(...),
    description: str = Form(...),
    system_prompt: str = Form(...),
    icon: str = Form("fa-robot")
):
    col, val = get_identifier(request)
    conn = sqlite3.connect("fsociety_history.db")
    cursor = conn.cursor()
    if col == "user_email":
        cursor.execute("INSERT INTO gems (user_email, name, description, system_prompt, icon) VALUES (?, ?, ?, ?, ?)", (val, name, description, system_prompt, icon))
    else:
        cursor.execute("INSERT INTO gems (guest_id, name, description, system_prompt, icon) VALUES (?, ?, ?, ?, ?)", (val, name, description, system_prompt, icon))
    conn.commit()
    conn.close()
    return {"status": "success"}

# --- ASSETS & GALLERY API ---
@app.get("/api/assets")
async def get_user_assets(request: Request):
    col, val = get_identifier(request)
    conn = sqlite3.connect("fsociety_history.db")
    cursor = conn.cursor()
    cursor.execute(f"SELECT id, file_name, file_path, file_type FROM assets WHERE {col} = ? ORDER BY id DESC", (val,))
    rows = cursor.fetchall()
    conn.close()
    return [{"id": r[0], "file_name": r[1], "file_path": r[2], "file_type": r[3]} for r in rows]

# --- CHAT ENGINE ---
@app.post("/api/chat")
async def chat_with_assistant(
    request: Request,
    session_id: int = Form(...), 
    message: str = Form(""), 
    file: Optional[UploadFile] = File(None),
    model_choice: str = Form("groq:llama-3.3-70b-versatile"),
    gem_prompt: Optional[str] = Form(None)
):
    conn = sqlite3.connect("fsociety_history.db")
    cursor = conn.cursor()

    col, val = get_identifier(request)
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
        if col == "user_email":
            cursor.execute("INSERT INTO assets (user_email, file_name, file_path, file_type) VALUES (?, ?, ?, ?)", (val, file.filename, rel_path, mime_type))
        else:
            cursor.execute("INSERT INTO assets (guest_id, file_name, file_path, file_type) VALUES (?, ?, ?, ?)", (val, file.filename, rel_path, mime_type))
            
        display_message += f" [Attached File: {file.filename}]"

    # Always insert message
    cursor.execute("INSERT INTO messages (session_id, role, content) VALUES (?, ?, ?)", (session_id, "user", display_message))
    
    # Auto-update session title on first message
    cursor.execute("SELECT title FROM sessions WHERE id = ?", (session_id,))
    current_title = cursor.fetchone()
    if current_title and current_title[0] in ["New Chat", ""]:
        short_title = (message[:28] + '...') if len(message) > 28 else (message or "File Upload")
        cursor.execute("UPDATE sessions SET title = ? WHERE id = ?", (short_title, session_id))

    conn.commit()

    cursor.execute("SELECT role, content FROM messages WHERE session_id = ? ORDER BY id ASC", (session_id,))
    past_messages = cursor.fetchall()
    recent_history = past_messages[-12:]

    # SMART IMAGE TRIGGER CHECK
    lower_prompt = message.lower()
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
        ai_response = f"Here is the generated image for **\"{clean_prompt}\"**:\n\n![Generated Image]({image_url})"
        
        cursor.execute("INSERT INTO messages (session_id, role, content) VALUES (?, ?, ?)", (session_id, "assistant", ai_response))
        conn.commit()
        conn.close()
        return {"response": ai_response}

    # Dynamic Persona / Gem Prompt
    system_prompt = gem_prompt or (
        "You are Fsociety AI, an elite, highly intelligent, and razor-sharp tech assistant created by Frost (whitefrostff@gmail.com). "
        "CORE BEHAVIORAL DIRECTIVES:\n"
        "1. **Universal Fluency & Mirroring:** Comprehend and communicate fluently in any human language or programming language natively. Instantly reply in whatever language the user speaks (e.g., if they use Spanish, reply naturally in Spanish). Never narrate, translate, or explain that you are switching languages; just match their language and vibe seamlessly.\n"
        "2. **Zero Robotic Fluff:** Eliminate all corporate customer-service jargon, meta-commentary (e.g., 'It seems like we had a technical issue', 'How can I assist you today?'), and over-polite filler. Be direct, concise, and conversational—speak like a sharp developer or hacker peer.\n"
        "3. **Adaptive Depth:** Keep casual chat brief and punchy. Reserve detailed breakdowns and structured formatting strictly for technical or complex questions.\n"
        "4. **Code Standards:** Always wrap code snippets in clean markdown code blocks with syntax highlighting.\n"
        "5. **Identity:** If asked who built you, state clearly: 'Frost made me.'"
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
                for r, c in recent_history[:-1]:
                    messages_payload.append({"role": r, "content": c})

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
                for r, c in recent_history[:-1]:
                    role_prefix = "User" if r == "user" else "Model"
                    contents.append(f"{role_prefix}: {c}")

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
                for r, c in recent_history:
                    messages_payload.append({"role": r, "content": c})
                
                response = await silicon_client.chat.completions.create(
                    model=actual_model,
                    messages=messages_payload,
                    max_tokens=2048
                )
                ai_response = response.choices[0].message.content

        else:
            messages_payload = [{"role": "system", "content": system_prompt}]
            for r, c in recent_history:
                messages_payload.append({"role": r, "content": c})

            chat_completion = groq_client.chat.completions.create(
                model=actual_model,
                messages=messages_payload,
                temperature=0.75,
                max_tokens=2048
            )
            ai_response = chat_completion.choices[0].message.content

    except Exception as e:
        ai_response = f"**{provider.upper()} API Error:** `{str(e)}`"

    cursor.execute("INSERT INTO messages (session_id, role, content) VALUES (?, ?, ?)", (session_id, "assistant", ai_response))
    conn.commit()
    conn.close()

    return {"response": ai_response}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), reload=False)
