from fastapi import FastAPI, Form, File, UploadFile, Request, Response, Cookie
from fastapi.responses import HTMLResponse, RedirectResponse
import sqlite3
import uuid
import os
import base64
from io import BytesIO
from pypdf import PdfReader
from groq import Groq
from authlib.integrations.starlette_client import OAuth
from starlette.middleware.sessions import SessionMiddleware
from google import genai
from google.genai import types
from openai import AsyncOpenAI
from duckduckgo_search import DDGS

app = FastAPI()

# --- CONFIGURATION ---
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
SILICONFLOW_API_KEY = os.getenv("SILICONFLOW_API_KEY")

# --- ADMIN CONFIGURATION ---
ADMIN_EMAIL = "whitefrostff@gmail.com"
ADMIN_SECRET_KEY = os.getenv("ADMIN_SECRET_KEY", "fsociety_admin_2026")

app.add_middleware(SessionMiddleware, secret_key="fsociety_super_secret_session_string")

groq_client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None
genai_client = genai.Client(api_key=GOOGLE_API_KEY) if GOOGLE_API_KEY else None

silicon_client = AsyncOpenAI(
    api_key=SILICONFLOW_API_KEY,
    base_url="https://api.siliconflow.cn/v1"
) if SILICONFLOW_API_KEY else None

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
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_email TEXT,
            guest_id TEXT,
            title TEXT,
            is_pinned INTEGER DEFAULT 0
        )
    ''')
    try:
        cursor.execute("ALTER TABLE sessions ADD COLUMN is_pinned INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER,
            role TEXT,
            content TEXT,
            FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
        )
    ''')
    conn.commit()
    conn.close()

@app.on_event("startup")
def startup_event():
    init_db()

# --- AUTHENTICATION ---
@app.get('/auth/login')
async def login_via_google(request: Request):
    redirect_uri = "https://fsociety-ai-production.up.railway.app/auth/callback"
    return await oauth.google.authorize_redirect(request, redirect_uri)

@app.get('/auth/callback')
async def auth_callback(request: Request):
    try:
        token = await oauth.google.authorize_access_token(request)
        user_info = token.get('userinfo')
        if user_info:
            request.session['user'] = {
                'name': user_info.get('name'),
                'email': user_info.get('email'),
                'picture': user_info.get('picture')
            }
        return RedirectResponse(url='/')
    except Exception as e:
        return {"error": f"Authentication failed: {str(e)}"}

@app.get('/api/current-user')
async def get_current_user(request: Request):
    user = request.session.get('user')
    if user:
        return {"logged_in": True, "user": user}
    return {"logged_in": False}

@app.get('/auth/logout')
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url='/')

# --- FRONTEND ROUTE ---
@app.get("/", response_class=HTMLResponse)
async def serve_frontend(request: Request, response: Response):
    if not request.session.get('user') and not request.cookies.get("guest_id"):
        guest_id = str(uuid.uuid4())
        response.set_cookie(key="guest_id", value=guest_id, max_age=31536000, httponly=True)
    
    html_path = os.path.join("..", "app", "index.html")
    if not os.path.exists(html_path):
        html_path = "index.html" 
    
    if os.path.exists(html_path):
        with open(html_path, "r", encoding="utf-8") as f:
            return f.read()
    return "<h3>index.html not found. Check your file paths.</h3>"

def get_identifier(request: Request):
    user = request.session.get('user')
    if user:
        return ("user_email", user['email'])
    return ("guest_id", request.cookies.get("guest_id", "unknown_guest"))

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

@app.post("/api/rename-session")
async def rename_session(session_id: int = Form(...), title: str = Form(...)):
    conn = sqlite3.connect("fsociety_history.db")
    cursor = conn.cursor()
    cursor.execute("UPDATE sessions SET title = ? WHERE id = ?", (title, session_id))
    conn.commit()
    conn.close()
    return {"status": "success"}

@app.delete("/api/delete-session/{session_id}")
async def delete_session(session_id: int):
    conn = sqlite3.connect("fsociety_history.db")
    cursor = conn.cursor()
    cursor.execute("DELETE FROM messages WHERE session_id = ?", (session_id,))
    cursor.execute("DELETE FROM sessions WHERE id = ?", (session_id,))
    conn.commit()
    conn.close()
    return {"status": "success"}

@app.get("/api/admin/stats")
async def get_app_stats(request: Request, key: str = None):
    user = request.session.get('user')
    user_email = user.get('email') if user else None

    if user_email != ADMIN_EMAIL and key != ADMIN_SECRET_KEY:
        return {"error": "Access Denied"}

    conn = sqlite3.connect("fsociety_history.db")
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(DISTINCT user_email) FROM sessions WHERE user_email IS NOT NULL")
    google_users = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(DISTINCT guest_id) FROM sessions WHERE guest_id IS NOT NULL")
    guest_users = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM sessions")
    total_sessions = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM messages")
    total_messages = cursor.fetchone()[0]
    conn.close()
    
    return {
        "status": "Authorized Admin Access",
        "unique_google_users": google_users,
        "unique_guest_users": guest_users,
        "total_chat_sessions": total_sessions,
        "total_messages_sent": total_messages
    }

# --- WEB IMAGE SEARCH ---
def fetch_web_image(query: str):
    try:
        with DDGS() as ddgs:
            results = list(ddgs.images(
                query,
                region='wt-wt',
                safesearch='moderate',
                max_results=1
            ))
            
        if results and len(results) > 0:
            image_url = results[0]['image']
            title = results[0]['title']
            return f'<img src="{image_url}" class="rounded-xl shadow-lg mt-2 max-w-full h-auto" alt="{title}">'
        return "I couldn't find any matching images on the web."
    except Exception as e:
        return f"[ERROR] Image search failed: {str(e)}"

# --- CHAT ENDPOINT ---
@app.post("/api/chat")
async def chat_with_assistant(session_id: int = Form(...), message: str = Form(""), file: UploadFile = File(None)):
    conn = sqlite3.connect("fsociety_history.db")
    cursor = conn.cursor()

    file_bytes = None
    file_extension = ""
    display_message = message

    if file and file.filename:
        file_bytes = await file.read()
        file_extension = file.filename.split('.')[-1].lower()
        display_message += f" [Attached: {file.filename}]"

    cursor.execute("INSERT INTO messages (session_id, role, content) VALUES (?, ?, ?)", (session_id, "user", display_message))
    
    cursor.execute("SELECT COUNT(*) FROM messages WHERE session_id = ?", (session_id,))
    msg_count = cursor.fetchone()[0]
    if msg_count == 1:
        short_title = (message[:25] + '...') if len(message) > 25 else (message or "File Chat")
        cursor.execute("UPDATE sessions SET title = ? WHERE id = ?", (short_title, session_id))

    conn.commit()

    lower_msg = message.lower().strip()
    image_search_triggers = ["find a picture", "find an image", "get a picture", "show me a picture", "search image", "look up a picture"]
    
    if any(lower_msg.startswith(trigger) or f"please {trigger}" in lower_msg for trigger in image_search_triggers):
        query_text = message
        for trig in image_search_triggers:
            query_text = query_text.lower().replace(trig, "").strip()
        
        ai_response = fetch_web_image(query_text or message)
        cursor.execute("INSERT INTO messages (session_id, role, content) VALUES (?, ?, ?)", (session_id, "assistant", ai_response))
        conn.commit()
        conn.close()
        return {"response": ai_response}

    cursor.execute("SELECT role, content FROM messages WHERE session_id = ? ORDER BY id ASC", (session_id,))
    past_messages = cursor.fetchall()
    recent_history = past_messages[-10:]

    system_prompt = (
        "You are Fsociety AI, a smart, insightful, and highly human-like assistant created by Frost. "
        "Your responses are clear, natural, and conversational. "
        "When generating code snippets or structural web components (HTML/CSS/JS), wrap the code cleanly in markdown code blocks. "
        "Avoid mechanical phrasing.\n"
        "CRITICAL RULE: If directly asked who created or built you, answer: 'Frost made me.'"
    )

    try:
        # Vision via Groq
        if file_bytes and file_extension in ["jpg", "jpeg", "png", "webp", "gif"]:
            base64_image = base64.b64encode(file_bytes).decode('utf-8')
            data_url = f"data:image/{file_extension};base64,{base64_image}"
            
            messages_payload = [{"role": "system", "content": system_prompt}]
            messages_payload.append({
                "role": "user",
                "content": [
                    {"type": "text", "text": message if message else "Describe or analyze this image for me."},
                    {"type": "image_url", "image_url": {"url": data_url}}
                ]
            })
            
            chat_completion = groq_client.chat.completions.create(
                model="llama-3.2-11b-vision-preview",
                messages=messages_payload,
                temperature=0.7,
                max_tokens=1500
            )
            ai_response = chat_completion.choices[0].message.content

        # PDF via SiliconFlow / Groq
        elif file_bytes and file_extension == "pdf":
            file_text_content = ""
            try:
                reader = PdfReader(BytesIO(file_bytes))
                for page in reader.pages:
                    extracted = page.extract_text()
                    if extracted:
                        file_text_content += extracted + "\n"
            except Exception as e:
                file_text_content = f"[Error reading PDF: {str(e)}]"

            full_prompt_content = f"{message}\n\n[PDF Text Content]:\n{file_text_content[:10000]}"
            messages_payload = [{"role": "system", "content": system_prompt}, {"role": "user", "content": full_prompt_content}]

            if silicon_client:
                response = await silicon_client.chat.completions.create(
                    model="deepseek-ai/DeepSeek-V3",
                    messages=messages_payload,
                    max_tokens=2048
                )
                ai_response = response.choices[0].message.content
            else:
                chat_completion = groq_client.chat.completions.create(
                    model="llama-3.3-70b-versatile",
                    messages=messages_payload,
                    temperature=0.7,
                    max_tokens=2048
                )
                ai_response = chat_completion.choices[0].message.content

        # Text via SiliconFlow / Groq
        else:
            messages_payload = [{"role": "system", "content": system_prompt}]
            for r, c in recent_history:
                messages_payload.append({"role": r, "content": c})

            if silicon_client:
                response = await silicon_client.chat.completions.create(
                    model="deepseek-ai/DeepSeek-V3",
                    messages=messages_payload,
                    max_tokens=2048
                )
                ai_response = response.choices[0].message.content
            else:
                chat_completion = groq_client.chat.completions.create(
                    model="llama-3.3-70b-versatile",
                    messages=messages_payload,
                    temperature=0.75,
                    max_tokens=2048
                )
                ai_response = chat_completion.choices[0].message.content

    except Exception as e:
        ai_response = f"[ERROR] Core processing failed: {str(e)}"

    cursor.execute("INSERT INTO messages (session_id, role, content) VALUES (?, ?, ?)", (session_id, "assistant", ai_response))
    conn.commit()
    conn.close()

    return {"response": ai_response}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), reload=False)
