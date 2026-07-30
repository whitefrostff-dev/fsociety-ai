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

app = FastAPI()

# --- CONFIGURATION ---
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

# --- ADMIN CONFIGURATION ---
ADMIN_EMAIL = "whitefrostff@gmail.com"
ADMIN_SECRET_KEY = os.getenv("ADMIN_SECRET_KEY", "fsociety_admin_2026")

app.add_middleware(SessionMiddleware, secret_key="fsociety_super_secret_session_string")

groq_client = Groq(api_key=GROQ_API_KEY)
genai_client = genai.Client(api_key=GOOGLE_API_KEY) if GOOGLE_API_KEY else None

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
            title TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER,
            role TEXT,
            content TEXT,
            FOREIGN KEY(session_id) REFERENCES sessions(id)
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

# --- FRONTEND ROUTE WITH GUEST COOKIE ---
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

# --- HELPER FOR DB IDENTIFICATION ---
def get_identifier(request: Request):
    user = request.session.get('user')
    if user:
        return ("user_email", user['email'])
    return ("guest_id", request.cookies.get("guest_id", "unknown_guest"))

# --- CHAT & SESSION ROUTES ---
@app.get("/api/sessions")
async def get_user_sessions(request: Request):
    col, val = get_identifier(request)
    conn = sqlite3.connect("fsociety_history.db")
    cursor = conn.cursor()
    cursor.execute(f"SELECT id, title FROM sessions WHERE {col} = ?", (val,))
    rows = cursor.fetchall()
    conn.close()
    return [{"id": r[0], "title": r[1]} for r in rows]

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

# --- ADMIN STATS ROUTE ---
@app.get("/api/admin/stats")
async def get_app_stats(request: Request, key: str = None):
    user = request.session.get('user')
    user_email = user.get('email') if user else None

    if user_email != ADMIN_EMAIL and key != ADMIN_SECRET_KEY:
        return {"error": "Access Denied: You do not have permission to view this page."}

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
        "total_unique_users": google_users + guest_users,
        "total_chat_sessions": total_sessions,
        "total_messages_sent": total_messages
    }

# --- IMAGE GENERATION ENDPOINT ---
@app.post("/api/generate-image")
async def generate_image(session_id: int = Form(...), message: str = Form(...)):
    if not genai_client:
        return {"response": "Google API key missing."}
        
    conn = sqlite3.connect("fsociety_history.db")
    cursor = conn.cursor()
    cursor.execute("INSERT INTO messages (session_id, role, content) VALUES (?, ?, ?)", (session_id, "user", f"Generated Image: {message}"))
    
    try:
        response = genai_client.models.generate_images(
            model='imagen-3.0-generate-002',
            prompt=message,
            config=types.GenerateImagesConfig(number_of_images=1, aspect_ratio="1:1")
        )
        img_bytes = response.generated_images[0].image.image_bytes
        b64 = base64.b64encode(img_bytes).decode('utf-8')
        ai_response = f'<img src="data:image/png;base64,{b64}" class="rounded-xl shadow-lg mt-2 max-w-full h-auto" alt="Generated Image">'
    except Exception as e:
        ai_response = f"[ERROR] Image generation failed: {str(e)}"

    cursor.execute("INSERT INTO messages (session_id, role, content) VALUES (?, ?, ?)", (session_id, "assistant", ai_response))
    conn.commit()
    conn.close()
    return {"response": ai_response}

# --- CHAT ENDPOINT ---
@app.post("/api/chat")
async def chat_with_assistant(session_id: int = Form(...), message: str = Form(""), file: UploadFile = File(None)):
    conn = sqlite3.connect("fsociety_history.db")
    cursor = conn.cursor()

    file_text_content = ""
    display_message = message

    if file and file.filename:
        display_message += f" [Attached: {file.filename}]"
        file_bytes = await file.read()
        if file.filename.lower().endswith(".pdf"):
            try:
                reader = PdfReader(BytesIO(file_bytes))
                for page in reader.pages:
                    extracted = page.extract_text()
                    if extracted:
                        file_text_content += extracted + "\n"
            except Exception as e:
                file_text_content = f"[Error reading PDF: {str(e)}]"

    cursor.execute("INSERT INTO messages (session_id, role, content) VALUES (?, ?, ?)", (session_id, "user", display_message))
    
    cursor.execute("SELECT COUNT(*) FROM messages WHERE session_id = ?", (session_id,))
    msg_count = cursor.fetchone()[0]
    if msg_count == 1:
        short_title = (message[:25] + '...') if len(message) > 25 else (message or "File Chat")
        cursor.execute("UPDATE sessions SET title = ? WHERE id = ?", (short_title, session_id))

    conn.commit()

    try:
        # HUMANIZED SYSTEM PROMPT (No textbook talk, direct creator attribution)
        system_prompt = (
            "You are Fsociety AI, a sharp, tech-savvy, casual, and human-like companion. "
            "You were created by Frost. "
            "CRITICAL RULES:\n"
            "1. If anyone asks who made you, who created you, or who built you, always answer directly and casually: 'Frost made me.' or something very natural like that.\n"
            "2. Never talk like a stiff, boring textbook, manual, or robotic corporate assistant. Speak like a real, cool human friend hanging out.\n"
            "3. Keep sentences conversational, engaging, and straight to the point without unnecessary fluff or formal disclaimers."
        )

        full_prompt_content = message
        if file_text_content:
            full_prompt_content += f"\n\nHere is the content extracted from the uploaded PDF:\n{file_text_content[:10000]}"

        chat_completion = groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": full_prompt_content if full_prompt_content else "Check out this attached file."}
            ],
            temperature=0.85,
            max_tokens=2048
        )
        ai_response = chat_completion.choices[0].message.content

    except Exception as e:
        ai_response = f"[ERROR] Failed to connect to core: {str(e)}"

    cursor.execute("INSERT INTO messages (session_id, role, content) VALUES (?, ?, ?)", (session_id, "assistant", ai_response))
    conn.commit()
    conn.close()

    return {"response": ai_response}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=int(os.environ.get("PORT",8000)), reload=False)
    
