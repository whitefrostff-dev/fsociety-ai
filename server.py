from fastapi import FastAPI, Form, File, UploadFile, Request,Response,Cookie
from fastapi.responses import HTMLResponse, RedirectResponse
import sqlite3
import uuid
import os
from io import BytesIO
from pypdf import PdfReader
from groq import Groq
from authlib.integrations.starlette_client import OAuth
from starlette.middleware.sessions import SessionMiddleware

app = FastAPI()

# --- CONFIGURATION ---
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

# Google OAuth Credentials
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")

app.add_middleware(SessionMiddleware, secret_key="random_super_secret_session_string_here")

groq_client = Groq(api_key=GROQ_API_KEY)

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

@app.get('/auth/login')
async def login_via_google(request: Request):
    redirect_uri = request.url_for('auth_callback')
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

@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    html_path = os.path.join("..", "app", "index.html")
    if not os.path.exists(html_path):
        html_path = "index.html" 
    
    if os.path.exists(html_path):
        with open(html_path, "r", encoding="utf-8") as f:
            return f.read()
    return "<h3>index.html not found. Check your file paths.</h3>"
@app.get("/api/sessions")
async def get_user_sessions(request: Request):
    user = request.session.get('user')
    email = user['email'] if user else 'guest@local'
    conn = sqlite3.connect("fsociety_history.db")
    cursor = conn.cursor()
    cursor.execute("SELECT id, title FROM sessions WHERE user_email = ? ", (email,))
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
    user = request.session.get('user')
    email = user['email'] if user else 'guest@local'
    
    conn = sqlite3.connect("fsociety_history.db")
    cursor = conn.cursor()
    cursor.execute("INSERT INTO sessions (user_email, title) VALUES (?, ?)", (email, "New Chat"))
    session_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return {"session_id": session_id}

@app.post("/api/chat")
async def chat_with_assistant(session_id: int = Form(...), message: str = Form(""), file: UploadFile = File(None)):
    conn = sqlite3.connect("fsociety_history.db")
    cursor = conn.cursor()

    file_text_content = ""
    display_message = message

    if file and file.filename:
        display_message += f" [Attached: {file.filename}]"
        file_bytes = await file.read()
        
        # Handle PDF parsing
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
        # STRICT HUMAN-LIKE VIBE PROMPT (No corporate robotics, natural speech)
        system_prompt = (
            "You are Fsociety AI, a sharp, street-smart, and completely natural AI companion. "
            "CRITICAL: Never use stiff corporate boilerplate like 'As an AI, I do not have feelings...' or lecture users on linguistics or grammar. "
            "Talk like a chill, friendly human companion. Match slang or casual talk (like Pidgin) naturally without pointing it out. "
            "The current year is 2026."
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
            temperature=0.8,
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
