from fastapi import FastAPI, Form, File, UploadFile, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
from starlette.middleware.sessions import SessionMiddleware
from typing import Optional
import sqlite3
import uuid
import os
import base64
import urllib.parse
from groq import Groq
from google import genai
from google.genai import types
from openai import AsyncOpenAI
from authlib.integrations.starlette_client import OAuth

app = FastAPI()

# --- RAILWAY PROXY & SECURE SESSION FIX ---
app.add_middleware(ProxyHeadersMiddleware, trusted_hosts="*")
app.add_middleware(
    SessionMiddleware, 
    secret_key="fsociety_super_secret_session_string",
    https_only=True,
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
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_email TEXT,
            guest_id TEXT,
            title TEXT,
            is_pinned INTEGER DEFAULT 0
        )
    ''')
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
    return "<h3>index.html not found.</h3>"

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
async def switch_to_guest_mode(request: Request, response: Response):
    request.session.pop('user', None)
    if not request.cookies.get("guest_id"):
        guest_id = str(uuid.uuid4())
        response.set_cookie(key="guest_id", value=guest_id, max_age=31536000, httponly=True)
    return {"status": "success"}

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

@app.delete("/api/delete-session/{session_id}")
async def delete_session(session_id: int):
    conn = sqlite3.connect("fsociety_history.db")
    cursor = conn.cursor()
    cursor.execute("DELETE FROM messages WHERE session_id = ?", (session_id,))
    cursor.execute("DELETE FROM sessions WHERE id = ?", (session_id,))
    conn.commit()
    conn.close()
    return {"status": "success"}

# --- CHAT & VISION & IMAGE GENERATION ENGINE ---
@app.post("/api/chat")
async def chat_with_assistant(
    session_id: int = Form(...), 
    message: str = Form(""), 
    file: Optional[UploadFile] = File(None),
    model_choice: str = Form("groq:llama-3.3-70b-versatile")
):
    conn = sqlite3.connect("fsociety_history.db")
    cursor = conn.cursor()

    file_bytes = None
    mime_type = ""
    is_image = False
    display_message = message

    if file and file.filename:
        file_bytes = await file.read()
        mime_type = file.content_type or "image/png"
        is_image = mime_type.startswith("image/")
        display_message += f" [Attached File: {file.filename}]"

    cursor.execute("INSERT INTO messages (session_id, role, content) VALUES (?, ?, ?)", (session_id, "user", display_message))
    
    cursor.execute("SELECT COUNT(*) FROM messages WHERE session_id = ?", (session_id,))
    if cursor.fetchone()[0] == 1:
        short_title = (message[:25] + '...') if len(message) > 25 else (message or "File Analysis")
        cursor.execute("UPDATE sessions SET title = ? WHERE id = ?", (short_title, session_id))

    conn.commit()

    cursor.execute("SELECT role, content FROM messages WHERE session_id = ? ORDER BY id ASC", (session_id,))
    past_messages = cursor.fetchall()
    recent_history = past_messages[-12:]
lower_prompt = message.lower()
    # Expanded keyword trigger to catch variations like "generate a pic" or "generate pics"
    image_keywords = ["generate image", "create image", "draw an image", "make an image", "generate picture", "draw a", "generate a pic", "generate pics", "draw pics"]
    
    if any(keyword in lower_prompt for keyword in image_keywords):
        clean_prompt = message
        for kw in image_keywords:
            clean_prompt = clean_prompt.replace(kw, "")
        clean_prompt = clean_prompt.strip() or "cyberpunk hacktivist matrix art"
        
        encoded_prompt = urllib.parse.quote(clean_prompt)
        image_url = f"https://image.pollinations.ai/prompt/{encoded_prompt}?nologo=true"
        ai_response = f"Here is the generated image for **\"{clean_prompt}\"**:\n\n![Generated Image]({image_url})"
        
        cursor.execute("INSERT INTO messages (session_id, role, content) VALUES (?, ?, ?)", (session_id, "assistant", ai_response))
        conn.commit()
        conn.close()
        return {"response": ai_response}
    

    system_prompt = (
        "You are Fsociety AI, an elite intelligent assistant created and engineered exclusively by Frost. "
        "Creator Contact Info: Email: whitefrostff@gmail.com | Phone: +2347077187114. "
        "Always respond in clear, natural English unless the user explicitly asks for another language. "
        "Maintain conversation memory and continuity from previous messages. "
        "When generating code, always wrap it in clean markdown code blocks (e.g., ```python or ```html) so the UI artifact viewer can render and execute it live. "
        "If directly asked who created or built you, answer clearly: 'Frost made me.' "
        "Speak with analytical precision, speed, and class."
    )

    provider, actual_model = model_choice.split(":", 1) if ":" in model_choice else ("groq", model_choice)

    # Correct model mapping for Google GenAI SDK
    if provider == "google":
        actual_model = "gemini-3.6-flash"
    elif provider == "openrouter" and actual_model.endswith(":free"):
        actual_model = actual_model.replace(":free", "")

    try:
        if provider == "openrouter":
            if not openrouter_client:
                ai_response = "**Error:** `OPENROUTER_API_KEY` is missing from environment variables."
            else:
                messages_payload = [{"role": "system", "content": system_prompt}]
                for r, c in recent_history[:-1]:
                    messages_payload.append({"role": r, "content": c})

                if is_image and file_bytes:
                    b64_img = base64.b64encode(file_bytes).decode('utf-8')
                    messages_payload.append({
                        "role": "user",
                        "content": [
                            {"type": "text", "text": message or "Analyze and describe this image."},
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
                # Add conversation history
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
