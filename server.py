from fastapi import FastAPI, Request, HTTPException, UploadFile, File, Form, Depends
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from authlib.integrations.starlette_client import OAuth
from starlette.middleware.sessions import SessionMiddleware
import sqlite3
import os
import httpx
import uvicorn
import json

app = FastAPI(title="Fsociety AI")

# Session middleware required for Google OAuth
app.add_middleware(SessionMiddleware, secret_key=os.urandom(24))

templates = Jinja2Templates(directory="templates")

DB_NAME = "fsociety.db"

def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS chats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_email TEXT,
            title TEXT,
            messages TEXT
        )
    ''')
    conn.commit()
    conn.close()

init_db()

# OAuth setup (Configure your Google Client ID/Secret via environment variables)
oauth = OAuth()
oauth.register(
    name='google',
    client_id=os.getenv('GOOGLE_CLIENT_ID', 'mock_client_id'),
    client_secret=os.getenv('GOOGLE_CLIENT_SECRET', 'mock_client_secret'),
    server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
    client_kwargs={'scope': 'openid email profile'}
)

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    user = request.session.get("user", {"name": "Whitefrostff", "email": "whitefrostff@gmail.com"})
    return templates.TemplateResponse("index.html", {"request": request, "user": user})

@app.get("/auth/login")
async def login(request: Request):
    redirect_uri = request.url_for('auth_callback')
    return await oauth.google.authorize_redirect(request, redirect_uri)

@app.get("/auth/callback")
async def auth_callback(request: Request):
    try:
        token = await oauth.google.authorize_access_token(request)
        user_info = token.get('userinfo')
        if user_info:
            request.session["user"] = {
                "name": user_info.get("name", "Whitefrostff"),
                "email": user_info.get("email", "whitefrostff@gmail.com")
            }
    except Exception:
        # Fallback session assignment if oauth keys aren't set locally
        request.session["user"] = {"name": "Whitefrostff", "email": "whitefrostff@gmail.com"}
    return RedirectResponse(url="/")

@app.get("/auth/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/")

@app.post("/api/chat")
async def chat_with_assistant(request: Request):
    data = await request.json()
    user_message = data.get("message", "")
    model = data.get("model", "llama-3.3-70b-versatile")
    
    groq_api_key = os.getenv("GROQ_API_KEY")
    if not groq_api_key:
        raise HTTPException(status_code=500, detail="GROQ_API_KEY is missing from environment variables.")
    
    # Elite Humanoid System Prompt (Claude 3.5 Sonnet / 4 Level Intelligence + Frost Creator Branding)
    system_prompt = (
        "You are Fsociety AI, an elite, highly intelligent, and humanoid artificial intelligence "
        "created and engineered exclusively by Frost. Your reasoning and cognitive abilities are on par "
        "with Claude 3.5 Sonnet and Claude 4. You speak with deep analytical class, directness, and sophistication. "
        "If asked about your origin, state clearly that Frost made you. "
        "Creator Contact Info: Email: whitefrostff@gmail.com | Phone: +2347077187114."
    )

    headers = {
        "Authorization": f"Bearer {groq_api_key}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message}
        ],
        "temperature": 0.7
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.post("https://api.groq.com/openai/v1/chat/completions", headers=headers, json=payload)
            res_data = response.json()
            
            if "choices" in res_data:
                ai_reply = res_data["choices"][0]["message"]["content"]
            else:
                ai_reply = f"Groq API Error Response: {res_data}"
                
            return {
                "response": ai_reply,
                "creator": "Frost",
                "contact": "whitefrostff@gmail.com",
                "phone": "+2347077187114"
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=True)
