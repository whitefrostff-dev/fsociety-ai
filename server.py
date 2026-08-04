from fastapi import FastAPI, Form, File, UploadFile, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
from starlette.middleware.sessions import SessionMiddleware
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

# --- NEW: FRONTEND AUTH CHECK ENDPOINT (FIXES THE "GUEST USER" ISSUE) ---
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
    # FIX: Using RedirectResponse instead of JS script stops the black screen flash on mobile
    return RedirectResponse(url="/")

@app.get('/auth/logout')
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/")

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
    return {"session_id":Here is the complete codebase for Fsociety AI, separated into the frontend interface and the backend server components. 

### Frontend (`index.html`)
Save this file in your project directory (or within an `app/` folder as referenced in your backend path setup). This handles the UI, session management, modal logic, and API calls.

```html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Fsociety AI</title>
    <script src="[https://cdn.tailwindcss.com](https://cdn.tailwindcss.com)"></script>
    <link rel="stylesheet" href="[https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css](https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css)">
    <script src="[https://cdn.jsdelivr.net/npm/marked/marked.min.js](https://cdn.jsdelivr.net/npm/marked/marked.min.js)"></script>
    <style>
        body { background-color: #0d1117; color: #c9d1d9; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
        .sidebar { background-color: #161b22; }
        .input-box { background-color: #21262d; border: 1px solid #30363d; }
        .user-msg { background-color: #ffffff; color: #000000; border: 1px solid #e5e7eb; }
        .assistant-msg { background-color: transparent; color: #c9d1d9; border: none; padding-left: 0; }
        
        /* MONOCHROME MODAL CARD STYLING */
        .model-card { background-color: #21262d; border: 1px solid #30363d; transition: all 0.2s ease-in-out; }
        .model-card:hover { border-color: #8b949e; background-color: #282e37; }
        .model-card.active { border-color: #ffffff; background-color: #1c2128; }
        .badge-tag { background-color: #30363d; color: #c9d1d9; font-size: 10px; font-weight: 600; padding: 2px 8px; border-radius: 12px; }
    </style>
</head>
<body class="h-screen flex overflow-hidden">

    <!-- SIDEBAR -->
    <aside class="sidebar w-64 flex-shrink-0 flex flex-col justify-between p-4 border-r border-gray-800">
        <div>
            <div class="flex items-center justify-between mb-6">
                <h1 class="text-xl font-bold text-white flex items-center gap-2">
                    <i class="fa-solid fa-terminal text-gray-400"></i> Fsociety AI
                </h1>
                <button onclick="createNewSession()" class="text-gray-400 hover:text-white text-lg" title="New Chat">
                    <i class="fa-solid fa-plus"></i>
                </button>
            </div>
            <div id="session-list" class="space-y-2 overflow-y-auto max-h-[70vh]"></div>
        </div>

        <!-- USER PROFILE AREA -->
        <div id="user-profile" class="border-t border-gray-800 pt-4 flex items-center justify-between">
            <span id="user-display-name" class="text-sm text-gray-400 truncate max-w-[120px]">Guest User</span>
            <a id="auth-btn" href="/auth/login" class="text-xs bg-gray-800 hover:bg-gray-700 border border-gray-700 text-white px-3 py-1.5 rounded-lg">Login</a>
        </div>
    </aside>

    <!-- MAIN CHAT INTERFACE -->
    <main class="flex-1 flex flex-col h-full relative">
        <header class="h-14 border-b border-gray-800 flex items-center justify-between px-6 bg-[#0d1117]">
            <div class="flex items-center gap-3">
                <h2 id="current-chat-title" class="font-semibold text-gray-200">New Chat</h2>
                
                <!-- MODEL SWITCHER TRIGGER BUTTON -->
                <button onclick="openModelModal()" class="flex items-center gap-2 bg-gray-800 hover:bg-gray-700 text-xs text-gray-200 px-3 py-1.5 rounded-lg border border-gray-700">
                    <span id="current-model-label">Llama 3.3 70B</span>
                    <i class="fa-solid fa-chevron-down text-[10px] text-gray-400"></i>
                </button>
            </div>
        </header>

        <div id="messages-container" class="flex-1 overflow-y-auto p-6 space-y-4 max-w-4xl mx-auto w-full"></div>

        <!-- INPUT BOX -->
        <div class="p-4 max-w-4xl mx-auto w-full">
            <form id="chat-form" onsubmit="sendMessage(event)" class="input-box rounded-xl p-3 flex flex-col gap-2 shadow-lg">
                <textarea id="user-input" rows="2" placeholder="Ask Fsociety AI or type 'generate image of...' " class="bg-transparent text-white focus:outline-none resize-none w-full text-sm px-2"></textarea>
                <div class="flex items-center justify-between pt-2 border-t border-gray-800">
                    <div class="flex items-center gap-2">
                        <label for="file-upload" class="cursor-pointer text-gray-400 hover:text-white px-2 py-1 rounded text-sm" title="Upload Image / File">
                            <i class="fa-solid fa-paperclip"></i>
                        </label>
                        <input id="file-upload" type="file" class="hidden" onchange="handleFileSelect(event)">
                        <span id="file-name-display" class="text-xs text-gray-400"></span>
                    </div>
                    <button type="submit" class="bg-white text-black hover:bg-gray-200 font-semibold px-4 py-1.5 rounded-lg text-sm flex items-center gap-2">
                        Send <i class="fa-solid fa-paper-plane text-xs"></i>
                    </button>
                </div>
            </form>
        </div>
    </main>

    <!-- MODEL SELECTION MODAL -->
    <div id="model-modal" class="fixed inset-0 bg-black/80 flex items-center justify-center hidden z-50 p-4">
        <div class="bg-[#161b22] w-full max-w-md rounded-3xl p-6 shadow-2xl border border-gray-800 flex flex-col gap-5">
            <div class="flex items-center gap-4">
                <button onclick="closeModelModal()" class="text-gray-400 hover:text-white text-lg">
                    <i class="fa-solid fa-arrow-left"></i>
                </button>
                <h3 class="text-xl font-bold text-white">More models</h3>
            </div>

            <div class="space-y-3 max-h-[60vh] overflow-y-auto pr-1">
                <div onclick="selectModel('openrouter:meta-llama/llama-3.3-70b-instruct:free', 'Llama 3.3 (OpenRouter)')" class="model-card p-4 rounded-2xl cursor-pointer flex items-center justify-between" id="m-openrouter-llama">
                    <div class="flex items-center gap-3">
                        <span class="text-base font-semibold text-white">Llama 3.3 70B</span>
                        <span class="badge-tag">OpenRouter • Free</span>
                    </div>
                    <i class="fa-solid fa-check text-white text-sm hidden check-icon"></i>
                </div>

                <div onclick="selectModel('openrouter:deepseek/deepseek-r1:free', 'DeepSeek R1 (OpenRouter)')" class="model-card p-4 rounded-2xl cursor-pointer flex items-center justify-between" id="m-openrouter-deepseek">
                    <div class="flex items-center gap-3">
                        <span class="text-base font-semibold text-white">DeepSeek R1</span>
                        <span class="badge-tag">OpenRouter • Free</span>
                    </div>
                    <i class="fa-solid fa-check text-white text-sm hidden check-icon"></i>
                </div>

                <div onclick="selectModel('groq:llama-3.3-70b-versatile', 'Llama 3.3 70B (Groq)')" class="model-card p-4 rounded-2xl cursor-pointer flex items-center justify-between" id="m-groq-llama">
                    <div class="flex items-center gap-3">
                        <span class="text-base font-semibold text-white">Llama 3.3 70B</span>
                        <span class="badge-tag">Groq • Fast</span>
                    </div>
                    <i class="fa-solid fa-check text-white text-sm hidden check-icon"></i>
                </div>

                <div onclick="selectModel('google:gemini-2.5-flash', 'Gemini 2.5 Flash (Vision)')" class="model-card p-4 rounded-2xl cursor-pointer flex items-center justify-between" id="m-google-flash">
                    <div class="flex items-center gap-3">
                        <span class="text-base font-semibold text-white">Gemini 2.5 Flash</span>
                        <span class="badge-tag">Google • Vision</span>
                    </div>
                    <i class="fa-solid fa-check text-white text-sm hidden check-icon"></i>
                </div>

                <div onclick="selectModel('silicon:Qwen/Qwen2.5-7B-Instruct', 'Qwen 2.5 7B')" class="model-card p-4 rounded-2xl cursor-pointer flex items-center justify-between" id="m-silicon-qwen">
                    <div class="flex items-center gap-3">
                        <span class="text-base font-semibold text-white">Qwen 2.5 7B</span>
                        <span class="badge-tag">SiliconFlow</span>
                    </div>
                    <i class="fa-solid fa-check text-white text-sm hidden check-icon"></i>
                </div>
            </div>
        </div>
    </div>

    <script>
        let currentSessionId = null;
        let selectedFile = null;
        let activeModelValue = "groq:llama-3.3-70b-versatile";

        async function init() {
            await checkUserStatus();
            await fetchSessions();
            highlightActiveModelCard();
        }

        async function checkUserStatus() {
            try {
                const res = await fetch('/api/user');
                if (res.ok) {
                    const user = await res.json();
                    if (user && user.email) {
                        document.getElementById('user-display-name').innerText = user.name || user.email;
                        const authBtn = document.getElementById('auth-btn');
                        authBtn.innerText = 'Logout';
                        authBtn.href = '/auth/logout';
                    }
                }
            } catch (e) {
                console.log("Not logged in");
            }
        }

        function openModelModal() { document.getElementById('model-modal').classList.remove('hidden'); }
        function closeModelModal() { document.getElementById('model-modal').classList.add('hidden'); }

        function selectModel(modelValue, labelName) {
            activeModelValue = modelValue;
            document.getElementById('current-model-label').innerText = labelName;
            highlightActiveModelCard();
            closeModelModal();
        }

        function highlightActiveModelCard() {
            document.querySelectorAll('.model-card').forEach(card => {
                card.classList.remove('active');
                card.querySelector('.check-icon')?.classList.add('hidden');
            });

            const cardMap = {
                'openrouter:meta-llama/llama-3.3-70b-instruct:free': 'm-openrouter-llama',
                'openrouter:deepseek/deepseek-r1:free': 'm-openrouter-deepseek',
                'groq:llama-3.3-70b-versatile': 'm-groq-llama',
                'google:gemini-2.5-flash': 'm-google-flash',
                'silicon:Qwen/Qwen2.5-7B-Instruct': 'm-silicon-qwen'
            };

            const activeId = cardMap[activeModelValue];
            if (activeId) {
                const activeCard = document.getElementById(activeId);
                activeCard?.classList.add('active');
                activeCard?.querySelector('.check-icon')?.classList.remove('hidden');
            }
        }

        async function fetchSessions() {
            try {
                const res = await fetch('/api/sessions');
                const sessions = await res.json();
                const list = document.getElementById('session-list');
                list.innerHTML = '';
                
                if (sessions.length > 0) {
                    sessions.forEach(s => {
                        list.innerHTML += `
                            <div class="flex items-center justify-between p-2 hover:bg-gray-800 rounded-lg cursor-pointer text-sm ${currentSessionId === s.id ? 'bg-gray-800 text-white' : 'text-gray-400'}" onclick="loadSession(${s.id}, '${s.title.replace(/'/g, "\\'")}')">
                                <span class="truncate w-40">${s.title}</span>
                                <button onclick="deleteSession(event, ${s.id})" class="hover:text-red-400 text-xs"><i class="fa-solid fa-trash"></i></button>
                            </div>
                        `;
                    });
                    if (!currentSessionId) loadSession(sessions[0].id, sessions[0].title);
                } else {
                    createNewSession();
                }
            } catch (e) {
                console.error(e);
            }
        }

        function renderEmptyState() {
            const container = document.getElementById('messages-container');
            container.innerHTML = `
                <div id="welcome-screen" class="flex flex-col items-center justify-center h-full text-center space-y-4 pt-16">
                    <div class="w-16 h-16 bg-white text-black rounded-full flex items-center justify-center text-3xl shadow-lg">
                        <i class="fa-solid fa-terminal"></i>
                    </div>
                    <h2 class="text-2xl font-bold text-white">Fsociety AI</h2>
                    <p class="text-gray-400 text-sm">How can I help you today?</p>
                </div>
            `;
        }

        async function createNewSession() {
            const res = await fetch('/api/new-session', { method: 'POST' });
            const data = await res.json();
            currentSessionId = data.session_id;
            renderEmptyState();
            document.getElementById('current-chat-title').innerText = 'New Chat';
            fetchSessions();
        }

        async function loadSession(id, title) {
            currentSessionId = id;
            document.getElementById('current-chat-title').innerText = title;
            const res = await fetch(`/api/history/${id}`);
            const messages = await res.json();
            const container = document.getElementById('messages-container');
            container.innerHTML = '';
            
            if (messages.length === 0) {
                renderEmptyState();
            } else {
                messages.forEach(m => renderMessage(m.role, m.content));
            }
            fetchSessions();
        }

        async function deleteSession(e, id) {
            e.stopPropagation();
            await fetch(`/api/delete-session/${id}`, { method: 'DELETE' });
            if (currentSessionId === id) currentSessionId = null;
            fetchSessions();
        }

        function handleFileSelect(e) {
            selectedFile = e.target.files[0];
            if (selectedFile) {
                document.getElementById('file-name-display').innerText = selectedFile.name;
            }
        }

        async function sendMessage(e) {
            e.preventDefault();
            const input = document.getElementById('user-input');
            const text = input.value.trim();
            if (!text && !selectedFile) return;

            renderMessage('user', text + (selectedFile ? ` [File: ${selectedFile.name}]` : ''));
            
            const formData = new FormData();
            formData.append('session_id', currentSessionId);
            formData.append('message', text);
            formData.append('model_choice', activeModelValue);
            
            if (selectedFile) formData.append('file', selectedFile);

            input.value = '';
            document.getElementById('file-name-display').innerText = '';
            selectedFile = null;

            const res = await fetch('/api/chat', { method: 'POST', body: formData });
            const data = await res.json();
            renderMessage('assistant', data.response);
        }

        function renderMessage(role, content) {
            const container = document.getElementById('messages-container');
            const welcomeScreen = document.getElementById('welcome-screen');
            if (welcomeScreen) welcomeScreen.remove();

            const isUser = role === 'user';
            const formattedContent = marked.parse(content);
            const msgHtml = `
                <div class="flex ${isUser ? 'justify-end' : 'justify-start'}">
                    <div class="max-w-2xl rounded-2xl p-4 ${isUser ? 'user-msg' : 'assistant-msg'}">
                        <div class="text-sm prose prose-invert">${formattedContent}</div>
                    </div>
                </div>
            `;
            container.innerHTML += msgHtml;
            container.scrollTop = container.scrollHeight;
        }

        // Initialize when DOM is completely loaded
        document.addEventListener('DOMContentLoaded', init);
    </script>
</body>
</html>
