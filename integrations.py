"""
integrations.py - MongoDB Atlas chat mirror + Firebase login verification.

Put this file in the same folder as server.py (src/).

Environment variables (set them on Render):
  MONGODB_URI                 your Atlas connection string (optional - without it the mirror is off)
  MONGODB_DB                  database name (optional, default "ranen")
  FIREBASE_SERVICE_ACCOUNT    the FULL contents of your Firebase service-account JSON key
                              (or set GOOGLE_APPLICATION_CREDENTIALS to a file path instead)

Safe by design: if a package or a variable is missing, this file does NOT crash the server.
The related feature simply turns itself off and prints a message in the logs.
"""
import os
import json
import threading
from datetime import datetime, timezone

# ---------------------------------------------------------------- MongoDB Atlas
MONGODB_URI = (os.environ.get("MONGODB_URI") or "").strip().strip('"').strip("'")
MONGODB_DB = (os.environ.get("MONGODB_DB") or "ranen").strip()

try:
    from pymongo import MongoClient
    HAS_PYMONGO = True
except ImportError:
    HAS_PYMONGO = False
    print("[integrations] pymongo is not installed - MongoDB mirror is OFF (add `pymongo` to requirements.txt)")

_mongo_client = None
_mongo_lock = threading.Lock()


def _chats_collection():
    """Returns the chats collection, or None if MongoDB isn't configured."""
    global _mongo_client
    if not (MONGODB_URI and HAS_PYMONGO):
        return None
    try:
        with _mongo_lock:
            if _mongo_client is None:
                _mongo_client = MongoClient(
                    MONGODB_URI,
                    serverSelectionTimeoutMS=5000,
                    connectTimeoutMS=5000,
                    socketTimeoutMS=10000,
                )
        return _mongo_client[MONGODB_DB]["chats"]
    except Exception as e:
        print(f"[integrations] MongoDB connect error: {e}")
        return None


def _save_chat_blocking(user_email, chat_id, title, messages):
    col = _chats_collection()
    if col is None:
        return
    try:
        col.update_one(
            {"user_email": user_email, "chat_id": str(chat_id)},
            {"$set": {
                "title": title,
                "messages": messages,
                "updated_at": datetime.now(timezone.utc),
            }},
            upsert=True,
        )
    except Exception as e:
        print(f"[integrations] MongoDB save error: {e}")


def mongo_save_chat(user_email, chat_id, title, messages):
    """Mirror a chat to Atlas in a background thread so replies are never slowed down."""
    if not (MONGODB_URI and HAS_PYMONGO):
        return
    threading.Thread(
        target=_save_chat_blocking,
        args=(user_email, chat_id, title, list(messages)),
        daemon=True,
    ).start()


def mongo_get_chat(user_email, chat_id):
    """Returns {"title": ..., "messages": [...]} or None."""
    col = _chats_collection()
    if col is None:
        return None
    try:
        doc = col.find_one({"user_email": user_email, "chat_id": str(chat_id)})
        if not doc:
            return None
        return {"title": doc.get("title") or "New Chat", "messages": doc.get("messages") or []}
    except Exception as e:
        print(f"[integrations] MongoDB read error: {e}")
        return None


def mongo_list_chats(user_email):
    """Returns [{"id", "title", "is_pinned"}] newest first (same shape as /api/sessions)."""
    col = _chats_collection()
    if col is None:
        return []
    try:
        cursor = col.find(
            {"user_email": user_email},
            {"chat_id": 1, "title": 1},
        ).sort("updated_at", -1)
        return [
            {"id": d["chat_id"], "title": d.get("title") or "Untitled Chat", "is_pinned": 0}
            for d in cursor
        ]
    except Exception as e:
        print(f"[integrations] MongoDB list error: {e}")
        return []


def mongo_delete_chat(user_email, chat_id):
    col = _chats_collection()
    if col is None:
        return
    try:
        col.delete_one({"user_email": user_email, "chat_id": str(chat_id)})
    except Exception as e:
        print(f"[integrations] MongoDB delete error: {e}")


# ---------------------------------------------------------------- Firebase login
firebase_ready = False
_fb_auth = None

try:
    import firebase_admin
    from firebase_admin import credentials, auth as _fb_auth_module

    _raw_key = (os.environ.get("FIREBASE_SERVICE_ACCOUNT") or "").strip()
    _cred = None
    if _raw_key:
        _cred = credentials.Certificate(json.loads(_raw_key))
    elif os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        _cred = credentials.ApplicationDefault()

    if _cred is not None:
        if not firebase_admin._apps:
            firebase_admin.initialize_app(_cred)
        _fb_auth = _fb_auth_module
        firebase_ready = True
        print("[integrations] Firebase ready.")
    else:
        print("[integrations] FIREBASE_SERVICE_ACCOUNT is not set - Google login is OFF")
except ImportError:
    print("[integrations] firebase-admin is not installed - Google login is OFF (add `firebase-admin` to requirements.txt)")
except Exception as e:
    print(f"[integrations] Firebase setup failed: {e}")
    firebase_ready = False


def verify_firebase_token(id_token):
    """Checks a Firebase ID token and returns the decoded claims
    (uid, email, email_verified, name, ...). Raises if the token is bad."""
    if not firebase_ready or _fb_auth is None:
        raise RuntimeError("Firebase is not configured on the server")
    if not id_token:
        raise ValueError("Missing id_token")
    return _fb_auth.verify_id_token(id_token, clock_skew_seconds=10)
