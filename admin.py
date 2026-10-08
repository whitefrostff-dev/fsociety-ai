"""
admin.py — Ranen admin panel backend.

Access rule: the logged-in Google session email must equal ADMIN_EMAIL.
The check happens on the SERVER for every admin route, so hiding the button
in the frontend is only cosmetic.

Drop this file next to server.py (and admin.html next to both).
"""
import os
import json
from datetime import datetime

from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse

ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "whitefrostff@gmail.com").strip().lower()
HERE = os.path.dirname(os.path.abspath(__file__))


def is_admin(request: Request) -> bool:
    user = request.session.get("user")
    if not user:
        return False
    email = (user.get("email") or "").strip().lower()
    if user.get("email_verified") is False:
        return False
    return email == ADMIN_EMAIL


def require_admin(request: Request):
    """Use as a dependency or call directly: require_admin(request)."""
    if not is_admin(request):
        raise HTTPException(status_code=403, detail="Admin access only.")


def _today() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def create_admin_router(get_db_connection, load_local_chats, save_local_chats,
                        local_usage_counts, get_limit):
    router = APIRouter()

    # ---------- data helpers (Postgres first, local json fallback) ----------
    def all_chats():
        """Every chat across every identifier, without message bodies."""
        conn = get_db_connection()
        if conn:
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT user_email, chat_id, title, created_at,
                               COALESCE(jsonb_array_length(
                                   CASE WHEN jsonb_typeof(messages) = 'array'
                                        THEN messages ELSE '[]'::jsonb END), 0) AS n
                        FROM user_chats ORDER BY created_at DESC LIMIT 2000
                    """)
                    rows = cur.fetchall()
                conn.close()
                return "postgres", [{
                    "identifier": r["user_email"], "chat_id": r["chat_id"],
                    "title": r.get("title") or "Untitled",
                    "messages": r["n"],
                    "created_at": r["created_at"].isoformat() if r.get("created_at") else "",
                } for r in rows]
            except Exception as e:
                print(f"[ADMIN] chats query error: {e}")
                try:
                    conn.close()
                except Exception:
                    pass
        out = []
        for ident, chats in load_local_chats().items():
            for cid, info in chats.items():
                out.append({
                    "identifier": ident, "chat_id": cid,
                    "title": info.get("title") or "Untitled",
                    "messages": len(info.get("messages", [])), "created_at": "",
                })
        return "local_file", out

    def usage_today():
        """{identifier: message_count} for today."""
        conn = get_db_connection()
        if conn:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT identifier, message_count FROM usage_limits WHERE usage_date = %s", (_today(),))
                    rows = cur.fetchall()
                conn.close()
                return {r["identifier"]: r["message_count"] for r in rows}
            except Exception as e:
                print(f"[ADMIN] usage query error: {e}")
                try:
                    conn.close()
                except Exception:
                    pass
        t = _today()
        return {k[0]: v for k, v in local_usage_counts.items() if k[1] == t}

    # ---------- pages ----------
    @router.get("/admin", response_class=HTMLResponse)
    async def admin_page(request: Request):
        if not request.session.get("user"):
            return RedirectResponse(url="/auth/login")
        if not is_admin(request):
            return HTMLResponse("<h3>403 — this account is not an admin.</h3><a href='/'>Back</a>", status_code=403)
        for p in (os.path.join(HERE, "admin.html"), os.path.join(HERE, "..", "app", "admin.html"), "admin.html"):
            if os.path.exists(p):
                with open(p, "r", encoding="utf-8") as f:
                    return HTMLResponse(f.read())
        return HTMLResponse("<h3>admin.html not found next to server.py</h3>", status_code=500)

    # ---------- API ----------
    @router.get("/api/admin/me")
    async def admin_me(request: Request):
        # Never 403 here: the main app calls this just to decide whether to show the Admin button.
        return {"is_admin": is_admin(request)}

    @router.get("/api/admin/stats")
    async def admin_stats(request: Request):
        require_admin(request)
        backend, chats = all_chats()
        usage = usage_today()
        idents = {c["identifier"] for c in chats}
        return {
            "storage": backend,
            "total_chats": len(chats),
            "total_messages": sum(c["messages"] for c in chats),
            "accounts": len([i for i in idents if "@" in (i or "")]),
            "guests": len([i for i in idents if "@" not in (i or "")]),
            "messages_today": sum(usage.values()),
            "active_today": len(usage),
            "daily_limit": get_limit(),
        }

    @router.get("/api/admin/users")
    async def admin_users(request: Request):
        require_admin(request)
        _, chats = all_chats()
        usage = usage_today()
        agg = {}
        for c in chats:
            a = agg.setdefault(c["identifier"], {"identifier": c["identifier"], "chats": 0, "messages": 0})
            a["chats"] += 1
            a["messages"] += c["messages"]
        for ident, count in usage.items():
            agg.setdefault(ident, {"identifier": ident, "chats": 0, "messages": 0})
        rows = []
        for a in agg.values():
            a["is_guest"] = "@" not in (a["identifier"] or "")
            a["today"] = usage.get(a["identifier"], 0)
            rows.append(a)
        rows.sort(key=lambda r: (r["today"], r["messages"]), reverse=True)
        return rows

    @router.get("/api/admin/chats")
    async def admin_chats(request: Request, identifier: str):
        require_admin(request)
        _, chats = all_chats()
        return [c for c in chats if c["identifier"] == identifier]

    @router.get("/api/admin/chat")
    async def admin_chat(request: Request, identifier: str, chat_id: str):
        require_admin(request)
        conn = get_db_connection()
        if conn:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT title, messages FROM user_chats WHERE user_email = %s AND chat_id = %s",
                                (identifier, chat_id))
                    row = cur.fetchone()
                conn.close()
                if row:
                    msgs = row["messages"]
                    if isinstance(msgs, str):
                        msgs = json.loads(msgs)
                    return {"title": row["title"], "messages": msgs or []}
            except Exception as e:
                print(f"[ADMIN] chat read error: {e}")
                try:
                    conn.close()
                except Exception:
                    pass
        info = load_local_chats().get(identifier, {}).get(chat_id)
        if not info:
            raise HTTPException(status_code=404, detail="Chat not found")
        return {"title": info.get("title"), "messages": info.get("messages", [])}

    @router.delete("/api/admin/chat")
    async def admin_delete_chat(request: Request, identifier: str, chat_id: str):
        require_admin(request)
        conn = get_db_connection()
        if conn:
            try:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM user_chats WHERE user_email = %s AND chat_id = %s", (identifier, chat_id))
                    conn.commit()
                conn.close()
            except Exception as e:
                print(f"[ADMIN] delete error: {e}")
                try:
                    conn.close()
                except Exception:
                    pass
        local = load_local_chats()
        if identifier in local and chat_id in local[identifier]:
            del local[identifier][chat_id]
            save_local_chats(local)
        return {"status": "deleted"}

    @router.post("/api/admin/reset-usage")
    async def admin_reset_usage(request: Request, identifier: str):
        require_admin(request)
        conn = get_db_connection()
        if conn:
            try:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM usage_limits WHERE identifier = %s AND usage_date = %s",
                                (identifier, _today()))
                    conn.commit()
                conn.close()
            except Exception as e:
                print(f"[ADMIN] reset error: {e}")
                try:
                    conn.close()
                except Exception:
                    pass
        local_usage_counts.pop((identifier, _today()), None)
        return {"status": "reset"}

    return router
