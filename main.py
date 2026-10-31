"""WebSocket Messenger - FastAPI server.

Run:  .venv/bin/uvicorn main:app --reload
Docs: README.md
"""
import json
from datetime import datetime, timezone

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles

app = FastAPI(title="WebSocket Messenger")


def now() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


class ConnectionManager:
    """Registry of open sockets + the message router.

    A WebSocket is only a pipe. The server itself decides who gets what,
    and this class is that decision-maker ("the router").
    """

    def __init__(self) -> None:
        self.connections: dict[str, WebSocket] = {}

    @property
    def users(self) -> list[str]:
        return sorted(self.connections)

    async def connect(self, username: str, websocket: WebSocket) -> None:
        await websocket.accept()  # completes the handshake (101 Switching Protocols)
        self.connections[username] = websocket

    def disconnect(self, username: str) -> None:
        self.connections.pop(username, None)

    async def send_to(self, username: str, payload: dict) -> bool:
        """Send to one user. Returns False if the user is offline."""
        ws = self.connections.get(username)
        if ws is None:
            return False
        await ws.send_json(payload)
        return True

    async def broadcast(self, payload: dict, exclude: str | None = None) -> None:
        """Send to every connected user (the public chat), optionally skipping one."""
        for username, ws in list(self.connections.items()):
            if username != exclude:
                await ws.send_json(payload)


manager = ConnectionManager()


def system(text: str) -> dict:
    return {"type": "system", "text": text, "users": manager.users, "time": now()}


def message(scope: str, sender: str, text: str, to: str | None = None) -> dict:
    return {
        "type": "message",
        "scope": scope,  # "public" or "private"
        "from": sender,
        "to": to,
        "text": text,
        "time": now(),
    }


@app.websocket("/ws/{username}")
async def chat(websocket: WebSocket, username: str) -> None:
    if username in manager.connections:
        # Custom close code + reason; the client can read both.
        await websocket.accept()
        await websocket.close(code=4001, reason="Username already taken")
        return

    await manager.connect(username, websocket)
    # The joiner gets a personal welcome instead of their own "joined" notice.
    await manager.broadcast(system(f"{username} joined"), exclude=username)
    await manager.send_to(username, system(f"Welcome, {username}!"))

    try:
        while True:
            data = await websocket.receive_json()
            kind = data.get("type")
            text = str(data.get("text", ""))

            if kind == "public":
                await manager.broadcast(message("public", username, text))

            elif kind == "private":
                target = data.get("to")
                if not target:
                    await manager.send_to(
                        username,
                        {"type": "error", "text": "A private message needs a 'to' field"},
                    )
                    continue
                if target == username:
                    await manager.send_to(
                        username,
                        {"type": "error", "text": "You cannot DM yourself"},
                    )
                    continue
                delivered = await manager.send_to(
                    target, message("private", username, text, to=target)
                )
                if not delivered:
                    await manager.send_to(
                        username,
                        {"type": "error", "text": f"{target} is not online"},
                    )
                    continue
                # Echo back so the sender sees their own DM.
                await manager.send_to(username, message("private", username, text, to=target))

            else:
                await manager.send_to(
                    username,
                    {"type": "error", "text": f"Unknown message type: {kind!r}"},
                )

    except WebSocketDisconnect:
        pass
    except json.JSONDecodeError:
        pass
    finally:
        manager.disconnect(username)
        await manager.broadcast(system(f"{username} left"))


# Serve the browser client. Mounted last so /ws/* is matched first.
app.mount("/", StaticFiles(directory="static", html=True), name="static")
