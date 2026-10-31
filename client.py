"""Terminal client for the WebSocket messenger.

Usage:
    python client.py <username>

Commands:
    <text>                    send a message to the public chat
    /private <user> <text>    send a private message to one user
    /quit                     disconnect
"""
import asyncio
import json
import sys

import websockets

URI = "ws://localhost:8000/ws/{}"


def print_msg(data: dict) -> None:
    if data["type"] == "system":
        print(f"[system] {data['text']}  (online: {', '.join(data['users'])})")
    elif data["type"] == "error":
        print(f"[error] {data['text']}")
    elif data["type"] == "message":
        if data["scope"] == "private":
            print(f"[private] {data['from']} -> {data['to']}: {data['text']}")
        else:
            print(f"[public] {data['from']}: {data['text']}")


async def receiver(ws):
    try:
        async for raw in ws:
            print_msg(json.loads(raw))
    except websockets.exceptions.ConnectionClosed as e:
        if e.code not in (1000, 1001):
            print(f"\n[disconnected] code={e.code} reason={e.reason}")


async def main() -> None:
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(1)
    username = sys.argv[1]
    print("connecting...")
    try:
        async with websockets.connect(URI.format(username)) as ws:
            recv_task = asyncio.create_task(receiver(ws))
            print(
                "connected. Type text for the public chat, "
                "/private <user> <text> for a DM, /quit to leave."
            )
            while True:
                line = (await asyncio.to_thread(input, "> ")).strip()
                if not line:
                    continue
                if line == "/quit":
                    break
                if line.startswith("/private "):
                    parts = line.split(" ", 2)
                    if len(parts) < 3 or not parts[1]:
                        print("usage: /private <username> <text>")
                        continue
                    await ws.send(
                        json.dumps({"type": "private", "to": parts[1], "text": parts[2]})
                    )
                else:
                    await ws.send(json.dumps({"type": "public", "text": line}))
            recv_task.cancel()
    except websockets.exceptions.ConnectionClosed as e:
        print(f"\ncould not stay connected: code={e.code} reason={e.reason}")
    except OSError as e:
        print(f"\ncould not connect: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
