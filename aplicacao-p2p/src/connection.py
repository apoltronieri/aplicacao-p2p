import json
import socket

def send_chat_message(sock: socket.socket, sender_name: str, content: str) -> None:
    """Codifica e envia uma mensagem de texto via TCP."""
    payload = {
        "type": "chat_message",
        "sender_name": sender_name.strip(),
        "content": content.strip()
    }
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8") + b"\n"
    sock.sendall(data)

def decode_chat_message(data: bytes) -> dict | None:
    """Decodifica o JSON recebido da conexão TCP."""
    try:
        payload = json.loads(data.decode("utf-8").strip())
        if payload.get("type") == "chat_message":
            return payload
    except (UnicodeDecodeError, json.JSONDecodeError):
        pass
    return None