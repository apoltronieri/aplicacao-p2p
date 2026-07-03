import threading
import socket
from src.connection import decode_chat_message

class TcpMessageReceiver:
    """Recebe mensagens TCP concorrentemente em uma thread dedicada."""
    
    def __init__(self, connection_socket: socket.socket):
        self.sock = connection_socket
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._listen_for_messages,
            name="tcp-message-receiver",
            daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
            self.sock.close()
        except OSError:
            pass

    def _listen_for_messages(self) -> None:
        buffer = b""
        while not self._stop_event.is_set():
            try:
                chunk = self.sock.recv(1024)
                if not chunk:
                    break # Conexão fechada
                
                buffer += chunk
                if b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    msg = decode_chat_message(line)
                    if msg:
                        print(f"\r[Mensagem de {msg['sender_name']}]: {msg['content']}")
                        print(">> ", end="", flush=True) 
            except (socket.error, ConnectionResetError):
                break