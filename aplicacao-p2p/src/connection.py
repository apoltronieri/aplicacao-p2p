import json
import socket
import threading

from src.file_transfer import decode_file_header, receive_file


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


def connect_to_peer(ip: str, port: int) -> socket.socket:
    """Cria uma conexão TCP com outro peer."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((ip, port))
    return sock


def start_tcp_server(port: int) -> None:
    """Inicia o servidor TCP em uma thread separada."""
    server_thread = threading.Thread(
        target=_run_tcp_server,
        args=(port,),
        daemon=True
    )
    server_thread.start()


def _run_tcp_server(port: int) -> None:
    """Mantém o servidor TCP escutando conexões."""
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("", port))
    server_socket.listen()

    print(f"Servidor TCP escutando na porta {port}...")

    while True:
        client_socket, address = server_socket.accept()

        client_thread = threading.Thread(
            target=_handle_client,
            args=(client_socket, address),
            daemon=True
        )
        client_thread.start()


def _handle_client(client_socket: socket.socket, address) -> None:
    """Recebe e processa uma mensagem TCP."""

    try:
        buffer = b""

        # Lê até encontrar o fim do cabeçalho
        while b"\n" not in buffer:
            chunk = client_socket.recv(4096)

            if not chunk:
                return

            buffer += chunk

        header_bytes, remaining = buffer.split(b"\n", 1)

        # Primeiro tenta interpretar como mensagem de chat
        msg = decode_chat_message(header_bytes)

        if msg:
            print(f"\n[Mensagem de {msg['sender_name']}]: {msg['content']}")
            print(">> ", end="", flush=True)
            return

        # Depois tenta interpretar como transferência de arquivo
        header = decode_file_header(header_bytes)

        if header:
            caminho = receive_file(
                client_socket,
                header,
                initial_data=remaining
            )

            print(f"\nArquivo recebido: {caminho}")
            print(">> ", end="", flush=True)
            return

        print(f"\nMensagem inválida recebida de {address}")
        print(">> ", end="", flush=True)

    except Exception as e:
        print(f"\nErro ao receber mensagem de {address}: {e}")
        print(">> ", end="", flush=True)

    finally:
        client_socket.close()