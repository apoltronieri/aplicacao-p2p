import json
import os
import socket


BUFFER_SIZE = 4096


def create_file_header(sender_name: str, filepath: str) -> dict:
    """
    Cria o cabeçalho de um arquivo.
    """

    if not os.path.isfile(filepath):
        raise FileNotFoundError(f"Arquivo não encontrado: {filepath}")

    return {
        "type": "file_transfer",
        "sender_name": sender_name.strip(),
        "filename": os.path.basename(filepath),
        "filesize": os.path.getsize(filepath),
    }

def send_file(sock: socket.socket, sender_name: str, filepath: str) -> None:
    """
    Envia um arquivo.
    """

    header = create_file_header(sender_name, filepath)

    sock.sendall(
        json.dumps(header, ensure_ascii=False).encode("utf-8") + b"\n"
    )

    with open(filepath, "rb") as file:
        while True:
            chunk = file.read(BUFFER_SIZE)

            if not chunk:
                break

            sock.sendall(chunk)


def decode_file_header(data: bytes) -> dict | None:
    """
    Decodifica um cabeçalho de transferência de arquivo.
    """

    try:
        payload = json.loads(data.decode("utf-8").strip())

        if payload.get("type") != "file_transfer":
            return None

        return payload

    except (UnicodeDecodeError, json.JSONDecodeError):
        return None

def receive_file(
    sock: socket.socket,
    header: dict,
    save_dir: str = "downloads",
    initial_data: bytes = b""
) -> str:
    """
    Recebe um arquivo utilizando um cabeçalho já validado.
    """

    os.makedirs(save_dir, exist_ok=True)

    filename = header["filename"]
    filesize = header["filesize"]

    filepath = os.path.join(save_dir, filename)

    received = len(initial_data)

    with open(filepath, "wb") as file:

        # escreve os bytes que ja vieram junto do cabeçalho
        if initial_data:
            file.write(initial_data)

        while received < filesize:

            chunk = sock.recv(min(BUFFER_SIZE, filesize - received))

            if not chunk:
                raise ConnectionError(
                    "Conexão encerrada durante a transferência."
                )

            file.write(chunk)
            received += len(chunk)

    return filepath