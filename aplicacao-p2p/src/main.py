import sys
import time
from src.discovery import PeerRegistry, UdpAnnouncer, UdpDiscoveryListener
from src.file_transfer import send_file
from src.connection import (
    send_chat_message,
    connect_to_peer,
    start_tcp_server,
)
from src.utils import mask_ip

"""Ponto de entrada da Aplicação P2P."""

def start_terminal(my_name: str, registry: PeerRegistry):
    print("=== Aplicação P2P ===")
    print("Comandos disponíveis: /peers, /msg <nome_do_peer> <texto>, /file <nome_do_peer> <caminho_arquivo> /sair")
    
    while True:
        try:
            cmd_line = input(">> ").strip()
            
            if not cmd_line:
                continue
                
            parts = cmd_line.split(" ", 2)
            command = parts[0].lower()

            if command == "/sair":
                print("Encerrando...")
                break
                
            elif command == "/peers":
                peers = registry.list_peers()
                print(f"Peers online ({len(peers)}):")
                if not peers:
                    print(" Nenhum outro peer encontrado ainda.")
                for p in peers:                   
                    print(f" - {p.name} ({mask_ip(p.ip)}:{p.tcp_port})")

            elif command == "/msg":
                if len(parts) < 3:
                    print("Uso correto: /msg <nome_do_peer> <texto da mensagem>")
                    continue
                
                target_name = parts[1]
                content = parts[2]
                
                target_peer = next((p for p in registry.list_peers() if p.name == target_name), None)
                
                if not target_peer:
                    print(f"Peer '{target_name}' não encontrado.")
                    continue

                try:
                    sock = connect_to_peer(target_peer.ip, target_peer.tcp_port)
                    send_chat_message(sock, my_name, content)
                    print(f"Mensagem enviada para {target_name}.")
                    sock.close() # Fecha a conexão após o envio da mensagem
                except Exception as e:
                    print(f"Erro ao enviar mensagem para {target_name}: {e}")

            elif command == "/file":
                if len(parts) < 3:
                    print("Uso correto: /msg <nome_do_peer> <caminho do arquivo>")
                    continue
                 
                target_name = parts[1]
                content = parts[2]
                
                target_peer = next((p for p in registry.list_peers() if p.name == target_name), None)
                
                if not target_peer:
                    print(f"Peer '{target_name}' não encontrado.")
                    continue
                try:
                    sock = connect_to_peer(target_peer.ip, target_peer.tcp_port)
                    send_file(sock, my_name, content)
                    print(f"Arquivo enviado para {target_name}.")
                    sock.close() # Fecha a conexão após o envio da mensagem
                except Exception as e:
                    print(f"Erro ao enviar arquivo para {target_name}: {e}")

            else:
                print("Comando desconhecido.")
                
        except KeyboardInterrupt:
            print("\nEncerrando...")
            break
        except Exception as e:
            print(f"Erro no terminal: {e}")

def main():
    if len(sys.argv) < 3:
        print("Uso correto: python -m src.main <seu_nome> <sua_porta_tcp>")
        sys.exit(1)

    my_name = sys.argv[1]
    try:
        my_tcp_port = int(sys.argv[2])
    except ValueError:
        print("Erro: A porta TCP deve ser um número inteiro.")
        sys.exit(1)

    print(f"Iniciando peer '{my_name}' na porta TCP {my_tcp_port}...")

    registry = PeerRegistry()
    announcer = UdpAnnouncer(name=my_name, tcp_port=my_tcp_port)
    listener = UdpDiscoveryListener(
        registry=registry,
        own_peer_id=announcer.announcement.peer_id
    )
    
    listener.start()
    announcer.start()
    start_tcp_server(my_tcp_port)
    time.sleep(0.5) 

    try:
        start_terminal(my_name, registry)
    finally:
        announcer.stop()
        listener.stop()

if __name__ == "__main__":
    main()