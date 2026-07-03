# Protocolo de descoberta de peers

A descoberta usa broadcast UDP na rede local. Ela serve apenas para localizar
peers; mensagens e arquivos continuam usando as conexões TCP da aplicação.

## Configuração padrão

| Parâmetro | Valor | Finalidade |
| --- | ---: | --- |
| Porta UDP | `37020` | Envio e recebimento dos anúncios |
| Intervalo | `5 segundos` | Frequência com que cada peer anuncia presença |
| Tempo de inatividade | `15 segundos` | Período sem anúncios antes da remoção do peer |

Os valores podem ser alterados ao criar `UdpAnnouncer` e
`UdpDiscoveryListener`. Todos os peers da mesma rede precisam usar a mesma
porta UDP.

## Formato do anúncio

Cada anúncio é um objeto JSON codificado em UTF-8:

```json
{
  "version": 1,
  "type": "peer_announcement",
  "peer_id": "6ec8bb7bcf5e4b82a228510a4d8ed4fb",
  "name": "ana",
  "tcp_port": 5000
}
```

- `version`: versão do protocolo de descoberta;
- `type`: tipo da mensagem UDP;
- `peer_id`: identificador da instância em execução;
- `name`: nome escolhido pelo usuário;
- `tcp_port`: porta na qual o peer aceita conexões TCP.

O IP não é enviado no JSON. O listener usa o endereço de origem do datagrama,
que representa o IP efetivamente observado pela rede e evita confiar em um
valor declarado pelo remetente.

## Fluxo

1. O peer cria um identificador para a execução.
2. O listener abre a porta UDP de descoberta.
3. O anunciante envia periodicamente o JSON para `255.255.255.255`.
4. Os outros listeners validam a versão, o tipo e os campos recebidos.
5. O anúncio da própria instância é ignorado por meio do `peer_id`.
6. Um anúncio remoto adiciona ou atualiza o peer no `PeerRegistry`.
7. Peers sem anúncios pelo tempo configurado são removidos do registro.

Datagramas inválidos, mensagens desconhecidas e versões incompatíveis são
ignorados sem encerrar o listener.

## Inicialização dos componentes

```python
from src.discovery import PeerRegistry, UdpAnnouncer, UdpDiscoveryListener

registry = PeerRegistry()
announcer = UdpAnnouncer(name="ana", tcp_port=5000)
listener = UdpDiscoveryListener(
    registry,
    own_peer_id=announcer.announcement.peer_id,
)

listener.start()
announcer.start()

# Ao encerrar a aplicação:
announcer.stop()
listener.stop()
```

O futuro comando `/peers` pode obter uma fotografia segura e ordenada do
estado atual chamando `registry.list_peers()`.

## Testes

Na pasta `aplicacao-p2p`, execute:

```bash
python3 -m unittest discover -s tests -v
```

A suíte cobre o formato dos anúncios, o registro e a expiração de peers, o
descarte de mensagens inválidas e o recebimento UDP pela interface de loopback.

## Protocolo de Mensagens (TCP)

As mensagens diretas entre peers e as transferências de arquivos ocorrem sobre conexões TCP. O formato base de uma mensagem de chat é um JSON codificado em UTF-8, seguido de uma quebra de linha (`\n`) para delimitar o fim do pacote.

```json
{
  "type": "chat_message",
  "sender_name": "peer_remetente",
  "content": "Olá, tudo bem?"
}