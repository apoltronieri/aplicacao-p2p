# Análise da Comunicação com Wireshark

## Objetivo

Utilizar o Wireshark para observar a comunicação entre os peers da aplicação P2P e verificar o funcionamento dos protocolos UDP e TCP implementados no projeto.

## Descoberta de Peers (UDP)

Ao iniciar um peer, são enviados anúncios periódicos utilizando broadcast UDP.

Cada pacote contém informações como:

- Identificador do peer;
- Nome do peer;
- Porta TCP utilizada para comunicação.

### O que observar

- Pacotes UDP de broadcast;
- Endereço de origem e destino;
- Conteúdo JSON contendo as informações do peer.

Filtro sugerido:

```
udp
```

## Envio de Mensagens (TCP)

Quando o comando

```
/msg <peer> <mensagem>
```

é executado, uma conexão TCP é criada entre os peers, a mensagem é enviada em formato JSON

### O que observar

- Handshake TCP (SYN, SYN-ACK e ACK);
- Segmento contendo a mensagem JSON;
- Encerramento da conexão (FIN/ACK).

Filtro sugerido:

```
tcp
```
ou
```
tcp.port == <porta TCP do peer>
```

## Transferência de Arquivos (TCP)

Quando o comando

```
/file <peer> <caminho_arquivo>
```

é executado, o emissor envia:

1. Um cabeçalho JSON contendo:
   - tipo da mensagem;
   - nome do arquivo;
   - tamanho do arquivo;

2. Os bytes do arquivo.

### O que observar

- Cabeçalho JSON da transferência;
- Segmentação dos dados em múltiplos pacotes TCP;
- Recebimento completo do arquivo pelo peer destino.

## Resultado Esperado

Durante a captura é possível verificar:

- descoberta automática dos peers via UDP;
- criação de conexões TCP para troca de mensagens;
- envio dos arquivos utilizando TCP;
- encerramento correto das conexões após a comunicação.