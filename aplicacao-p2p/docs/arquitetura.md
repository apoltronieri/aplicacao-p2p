## Fluxo de Mensagens e Concorrência

Para permitir que os usuários enviem e recebam mensagens simultaneamente sem travamentos na interface, o sistema utiliza a biblioteca `threading`. 

1. Thread Principal: Mantém o prompt de comando (`input()`) ativo.
2. Threads de Recebimento TCP: Para cada conexão estabelecida (Pessoa 1), uma thread dedicada (`TcpMessageReceiver`) aguarda novos pacotes.
3. Quando um pacote chega, a thread imprime a mensagem no terminal usando `\r` para evitar que a linha atual do `input` seja corrompida.