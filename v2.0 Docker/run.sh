#!/bin/bash

NODE_ID=$1

echo " [BOOT] Iniciando Middleware do Nó $NODE_ID "
#liga os clusters
python3 -u cluster_sync.py $NODE_ID &

#agurda 15seg
echo " [AGUARDANDO] 10 segundos para sincronia do cluster... "
sleep 10

echo " [START] Iniciando Cliente do Nó $NODE_ID "
#inicia clientes
python3 -u cliente.py $NODE_ID

echo "[FINALIZADO] Cliente $NODE_ID não tem mais nenhuma requisição. Cluster_sync apenas respondendo OKs agora."
#o wait vai evitar que o container caia quando terminar de roda o script ja que o cluster_sync tem um while True.
wait