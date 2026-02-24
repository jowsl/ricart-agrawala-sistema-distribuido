#!/bin/bash

NODE_ID=$1

echo " [BOOT] Iniciando Middleware do Nó $NODE_ID "
#liga os clusters
python3 cluster_sync.py $NODE_ID &

#agurda 15seg
echo " [AGUARDANDO] 15 segundos para sincronia do cluster... "
sleep 15

echo " [START] Iniciando Cliente do Nó $NODE_ID "
#inicia clientes
python3 cliente.py $NODE_ID