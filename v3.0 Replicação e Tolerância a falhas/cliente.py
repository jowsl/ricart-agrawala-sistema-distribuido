import socket
import time
import sys
import random
from configuracoes import NOS

#verificação do argv
if len(sys.argv) < 2:
    print("Informe qual o ID deste cliente ex: cliente.py 1")
    sys.exit(1)

meu_id = int(sys.argv[1])

#porta do no local
try:
    dados_no = next(no for no in NOS if no['id'] == meu_id)
    PORTA_LOCAL_SYNC = dados_no['client_port']
except StopIteration:
    print(f"ID {meu_id} não encontrado no configuracoes")
    sys.exit(1)

print(f"--- Cliente {meu_id} Iniciado ---")
print(f"Alvo Local: 127.0.0.1:{PORTA_LOCAL_SYNC}")
print("---------------------------------")

aleatorio = random.randint(10, 50)

for i in range(aleatorio): 
    #Simula pensamento
    tempo_espera = random.randint(1, 5)
    print(f"\n[Tentativa {i+1}/{aleatorio}] Aguardando {tempo_espera}s.")
    time.sleep(tempo_espera)

    msg_dado = f"Cliente {meu_id} escreveu log {i+1} as {time.time()}"
    middle_commando = f"ESCRITA|{msg_dado}"

    try:
        #cliente fala com clustersync local
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(('127.0.0.1', PORTA_LOCAL_SYNC))
            s.sendall(middle_commando.encode())
            
            #bloqueado ate receber permissão.
            resposta = s.recv(1024).decode()
            
            if "COMMITTED" in resposta:
                print("PERMISSÃO CONCEDIDA! Cluster_sync escreveu no recurso")
                
            else:
                print(f"[ERRO] Falha ao estabelecer conexão com Middleware/Recurso")
                
            print(" Acesso finalizado.")
                
    except ConnectionRefusedError:
        print(f"ERRO: Seu Nó Sync local (ID {meu_id}) não está rodando!")
        break