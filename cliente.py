import socket
import time
import sys
import random
from configuracoes import NOS

# recurso esta na vm2
IP_RECURSO = "10.0.0.19" 
PORTA_RECURSO = 8080

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
print(f"Alvo Remoto (Recurso): {IP_RECURSO}:{PORTA_RECURSO}")
print("---------------------------------")

aleatorio = random.randint(10, 50)

for i in range(aleatorio): 
    #Simula pensamento
    tempo_espera = random.randint(1, 5)
    print(f"\n[Tentativa {i+1}/{aleatorio}] Aguardando {tempo_espera}s.")
    time.sleep(tempo_espera)

    print(f"[ACQUIRE] Solicitando acesso ao Nó {meu_id}")

    try:
        #cliente fala com clustersync local
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(('127.0.0.1', PORTA_LOCAL_SYNC))
            s.sendall(b"ACQUIRE")
            
            #bloqueado ate receber permissão.
            resposta = s.recv(1024).decode()
            
            if "COMMITTED" in resposta:
                print("PERMISSÃO CONCEDIDA! Escrevendo no recurso")
                
                #Servidor de Arquivo
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_rec:
                        s_rec.settimeout(3) #timeout de segurança
                        s_rec.connect((IP_RECURSO, PORTA_RECURSO))
                        
                        msg = f"Cliente {meu_id} escreveu log {i+1} as {time.time()}"
                        s_rec.sendall(msg.encode())
                        print(f"[SUCESSO] Dado salvo na VM2.")
                except Exception as e:
                    print(f"[ERRO] Falha ao conectar no Recurso: {e}")
                
                print(" Acesso finalizado.")
                
    except ConnectionRefusedError:
        print(f"ERRO: Seu Nó Sync local (ID {meu_id}) não está rodando!")
        break