import socket
import sys
import os
import threading 
 
if len(sys.argv) < 2:
    print("Informe o ID do Store. Ex: python3 cluster_store.py 1")
    sys.exit(1)

MEU_ID = int(sys.argv[1])
PORTA = 8080
ARQ_RECURSO = f"recurso_{MEU_ID}.txt"

#parar usar o lock e evitar que mais de um sync tente alterar o recurso aomesmo tempo
lock_arquivo = threading.Lock()

def get_versao_atual():
    #vamos usar o numero de linhas do arquivo para ser a versão
    if not os.path.exists(ARQ_RECURSO):
        return 0
    with open(ARQ_RECURSO, "r") as f:
        return sum(1 for _ in f)

def tratar_requisicoes(conn, addr):
    with conn:
        try:
            cmd = conn.recv(4096).decode()
            if not cmd:
                return
            
            #ping reposta
            if cmd == "PING":
                conn.sendall(b"PONG")

            #se for para verificar a versao
            elif cmd == "GET_VERSAO":
                with lock_arquivo:
                    versao = get_versao_atual()
                conn.sendall(str(versao).encode())

            #se for escrita comum
            elif cmd.startswith("ESCRITA|"):
                dado = cmd.split("|", 1)[1]
                
                with lock_arquivo:
                    with open(ARQ_RECURSO, "a") as f:
                        f.write(dado + "\n")
                    nova_versao = get_versao_atual()
                    print(f"[STORE {MEU_ID}] escrita concluida versão: {nova_versao} | {dado}")
                    conn.sendall(f"SUCESSO|{nova_versao}".encode())

            #clustersync pede as linhas perdidas
            elif cmd.startswith("GET_PERDIDAS|"):
                versao_anterior = int(cmd.split("|")[1])
                with lock_arquivo:
                    if not os.path.exists(ARQ_RECURSO):
                        linhas = []
                    else:
                        with open(ARQ_RECURSO, "r") as f:
                            linhas = f.readlines()
                        
                #pega apenas a linhas a partir da versão antiga que o STORE caido parou
                linhas_perdidas = linhas[versao_anterior:] #tudo da versao do antigo pra frente
                todas_perdidas = "".join(linhas_perdidas) #junta em uma string só
                conn.sendall(todas_perdidas.encode())


            #clustersync coloca as linhas perdidas no STORE desatualizado
            elif cmd.startswith("UPDATE|"):
                dado_update = cmd.split("|", 1)[1]
                if dado_update: #so se existir algo pra update
                    with lock_arquivo:
                        with open(ARQ_RECURSO, "a") as f:
                            f.write(dado_update)
                        nova_versao = get_versao_atual()
                    print(f"[STORE {MEU_ID}] Update completo, versão: {nova_versao}")
                conn.sendall(b"OK")

        except Exception as e:
            pass #ignorar erros de rede

def iniciar_store():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('0.0.0.0', PORTA))
        s.listen()
        print(f"- Cluster Store {MEU_ID} rodando na porta {PORTA} -")

        while True:
            conn, addr = s.accept()
            threading.Thread(target=tratar_requisicoes, args=(conn, addr)).start()

if __name__ == "__main__":
    iniciar_store()