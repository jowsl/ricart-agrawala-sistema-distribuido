import socket
import threading
import time
import json
import sys
from configuracoes import NOS

#id do nó
if len(sys.argv) < 2:
    print("ERRO: Informe o ID do nó. Ex: python3 node_final.py 1")
    sys.exit(1)

MEU_ID = int(sys.argv[1])

#pega os dados do configurações
try:
    meus_dados = next(n for n in NOS if n["id"] == MEU_ID) #
    MINHA_PORTA_REDE = meus_dados['p2p_port']
    MINHA_PORTA_LOCAL = meus_dados['client_port']
except StopIteration:
    print("ID não encontrado no arquivo configuracoes.py")
    sys.exit(1)

#variaveis globais
relogio_lamport = 0
estado = "LIBERADO" #pode ser: LIBERADO, QUERENDO, OCUPADO
ok_recebidos = 0
fila_de_espera = []
ts_meu_pedido = 0
lock = threading.Lock() #criamos um mutex pra evitar condição de corrida entre cliente/cluster_sync que podem alterar as 
                        #variáveis globais. Evita que as funções ouvir_cliente e tratar_rede alterem as variaveis 
                        # estado e relogio_lamport ao mesmo tempo 

#comunicações entre os nós
def enviar_msg_p2p(id_destino, msg):
    #IP e Porta do destino no config
    destino = next(n for n in NOS if n['id'] == id_destino) #destino vai ter os dados do id solicitado (dados: ip e porta p2p)
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: #AF_INET significa que vamos usar IPV4 e SOCK_STREAM padrão é o tcp
                                                                     # vamos usar tcp pra garantir a entrega, diferente do udp
            s.settimeout(2) #timeout para não travar esperando resposta
            s.connect((destino['ip'], destino['p2p_port'])) #faz o handshake do tcp
            s.sendall(json.dumps(msg).encode()) #evia tudo como bytes com encode()
            #se for enviado ou se cair no excepetion, o "with" faz o s.close() automatico
    except Exception as e:
        # se um nó tiver offline n fazemos nada, mas em um sistema real teria tolerancia a falhas.
        pass #pass evita que crash caso tenha erro.

#ricart-agrawala
#essa unfção escuta os nos e decide quem espera/passa
def tratar_rede(conn):
    global relogio_lamport, estado, ok_recebidos, fila_de_espera
    try:
        #recebe dados
        data = conn.recv(1024).decode()
        if not data: return
        msg = json.loads(data)
        
        with lock:
            #sincronização do relogio, regra de lamport.
            relogio_lamport = max(relogio_lamport, msg['ts']) + 1

            if msg['tipo'] == "REQUEST":
                #Lógica Ricart-Agrawala
                # ocupado = esta sendo utilizado
                # querendo = verifica quem tem o menor timesramp, em caso de empate desempata por id             
                tem_prioridade = (estado == "OCUPADO") or \
                                      (estado == "QUERENDO" and (ts_meu_pedido < msg['ts'] or \
                                      (ts_meu_pedido == msg['ts'] and MEU_ID < msg['id'])))
                
                if tem_prioridade: #se for True
                    print(f"[LOG] REQ de {msg['id']} postergado") 
                    fila_de_espera.append(msg['id']) #enfileira
                                                     #se eu enfileirar, o request n tem meu "OK" então n entra na critica.
                else: # sem tem prioridade é False então o dado que chegou tem a prioridade
                    #Enviar OK
                    #função enviar com o id e a msg/discinario com o ok e dados
                    #usando outra thread pra destravar a função de tratar_rede
                    threading.Thread(target=enviar_msg_p2p, args=(msg['id'], {"tipo": "OK", "id": MEU_ID, "ts": relogio_lamport})).start()

            elif msg['tipo'] == "OK": #se for do tipo ok, incrementa o contator.
                ok_recebidos += 1 #ouvir_cliente verifica ao necessário, se chegar, entra na sessão crítica

    except Exception as e:
        print(f"Erro rede: {e}")
    finally:
        conn.close()

#CLIENTE
def ouvir_cliente():
    global estado, ts_meu_pedido, ok_recebidos, relogio_lamport, fila_de_espera
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('0.0.0.0', MINHA_PORTA_LOCAL)) #porta de um cliente local vai ser 6001
        s.listen()
        print(f" Nó {MEU_ID} ouvindo Cliente na porta {MINHA_PORTA_LOCAL} ")
        print(f" Nó {MEU_ID} ouvindo P2P na porta {MINHA_PORTA_REDE} ")
        
        while True:
            conn, _ = s.accept()
            try:
                cmd = conn.recv(1024).decode()
                
                #Cliente manda "ACQUIRE" -- "Quero acessar"
                if "ACQUIRE" in cmd:
                    print("\n> Cliente pediu acesso! Iniciando protocolo")
                    
                    with lock: # garante que so ouvir_client esteja alterandoa gora
                        estado = "QUERENDO"
                        relogio_lamport += 1
                        ts_meu_pedido = relogio_lamport
                        ok_recebidos = 0
                    
                    #REQUEST para todos os outros nos/clustersync -- "faz o pedido"
                    msg_req = {"tipo": "REQUEST", "id": MEU_ID, "ts": ts_meu_pedido}
                    for no in NOS: 
                        if no['id'] != MEU_ID:
                            #vazemos com outra Thread para destravar a função
                            threading.Thread(target=enviar_msg_p2p, args=(no['id'], msg_req)).start()
                    
                    #verifica se já temos todos os oks
                    total_necessario = len(NOS) - 1 #ja temos o ok local precisamos de +4
                    while ok_recebidos < total_necessario:
                        time.sleep(0.05)
                    
                    #entrou na seção crítica
                    with lock: estado = "OCUPADO"
                    print(">>> CONSEGUI ACESSO! (Todos OKs recebidos)")
                    
                    # avisa o cliente que ele por acessar o recurso.
                    conn.sendall(b"COMMITTED")
                    
                    #sleep por 3 segundos so por segurança, enquanto cliente escreve no recurso.txt
                    time.sleep(3) 
                    
                    #fim da seção crítica
                    print("<<< LIBERANDO RECURSO >>>")
                    with lock:
                        estado = "LIBERADO" #altera o status e começa liberar a fila que n respondemos aqui
                        # Responde OK para quem estava na fila, thread para n ficar esperando em deadlock
                        for id_dest in fila_de_espera:
                            print(f"[LOG] Liberando pendente: {id_dest}")
                            threading.Thread(target=enviar_msg_p2p, args=(id_dest, {"tipo": "OK", "id": MEU_ID, "ts": relogio_lamport})).start()
                        fila_de_espera = [] #limpa a fila depois de liberar os "ok"

            except Exception as e:
                print(f"Erro cliente: {e}")
            finally:
                conn.close()

# main
def iniciar():
    
    # t_rede = threading.Thread(target=ouvir_cliente, daemon=True) 
    
    #thread para ouvir os outros cluster_sync
    threading.Thread(target=ouvir_cliente, daemon=True).start()
    
    #fica ouvindo o cliente, chama o tratar_rede em outra Thread
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('0.0.0.0', MINHA_PORTA_REDE))
        s.listen()
        while True:
            conn, addr = s.accept()
            threading.Thread(target=tratar_rede, args=(conn,)).start()

if __name__ == "__main__":
    iniciar()