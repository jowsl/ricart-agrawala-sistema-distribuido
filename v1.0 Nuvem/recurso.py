import socket
import datetime

HOST = '0.0.0.0'
PORT = 8080

print(f"RECURSO ONLINE EM {PORT} ---")

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    
    while True:
        conn, addr = s.accept()
        with conn:
            data = conn.recv(1024)
            if not data: break
            
            mensagem = data.decode()
            
            #pega o timestramp
            timestamp = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
            
            log_entry = f"[{timestamp}] {mensagem.strip()}"
            print(f"Escrevendo: {log_entry}")
            
            # Escreve no disco
            with open("RECURSO.txt", "a") as f:
                f.write(log_entry + "\n")
                f.flush()