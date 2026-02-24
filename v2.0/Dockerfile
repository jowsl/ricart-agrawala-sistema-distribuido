FROM python:3.9-slim

#define pasta default como a /app
WORKDIR /app

#tudo da sua pasta atual para dentro do container
COPY . /app

#Dá permissão de execução para o script
RUN chmod +x run.sh

#Define o script como padrão ao ligar a máquina
ENTRYPOINT ["./run.sh"]