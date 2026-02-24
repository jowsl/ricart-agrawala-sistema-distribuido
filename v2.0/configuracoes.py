

# cada nó vai ter um "host" que será o nome do seu container no docker
NOS = [
    {"id": 1, "ip": "node1", "p2p_port": 5000, "client_port": 6000},
    {"id": 2, "ip": "node2", "p2p_port": 5000, "client_port": 6000},
    {"id": 3, "ip": "node3", "p2p_port": 5000, "client_port": 6000},
    {"id": 4, "ip": "node4",  "p2p_port": 5000, "client_port": 6000},
    {"id": 5, "ip": "node5",  "p2p_port": 5000, "client_port": 6000}
]

#dados do servidor central
RECURSO_HOST = "recurso"
RECURSO_PORT = 8080