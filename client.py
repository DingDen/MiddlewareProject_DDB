import socket
import json
import time

DISCOVERY_PORT = 50000
TIMEOUT = 3

# ===================== DISCOVERY =====================
def discover_nodes():
    with open("nodes.json") as f:
        nodes = json.load(f)
    return nodes

# ===================== SEND QUERY =====================
def send_query(node, sql):
    """
    Envia uma query SQL para um nó específico
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((node["ip"], node["port"]))
            s.sendall(json.dumps({
                "type": "CLIENT_QUERY",
                "sql": sql
            }).encode())
            response = json.loads(s.recv(4096).decode())
            return response
    except Exception as e:
        return {"result": f"ERRO: {e}"}

# ===================== MAIN =====================
if __name__ == "__main__":
    print("[CLIENT] Descobrindo nós na rede...")
    nodes = discover_nodes()

    if not nodes:
        print("Nenhum nó encontrado.")
        exit()

    # Seleciona o coordenador pelo maior ID
    coordinator = max(nodes, key=lambda n: n["id"])
    print(f"[CLIENT] Coordenador encontrado: Nó {coordinator['id']} ({coordinator['ip']}:{coordinator['port']})")

    while True:
        sql = input("\nSQL> ")

        if sql.lower() in ["exit", "quit"]:
            break

        response = send_query(coordinator, sql)
        print("\nResultado:")
        print(response.get("result"))
        print(f"(Executado no nó: {response.get('node')})")