import socket
import json
import time

DISCOVERY_PORT = 50000
TIMEOUT = 3

# ===================== DISCOVERY =====================
def discover_nodes():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.settimeout(TIMEOUT)

    message = json.dumps({
        "type": "DISCOVERY_REQUEST"
    }).encode()

    sock.sendto(message, ('<broadcast>', DISCOVERY_PORT))

    nodes = []

    start = time.time()
    while True:
        try:
            data, _ = sock.recvfrom(1024)
            msg = json.loads(data.decode())
            nodes.append(msg)
        except socket.timeout:
            break
        if time.time() - start > TIMEOUT:
            break

    sock.close()
    return nodes

# ===================== SEND QUERY =====================
def send_query(node, sql):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((node["ip"], node["port"]))
        s.sendall(json.dumps({
            "type": "CLIENT_QUERY",
            "sql": sql
        }).encode())
        return json.loads(s.recv(4096).decode())

# ===================== MAIN =====================
if __name__ == "__main__":
    print("[CLIENT] Descobrindo nós...")
    nodes = discover_nodes()

    if not nodes:
        print("Nenhum nó encontrado.")
        exit()

    coordinator = max(nodes, key=lambda n: n["id"])
    print(f"[CLIENT] Coordenador: Nó {coordinator['id']} ({coordinator['ip']})")

    while True:
        sql = input("\nSQL> ")

        if sql.lower() in ["exit", "quit"]:
            break

        try:
            response = send_query(coordinator, sql)
            print("\nResultado:")
            print(response["result"])
            print(f"\nExecutado no nó: {response['node']}")
        except Exception as e:
            print("Erro ao executar query:", e)