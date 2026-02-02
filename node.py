import socket
import threading
import json
import time
import hashlib
import mysql.connector # lib externa

# ===================== CONFIG =====================
with open("config.json") as f:
    CONFIG = json.load(f)

NODE_ID = CONFIG["node_id"]
PORT = CONFIG["port"]

DISCOVERY_PORT = 50000
DISCOVERY_INTERVAL = 3
HEARTBEAT_INTERVAL = 3
HEARTBEAT_TIMEOUT = 6

# ===================== DB =====================
db = mysql.connector.connect(**CONFIG["mysql"])
db.autocommit = False

# ===================== STATE =====================
nodes = {}                 # node_id -> {id, ip, port}
is_coordinator = False
coordinator_id = None
last_heartbeat = {}

# ===================== IP AUTO =====================
def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    finally:
        s.close()

NODE_IP = get_local_ip()

# ===================== UTILS =====================
def checksum(data):
    return hashlib.sha256(data.encode()).hexdigest()

def send_message(node, msg, wait=False):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(2)
            s.connect((node["ip"], node["port"]))
            s.sendall(json.dumps(msg).encode())
            if wait:
                return json.loads(s.recv(4096).decode())
    except:
        return None

# ===================== DISCOVERY =====================
def broadcast_discovery():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    msg = json.dumps({
        "type": "DISCOVERY",
        "id": NODE_ID,
        "ip": NODE_IP,
        "port": PORT
    })

    sock.sendto(msg.encode(), ('<broadcast>', DISCOVERY_PORT))
    sock.close()

def listen_discovery():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('', DISCOVERY_PORT))

    while True:
        data, _ = sock.recvfrom(1024)
        msg = json.loads(data.decode())

        if msg["id"] != NODE_ID:
            if msg["id"] not in nodes:
                print(f"[DISCOVERY] Nó {msg['id']} em {msg['ip']}")
            nodes[msg["id"]] = msg

def discovery_loop():
    while True:
        broadcast_discovery()
        time.sleep(DISCOVERY_INTERVAL)

# ===================== HEARTBEAT =====================
def heartbeat_sender():
    while True:
        for n in nodes.values():
            if n["id"] != NODE_ID:
                send_message(n, {
                    "type": "HEARTBEAT",
                    "from": NODE_ID
                })
        time.sleep(HEARTBEAT_INTERVAL)

def heartbeat_monitor():
    global coordinator_id, is_coordinator

    while True:
        now = time.time()
        for nid in list(last_heartbeat.keys()):
            if now - last_heartbeat[nid] > HEARTBEAT_TIMEOUT:
                print(f"[!] Nó {nid} caiu")
                last_heartbeat.pop(nid)
                nodes.pop(nid, None)

                if nid == coordinator_id:
                    start_election()
        time.sleep(2)

# ===================== ELECTION (BULLY) =====================
def start_election():
    global is_coordinator, coordinator_id

    print("[ELECTION] Iniciando eleição")
    higher_exists = False

    for n in nodes.values():
        if n["id"] > NODE_ID:
            resp = send_message(n, {
                "type": "ELECTION",
                "from": NODE_ID
            }, wait=True)
            if resp:
                higher_exists = True

    if not higher_exists:
        is_coordinator = True
        coordinator_id = NODE_ID
        announce_coordinator()

def announce_coordinator():
    for n in nodes.values():
        send_message(n, {
            "type": "COORDINATOR",
            "id": NODE_ID
        })
    print(f"[COORDINATOR] Sou o novo coordenador ({NODE_ID})")

# ===================== QUERY =====================
def execute_query(sql):
    cur = db.cursor()
    cur.execute(sql)
    if sql.lower().startswith("select"):
        return cur.fetchall()
    return "OK"

def replicate(sql):
    cs = checksum(sql)
    for n in nodes.values():
        if n["id"] != NODE_ID:
            send_message(n, {
                "type": "REPLICA",
                "sql": sql,
                "checksum": cs
            })

# ===================== SERVER =====================
def handle_client(conn):
    global is_coordinator, coordinator_id

    try:
        data = json.loads(conn.recv(4096).decode())
    except:
        return

    if data["type"] == "DISCOVERY":
        conn.sendall(json.dumps({
            "id": NODE_ID,
            "ip": NODE_IP,
            "port": PORT
        }).encode())
        return

    if data["type"] == "HEARTBEAT":
        last_heartbeat[data["from"]] = time.time()
        return

    if data["type"] == "ELECTION":
        conn.sendall(json.dumps({"ok": True}).encode())
        start_election()
        return

    if data["type"] == "COORDINATOR":
        coordinator_id = data["id"]
        is_coordinator = (coordinator_id == NODE_ID)
        print(f"[INFO] Coordenador agora é {coordinator_id}")
        return

    if data["type"] == "REPLICA":
        if checksum(data["sql"]) == data["checksum"]:
            execute_query(data["sql"])
            db.commit()
        return

    if data["type"] == "CLIENT_QUERY":
        sql = data["sql"]
        try:
            if is_coordinator and not sql.lower().startswith("select"):
                execute_query(sql)
                replicate(sql)
                db.commit()
                conn.sendall(json.dumps({
                    "result": "COMMIT",
                    "node": NODE_ID
                }).encode())
            else:
                res = execute_query(sql)
                conn.sendall(json.dumps({
                    "result": res,
                    "node": NODE_ID
                }).encode())
        except:
            db.rollback()

# ===================== TCP SERVER =====================
def server():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((NODE_IP, PORT))
    s.listen()
    print(f"[START] Nó {NODE_ID} ativo em {NODE_IP}:{PORT}")

    while True:
        conn, _ = s.accept()
        threading.Thread(target=handle_client, args=(conn,), daemon=True).start()

# ===================== START =====================
if __name__ == "__main__":
    nodes[NODE_ID] = {
        "id": NODE_ID,
        "ip": NODE_IP,
        "port": PORT
    }

    threading.Thread(target=server, daemon=True).start()
    threading.Thread(target=listen_discovery, daemon=True).start()
    threading.Thread(target=discovery_loop, daemon=True).start()
    threading.Thread(target=heartbeat_sender, daemon=True).start()
    threading.Thread(target=heartbeat_monitor, daemon=True).start()

    time.sleep(5)
    start_election()

    print("[✓] DDB iniciado com sucesso")

    while True:
        time.sleep(1)