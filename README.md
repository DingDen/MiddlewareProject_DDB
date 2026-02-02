# Banco de Dados Distribuído com Middleware em Python

## 1. Visão Geral

Este projeto implementa um **Banco de Dados Distribuído (DDB)** baseado no SGBD **MySQL**, utilizando um **middleware desenvolvido em Python**.  
O sistema opera em **múltiplas máquinas independentes**, comunicando-se por **sockets**, seguindo o modelo de um **DDB homogêneo e autônomo**.

Cada nó executa:
- Uma instância do MySQL
- Um middleware responsável pela comunicação, coordenação e replicação

O acesso ao banco distribuído é feito por uma aplicação cliente (`client.py`) com interface simples via terminal.

---

## 2. Arquitetura do Sistema

O sistema é composto por:

- **Nós do DDB (`node.py`)**
  - Executam queries localmente
  - Replicam alterações para os demais nós
  - Monitoram falhas
  - Participam da eleição de coordenador

- **Cliente (`client.py`)**
  - Descobre automaticamente os nós disponíveis
  - Envia queries SQL
  - Exibe o resultado da query e o nó executor

---

## 3. Tecnologias Utilizadas

- Python 3
- MySQL
- Sockets TCP e UDP
- Threads
- JSON
- Hash SHA-256 (checksum)

---

## 4. Modelo de Comunicação

### 4.1 Descoberta de Nós
- Realizada via **UDP Broadcast**
- Não exige configuração manual de IP
- Cada nó anuncia sua presença periodicamente

### 4.2 Comunicação entre Nós
- Realizada via **TCP**
- Usada para replicação, heartbeat, eleição e execução de queries

**Tipos de comunicação utilizados:**
- Broadcast → descoberta
- Unicast → controle e replicação

---

## 5. Configuração dos Nós (`config.json`)

Cada máquina possui um arquivo `config.json`.  
A **única diferença entre eles é o `node_id`**.

### Exemplo:

```json
{
  "node_id": 1,
  "port": 6000,
  "mysql": {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "ddb"
  }
}
```
---

## 6. Execução do sistema

### 6.1 Inicialização dos nós

Em cada máquina:
```bash
python node.py
```

### 6.2 Execução do cliente

Em qualquer máquina:
```bash
python client.py
```

Exemplo:
```sql
CREATE TABLE teste (id INT);
INSERT INTO teste VALUES (1);
SELECT * FROM teste;
```

Saída esperada:
```text
Resultado:
[(1,)]

Executado no nó: 2
```
