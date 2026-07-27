from faker import Faker
import csv
import random

fake = Faker("pt_BR")

def gerar_clientes(n=2000):
    clientes = []
    for i in range(n):
        clientes.append({
            "cliente_id": i + 1,
            "nome": fake.name(),
            "cpf": fake.cpf(),
            "email": fake.email(),
            "renda_mensal": round(random.uniform(1500, 25000), 2),
            "cidade": fake.city(),
            "data_cadastro": fake.date_between(start_date="-3y", end_date="today"),
        })
    return clientes

def gerar_transacoes(clientes, n=20000):
    transacoes = []
    for i in range(n):
        cliente = random.choice(clientes)
        transacoes.append({
            "transacao_id": i + 1,
            "cliente_id": cliente["cliente_id"],
            "valor": round(random.uniform(10, 5000), 2),
            "tipo": random.choice(["PIX", "TED", "COMPRA_CARTAO", "BOLETO"]),
            "data": fake.date_time_between(start_date="-1y", end_date="now"),
        })
    return transacoes

if __name__ == "__main__":
    clientes = gerar_clientes()
    transacoes = gerar_transacoes(clientes)

    with open("clientes.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=clientes[0].keys())
        writer.writeheader()
        writer.writerows(clientes)

    with open("transacoes.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=transacoes[0].keys())
        writer.writeheader()
        writer.writerows(transacoes)

    print(f"Gerados {len(clientes)} clientes e {len(transacoes)} transações.")