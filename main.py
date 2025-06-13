import psycopg2
import requests
import time
import os
from datetime import datetime
from dateutil import parser
from dotenv import load_dotenv

print("🔄 Carregando variáveis de ambiente...")
load_dotenv()

DB_PARAMS = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}
print("✅ Variáveis de ambiente carregadas:")
print(DB_PARAMS)

WEBHOOKS = [
    "https://marketingsolucoes.bitrix24.com.br/rest/5332/8zyo7yj1ry4k59b5/crm.deal.list",
    "https://marketingsolucoes.bitrix24.com.br/rest/5332/y5q6wd4evy5o57ze/crm.deal.list",
]

PARAMS = {
    "select[]": [
        "ID", "TITLE", "STAGE_ID", "CATEGORY_ID", "OPPORTUNITY", "CONTACT_ID", "BEGINDATE",
        "SOURCE_ID", "UF_CRM_1700661314351", "UF_CRM_1698698407472", "UF_CRM_1698698858832",
        "UF_CRM_1697653896576", "UF_CRM_1697762313423", "UF_CRM_1697763267151", "UF_CRM_1697764091406",
        "UF_CRM_1697807340141", "UF_CRM_1697807353336", "UF_CRM_1697807372536", "UF_CRM_1697808018193",
        "UF_CRM_1698688252221", "UF_CRM_1698761151613", "UF_CRM_1699452141037", "DATE_CREATE",
        "UF_CRM_1700661287551", "UF_CRM_1731588487", "UF_CRM_1700661252544", "UF_CRM_1731589190"
    ],
    "filter[>=DATE_CREATE]": "2025-01-01",
    "start": 0,
}

PAGE_DELAY = 10
_CACHE_TTL = 3600

_cache = {
    "categories": {"data": {}, "timestamp": 0},
    "stages": {},
}

def fetch_with_retry(url, params=None, retries=3, backoff_in_seconds=1):
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            print(f"❌ Erro na tentativa {attempt + 1} da url {url}: {e}")
            if attempt == retries - 1:
                raise
            time.sleep(backoff_in_seconds * (2 ** attempt))

def get_operadora_map():
    try:
        resp = requests.get(
            "https://marketingsolucoes.bitrix24.com.br/rest/5332/8zyo7yj1ry4k59b5/crm.deal.fields"
        )
        data = resp.json()
        items = data.get("result", {}).get("UF_CRM_1699452141037", {}).get("items", [])
        return {item["ID"]: item["VALUE"] for item in items}
    except Exception as e:
        print("❌ Erro ao buscar operadoras:", e)
        return {}

def get_conn():
    return psycopg2.connect(**DB_PARAMS)

def format_date(date_str):
    if not date_str:
        return None
    dt = parser.isoparse(date_str)
    dt_naive = dt.replace(tzinfo=None)
    return dt_naive.strftime("%d/%m/%Y")

def get_stages(cat_id):
    now = time.time()
    if cat_id in _cache["stages"]:
        if now - _cache["stages"][cat_id]["timestamp"] < _CACHE_TTL:
            return _cache["stages"][cat_id]["data"]

    url = "https://marketingsolucoes.bitrix24.com.br/rest/5332/8zyo7yj1ry4k59b5/crm.dealcategory.stage.list"
    params = {"id": cat_id, "start": 0}
    data = fetch_with_retry(url, params=params)
    stages = {}
    if "result" in data:
        for stage in data["result"]:
            stages[stage["STATUS_ID"]] = stage["NAME"]
    _cache["stages"][cat_id] = {"data": stages, "timestamp": now}
    return stages

def get_categories():
    now = time.time()
    if _cache["categories"]["data"] and now - _cache["categories"]["timestamp"] < _CACHE_TTL:
        return _cache["categories"]["data"]

    url = "https://marketingsolucoes.bitrix24.com.br/rest/5332/8zyo7yj1ry4k59b5/crm.dealcategory.list"
    data = fetch_with_retry(url)
    categories = {}
    if "result" in data:
        for cat in data["result"]:
            categories[cat["ID"]] = cat["NAME"]
    _cache["categories"]["data"] = categories
    _cache["categories"]["timestamp"] = now
    return categories

def upsert_deal(conn, deal):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO deals (
                id, title, stage_id, category_id, uf_crm_cep, uf_crm_contato, date_create,
                contato01, contato02, ordem_de_servico, nome_do_cliente, nome_da_mae,
                data_de_vencimento, email, cpf, rg, referencia, rua, data_de_instalacao,
                quais_operadoras_tem_viabilidade,
                uf_crm_bairro, uf_crm_cidade, uf_crm_numero, uf_crm_uf
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title,
                stage_id = EXCLUDED.stage_id,
                category_id = EXCLUDED.category_id,
                uf_crm_cep = EXCLUDED.uf_crm_cep,
                uf_crm_contato = EXCLUDED.uf_crm_contato,
                date_create = EXCLUDED.date_create,
                contato01 = EXCLUDED.contato01,
                contato02 = EXCLUDED.contato02,
                ordem_de_servico = EXCLUDED.ordem_de_servico,
                nome_do_cliente = EXCLUDED.nome_do_cliente,
                nome_da_mae = EXCLUDED.nome_da_mae,
                data_de_vencimento = EXCLUDED.data_de_vencimento,
                email = EXCLUDED.email,
                cpf = EXCLUDED.cpf,
                rg = EXCLUDED.rg,
                referencia = EXCLUDED.referencia,
                rua = EXCLUDED.rua,
                data_de_instalacao = EXCLUDED.data_de_instalacao,
                quais_operadoras_tem_viabilidade = EXCLUDED.quais_operadoras_tem_viabilidade,
                uf_crm_bairro = EXCLUDED.uf_crm_bairro,
                uf_crm_cidade = EXCLUDED.uf_crm_cidade,
                uf_crm_numero = EXCLUDED.uf_crm_numero,
                uf_crm_uf = EXCLUDED.uf_crm_uf;
            """,
            (
                deal.get("ID"),
                deal.get("TITLE"),
                deal.get("STAGE_ID"),
                deal.get("CATEGORY_ID"),
                deal.get("UF_CRM_1700661314351"),
                deal.get("CONTACT_ID"),
                deal.get("DATE_CREATE"),
                deal.get("UF_CRM_1698698407472"),
                deal.get("UF_CRM_1698698858832"),
                deal.get("UF_CRM_1697653896576"),
                deal.get("UF_CRM_1697762313423"),
                deal.get("UF_CRM_1697763267151"),
                deal.get("UF_CRM_1697764091406"),
                deal.get("UF_CRM_1697807340141"),
                deal.get("UF_CRM_1697807353336"),
                deal.get("UF_CRM_1697807372536"),
                deal.get("UF_CRM_1697808018193"),
                deal.get("UF_CRM_1698688252221"),
                deal.get("UF_CRM_1698761151613"),
                deal.get("UF_CRM_1699452141037"),
                deal.get("UF_CRM_1700661287551"),
                deal.get("UF_CRM_1731588487"),
                deal.get("UF_CRM_1700661252544"),
                deal.get("UF_CRM_1731589190"),
            ),
        )

def fetch_all_deals(url):
    start = 0
    all_deals = []

    while True:
        params = PARAMS.copy()
        params["start"] = start

        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        result = data.get("result", [])
        all_deals.extend(result)

        print(f"📥 Recebidos {len(result)} negócios com start={start}")

        if "next" in data:
            start = data["next"]
        else:
            break

    return all_deals

def main():
    print("\n🚀 Script iniciado em:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    conn = get_conn()

    try:
        print("🔄 Carregando operadoras e categorias...")
        operadora_map = get_operadora_map()
        categorias = get_categories()

        for url in WEBHOOKS:
            print(f"\n🔁 Processando webhook: {url}")
            deals = fetch_all_deals(url)
            print(f"🔍 {len(deals)} deals obtidos")

            for deal in deals:
                deal_id = deal.get('ID')
                print(f"➡️ Processando deal ID: {deal_id}")

                cat_id = deal.get("CATEGORY_ID")
                stage_id = deal.get("STAGE_ID")

                deal["CATEGORY_NAME"] = categorias.get(str(cat_id), "Desconhecida")
                stages = get_stages(cat_id)
                deal["STAGE_NAME"] = stages.get(stage_id, "Desconhecido")

                print(f"📂 Categoria: {deal['CATEGORY_NAME']} | 🪜 Estágio: {deal['STAGE_NAME']}")

                upsert_deal(conn, deal)

            print(f"⏱️ Aguardando {PAGE_DELAY} segundos antes da próxima requisição...")
            time.sleep(PAGE_DELAY)

        conn.commit()
        print("\n✅ Todos os dados foram processados e salvos com sucesso.")

    except Exception as e:
        print(f"❌ Erro durante execução principal: {e}")

    finally:
        conn.close()
        print("🔒 Conexão com banco encerrada.")

if __name__ == "__main__":
    main()

# 🔎 Teste com fake_deal após o main
print("🧪 Teste final: inserindo deal fake manualmente...")

conn = get_conn()

fake_deal = {
    "ID": 999999,
    "TITLE": "Deal Teste",
    "STAGE_ID": "NEW",
    "CATEGORY_ID": "0",
    "UF_CRM_1700661314351": "00000-000",
    "CONTACT_ID": "1",
    "DATE_CREATE": "2024-10-01T00:00:00+03:00",
    "UF_CRM_1698698407472": "Contato 01",
    "UF_CRM_1698698858832": "Contato 02",
    "UF_CRM_1697653896576": "1234",
    "UF_CRM_1697762313423": "João",
    "UF_CRM_1697763267151": "Maria",
    "UF_CRM_1697764091406": "2024-10-10",
    "UF_CRM_1697807340141": "email@email.com",
    "UF_CRM_1697807353336": "000.000.000-00",
    "UF_CRM_1697807372536": "123456",
    "UF_CRM_1697808018193": "Referência",
    "UF_CRM_1698688252221": "Rua Teste",
    "UF_CRM_1698761151613": "2024-11-11",
    "UF_CRM_1699452141037": "Vivo",
    "UF_CRM_1700661287551": "Bairro X",
    "UF_CRM_1731588487": "Cidade Y",
    "UF_CRM_1700661252544": "123",
    "UF_CRM_1731589190": "SP",
}

try:
    upsert_deal(conn, fake_deal)
    conn.commit()
    print("✅ Deal fake inserido com sucesso!")
except Exception as e:
    print("❌ Erro ao inserir deal fake:", e)
finally:
    conn.close()
    print("🔒 Conexão encerrada após teste.")
