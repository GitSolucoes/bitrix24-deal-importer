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
    ],
    "filter[>=DATE_CREATE]": "2024-01-01",
    "start": 0,
}

PAGE_DELAY = 30

def get_conn():
    print("🔌 Tentando conectar ao banco de dados...")
    conn = psycopg2.connect(**DB_PARAMS)
    print("✅ Conectado ao banco com sucesso.")
    return conn

def format_date(date_str):
    if not date_str:
        return None
    dt = parser.isoparse(date_str)
    dt_naive = dt.replace(tzinfo=None)
    return dt_naive.strftime("%d/%m/%Y")

def upsert_deal(conn, deal):
    try:
        with conn.cursor() as cur:
            print(f"\n📝 Inserindo/atualizando deal ID: {deal.get('ID')}")

            dados = (
                deal.get("ID"), deal.get("TITLE"), deal.get("STAGE_ID"), deal.get("CATEGORY_ID"),
                deal.get("UF_CRM_1700661314351"), deal.get("CONTACT_ID"), deal.get("DATE_CREATE"),
                deal.get("UF_CRM_1698698407472"), deal.get("UF_CRM_1698698858832"),
                deal.get("UF_CRM_1697653896576"), deal.get("UF_CRM_1697762313423"),
                deal.get("UF_CRM_1697763267151"), deal.get("UF_CRM_1697764091406"),
                deal.get("UF_CRM_1697807340141"), deal.get("UF_CRM_1697807353336"),
                deal.get("UF_CRM_1697807372536"), deal.get("UF_CRM_1697808018193"),
                deal.get("UF_CRM_1698688252221"), deal.get("UF_CRM_1698761151613"),
                deal.get("UF_CRM_1699452141037"), deal.get("UF_CRM_1700661287551"),
                deal.get("UF_CRM_1731588487"), deal.get("UF_CRM_1700661252544"), deal.get("UF_CRM_1731589190")
            )

            print("📌 Dados que serão inseridos no banco:")
            print(dados)

            cur.execute("""
                INSERT INTO deals (
                    id, title, stage_id, category_id, uf_crm_cep, uf_crm_contato, date_create,
                    contato01, contato02, ordem_de_servico, nome_do_cliente, nome_da_mae,
                    data_de_vencimento, email, cpf, rg, referencia, rua, data_de_instalacao,
                    quais_operadoras_tem_viabilidade,
                    uf_crm_bairro, uf_crm_cidade, uf_crm_numero, uf_crm_uf
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            """, dados)

            print(f"✅ Deal {deal.get('ID')} inserido/atualizado com sucesso.")

    except Exception as e:
        print(f"❌ Erro ao inserir/atualizar deal {deal.get('ID')}: {e}")

def fetch_deals(url):
    print(f"🌐 Requisição para {url}")
    try:
        response = requests.get(url, params=PARAMS)
        print(f"📶 Status HTTP: {response.status_code}")
        response.raise_for_status()
        json_data = response.json()
        result = json_data.get("result", [])
        print(f"📥 Recebidos {len(result)} deals da URL: {url}")
        return result
    except Exception as e:
        print(f"❌ Erro ao buscar deals de {url}: {e}")
        return []

def main():
    print("\n🚀 Script iniciado em:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    conn = get_conn()

    try:
        for url in WEBHOOKS:
            print(f"\n🔁 Processando webhook: {url}")
            deals = fetch_deals(url)
            print(f"🔍 {len(deals)} deals obtidos")

            for deal in deals:
                print(f"➡️ Processando deal ID: {deal.get('ID')}")
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
