import psycopg2
import requests
import time
import os
from datetime import datetime
from dateutil import parser
from dotenv import load_dotenv

load_dotenv()

# Validação de variáveis de ambiente
required_env_vars = ["DB_NAME", "DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT"]
for var in required_env_vars:
    if not os.getenv(var):
        raise ValueError(f"Variável de ambiente {var} não encontrada no .env")

DB_PARAMS = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}

WEBHOOKS = [
    "https://marketingsolucoes.bitrix24.com.br/rest/5332/8zyo7yj1ry4k59b5/crm.deal.list",
    "https://marketingsolucoes.bitrix24.com.br/rest/5332/y5q6wd4evy5o57ze/crm.deal.list",
]

WEBHOOK_CATEGORIES = [
    "https://marketingsolucoes.bitrix24.com.br/rest/5332/8zyo7yj1ry4k59b5/crm.dealcategory.list",
    "https://marketingsolucoes.bitrix24.com.br/rest/5332/y5q6wd4evy5o57ze/crm.dealcategory.list",
]

WEBHOOK_STAGES = [
    "https://marketingsolucoes.bitrix24.com.br/rest/5332/8zyo7yj1ry4k59b5/crm.dealcategory.stage.list",
    "https://marketingsolucoes.bitrix24.com.br/rest/5332/y5q6wd4evy5o57ze/crm.dealcategory.stage.list",
]

WEBHOOK_FIELDS = [
    "https://marketingsolucoes.bitrix24.com.br/rest/5332/8zyo7yj1ry4k59b5/crm.deal.fields",
    "https://marketingsolucoes.bitrix24.com.br/rest/5332/y5q6wd4evy5o57ze/crm.deal.fields",
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
    "filter[>=DATE_CREATE]": "2021-01-01",
    "start": 0,
}

MAX_RETRIES = 20
RETRY_DELAY = 30
REQUEST_DELAY = 2
PAGE_DELAY = 30
LIMITE_REGISTROS_TURBO = 20000

def get_conn():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except psycopg2.Error as e:
        print(f"❌ Erro ao conectar ao banco de dados: {e.pgerror}")
        raise

def format_date(date_str):
    if not date_str:
        return None
    try:
        dt = parser.isoparse(date_str)
        dt_naive = dt.replace(tzinfo=None)
        return dt_naive.strftime("%d/%m/%Y")
    except ValueError as e:
        print(f"❌ Erro ao formatar data '{date_str}': {e}")
        return None

def upsert_deal(conn, deal):
    try:
        with conn.cursor() as cur:
            print(f"\n📝 Inserindo/atualizando deal ID: {deal.get('ID')}")
            data = (
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
            """, data)
            print(f"✅ Deal {deal.get('ID')} inserido/atualizado com sucesso.")
    except psycopg2.Error as e:
        print(f"❌ Erro SQL ao inserir/atualizar deal {deal.get('ID')}: {e.pgerror}")
        raise
    except Exception as e:
        print(f"❌ Erro geral ao inserir/atualizar deal {deal.get('ID')}: {e}")
        raise

def get_operadora_map(webhook):
    try:
        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.get(webhook, timeout=15)
                resp.raise_for_status()
                data = resp.json()
                items = data.get("result", {}).get("UF_CRM_1699452141037", {}).get("items", [])
                operadora_map = {item["ID"]: item["VALUE"] for item in items}
                if not operadora_map:
                    print(f"⚠ Nenhum mapeamento de operadoras retornado por {webhook}")
                return operadora_map
            except requests.RequestException as e:
                print(f"❌ Erro {attempt+1}/{MAX_RETRIES} ao buscar operadoras em {webhook}: {e}")
                time.sleep(RETRY_DELAY * (2 ** attempt))
        print(f"❌ Falha ao buscar operadoras após {MAX_RETRIES} tentativas.")
        return {}
    except Exception as e:
        print(f"❌ Erro geral ao buscar operadoras: {e}")
        return {}
