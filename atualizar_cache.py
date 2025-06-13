# utils.py
import psycopg2
from dateutil import parser
import os
import requests
from dotenv import load_dotenv

load_dotenv()

DB_PARAMS = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}

def get_conn():
    return psycopg2.connect(**DB_PARAMS)

def format_date(date_str):
    if not date_str:
        return None
    dt = parser.isoparse(date_str)
    dt_naive = dt.replace(tzinfo=None)
    return dt_naive.strftime("%d/%m/%Y")

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
