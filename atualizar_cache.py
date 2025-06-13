import psycopg2
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv

load_dotenv()

def get_conn():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )

def upsert_deal(conn, deal):
    with conn.cursor() as cur:
        sql = """
        INSERT INTO deals (id, nome, data_criacao, categoria, estagio, operadoras)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
          nome = EXCLUDED.nome,
          data_criacao = EXCLUDED.data_criacao,
          categoria = EXCLUDED.categoria,
          estagio = EXCLUDED.estagio,
          operadoras = EXCLUDED.operadoras;
        """
        cur.execute(sql, (
            deal.get("ID"),
            deal.get("TITLE"),
            deal.get("DATE_CREATE"),
            deal.get("CATEGORY_ID"),
            deal.get("STAGE_ID"),
            deal.get("UF_CRM_1699452141037")
        ))

def format_date(date_str):
    from datetime import datetime
    if not date_str:
        return None
    dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
    return dt.strftime("%d/%m/%Y")

