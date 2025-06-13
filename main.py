import psycopg2
import requests
import time
import os
from datetime import datetime
from dateutil import parser 
from dotenv import load_dotenv
import requests, time

load_dotenv()


WEBHOOK_CATEGORIES = "https://marketingsolucoes.bitrix24.com.br/rest/5332/8zyo7yj1ry4k59b5/crm.dealcategory.list"
WEBHOOK_STAGES = "https://marketingsolucoes.bitrix24.com.br/rest/5332/8zyo7yj1ry4k59b5/crm.dealstage.list"


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
    try:
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
                )
            )
    except Exception as e:
        print(f"❌ Erro ao inserir/atualizar deal {deal.get('ID')}: {e}")





def fazer_requisicao(url, params):
    while True:
        try:
            resp = requests.post(url, json=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            return data
        except requests.exceptions.HTTPError as e:
            if resp.status_code == 429:
                print("⏳ Limite de requisições atingido. Aguardando 60 segundos...")
                time.sleep(60)
            else:
                print(f"❌ Erro na requisição: {e}")
                return None
        except Exception as e:
            print(f"❌ Erro inesperado na requisição: {e}")
            return None

def get_categories():
    params = {"start": 0}
    categories = {}
    while True:
        data = fazer_requisicao(WEBHOOK_CATEGORIES, params)
        if not data:
            break
        for cat in data.get("result", []):
            categories[cat["ID"]] = cat["NAME"]
        if data.get("next"):
            params["start"] = data["next"]
        else:
            break
    return categories

def get_stages(category_id):
    params = {"id": category_id, "start": 0}
    stages = {}
    while True:
        data = fazer_requisicao(WEBHOOK_STAGES, params)
        if not data:
            print(f"🚫 Falha ao obter estágios para categoria {category_id}")
            break
        for stage in data.get("result", []):
            stages[stage["STATUS_ID"]] = stage["NAME"]
        if data.get("next"):
            params["start"] = data["next"]
        else:
            break
    return stages

def load_all_deals():
    print("🔁 Iniciando carga completa de negócios antigos...")
    all_deals = []
    start = 0
    url = "https://marketingsolucoes.bitrix24.com.br/rest/5332/8zyo7yj1ry4k59b5/crm.deal.list"

    while True:
        try:
            print(f"📨 Fazendo requisição com start={start}")
            response = requests.post(url, json={
                "start": start,
                "order": {"ID": "ASC"},
                "filter": {">=DATE_CREATE": "2024-02-09T00:00:00Z"},
                "select": ["*"]
            }, timeout=30)
            response.raise_for_status()
            data = response.json()
            result = data.get("result", [])
            if not result:
                break

            print("📥 IDs dos deals recebidos nesta página:", [deal.get("ID") for deal in result])
            all_deals.extend(result)

            # Avança o start para próxima página (paginacao correta)
            start = data.get("next")
            print(f"📦 Total acumulado: {len(all_deals)} negócios")

            if not start:
                break

            time.sleep(2)  # pausa para evitar 429

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print("⏳ Limite de requisições atingido. Aguardando 60 segundos...")
                time.sleep(60)
                continue
            else:
                print(f"❌ Erro durante paginação: {e}")
                break
        except Exception as e:
            print(f"❌ Erro durante paginação: {e}")
            break

    if not all_deals:
        print("⚠️ Nenhum negócio encontrado.")
        return

    categorias = get_categories()
    estagios = {cat_id: get_stages(cat_id) for cat_id in categorias}
    operadora_map = get_operadora_map()
    conn = get_conn()

    sucesso = 0
    ids_atualizados = []
    for deal in all_deals:
        try:
            print(f"🔍 Processando deal ID: {deal.get('ID')}")
            if "DATE_CREATE" in deal:
                deal["DATE_CREATE"] = format_date(deal["DATE_CREATE"])
            if "UF_CRM_1698761151613" in deal:
                deal["UF_CRM_1698761151613"] = format_date(deal["UF_CRM_1698761151613"])
            cat_id = deal.get("CATEGORY_ID")
            stage_id = deal.get("STAGE_ID")
            deal["CATEGORY_ID"] = categorias.get(cat_id, cat_id)
            deal["STAGE_ID"] = estagios.get(cat_id, {}).get(stage_id, stage_id)
            ids = deal.get("UF_CRM_1699452141037", [])
            if not isinstance(ids, list):
                ids = []
            nomes = [operadora_map.get(str(i)) for i in ids if str(i) in operadora_map]
            deal["UF_CRM_1699452141037"] = ", ".join(filter(None, nomes))
            upsert_deal(conn, deal)
            sucesso += 1
            ids_atualizados.append(deal.get("ID"))
        except Exception as e:
            print(f"⚠️ Erro ao processar deal {deal.get('ID')}: {e}")

    conn.commit()
    conn.close()
    

    print(f"✅ Inseridos {sucesso} de {len(all_deals)} negócios antigos com sucesso.")
    print(f"📋 IDs atualizados no banco: {ids_atualizados}")


def baixar_todos_dados():
    conn = get_conn()
    conn.autocommit = False
    todos = []
    local_params = PARAMS.copy()
    tentativas = 0

    print("🚀 Buscando operadoras dinamicamente...")
    operadora_map = get_operadora_map()


    print("🚀 Buscando categorias para mapear nomes...")
    categorias = get_categories()

    print("🚀 Buscando estágios para todas as categorias...")
    estagios_por_categoria = {}
    for cat_id in categorias.keys():
        estagios_por_categoria[cat_id] = get_stages(cat_id)

    while True:
        print(
            f"📡 Requisição start={local_params['start']} | Total acumulado: {len(todos)}"
        )
        data = fazer_requisicao(WEBHOOKS, local_params)
        if data is None:
            tentativas += 1
            if tentativas >= MAX_RETRIES:
                print("🚫 Máximo de tentativas. Abortando.")
                break
            print(f"⏳ Retentativa {tentativas}/{MAX_RETRIES} em {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)
            continue

        tentativas = 0
        deals = data.get("result", [])

        # Substituir IDs por nomes antes de salvar:
        for deal in deals:
            cat_id = deal.get("CATEGORY_ID")
            stage_id = deal.get("STAGE_ID")
        
            # Substitui categoria e estágio por nome
            if cat_id in categorias:
                deal["CATEGORY_ID"] = categorias[cat_id]
            if cat_id in estagios_por_categoria and stage_id in estagios_por_categoria[cat_id]:
                deal["STAGE_ID"] = estagios_por_categoria[cat_id][stage_id]
        
            # ✅ Converte IDs de operadoras para nomes
            ids = deal.get("UF_CRM_1699452141037", [])
            if not isinstance(ids, list):
                ids = []
            nomes = [operadora_map.get(str(i)) for i in ids if str(i) in operadora_map]
            nomes_filtrados = [n for n in nomes if isinstance(n, str) and n.strip()]
            deal["UF_CRM_1699452141037"] = ", ".join(nomes_filtrados) if nomes_filtrados else ""
        
            # ✅ Formata a data de criação
            deal["DATE_CREATE"] = format_date(deal.get("DATE_CREATE"))
            deal["UF_CRM_1698761151613"] = format_date(deal.get("UF_CRM_1698761151613"))


        
            # ⬇️ Grava no banco
            upsert_deal(conn, deal)




        todos.extend(deals)
        conn.commit()
        print(f"💾 Processados {len(deals)} registros.")

        if "next" in data and data["next"]:
            local_params["start"] = data["next"]
            time.sleep(
                PAGE_DELAY if len(todos) >= LIMITE_REGISTROS_TURBO else REQUEST_DELAY
            )
        else:
            print("🏁 Fim da paginação.")
            break

    conn.close()
    return todos




if __name__ == "__main__":
    load_all_deals()
