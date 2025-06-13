from atualizar_cache import get_conn, upsert_deal, format_date
import requests, time

WEBHOOK_CATEGORIES = "https://marketingsolucoes.bitrix24.com.br/rest/5332/8zyo7yj1ry4k59b5/crm.dealcategory.list"
WEBHOOK_STAGES = "https://marketingsolucoes.bitrix24.com.br/rest/5332/8zyo7yj1ry4k59b5/crm.dealstage.list"

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

if __name__ == "__main__":
    load_all_deals()
