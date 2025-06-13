import requests
import time
from atualizar_cache import get_conn, upsert_deal, format_date, get_operadora_map

# Webhook de leitura com paginação
BITRIX_WEBHOOK = "https://marketingsolucoes.bitrix24.com.br/rest/5332/8zyo7yj1ry4k59b5/crm.deal.list"

# Webhook auxiliar para categorias e estágios
WEBHOOK_CATEGORIES = "https://marketingsolucoes.bitrix24.com.br/rest/5332/8zyo7yj1ry4k59b5/crm.dealcategory.list"
WEBHOOK_STAGES = "https://marketingsolucoes.bitrix24.com.br/rest/5332/8zyo7yj1ry4k59b5/crm.dealcategory.stage.list"

# Cache simples
_cache = {"categories": {"data": None, "timestamp": 0}, "stages": {}}
_CACHE_TTL = 3600  # 1 hora

def fetch_with_retry(url, params=None, retries=3, backoff=1):
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"❌ Erro {attempt+1}/{retries} ao buscar {url}: {e}")
            time.sleep(backoff * (2 ** attempt))
    raise Exception("Erro após múltiplas tentativas")

def get_categories():
    now = time.time()
    if _cache["categories"]["data"] and now - _cache["categories"]["timestamp"] < _CACHE_TTL:
        return _cache["categories"]["data"]
    
    data = fetch_with_retry(WEBHOOK_CATEGORIES)
    categorias = {c["ID"]: c["NAME"] for c in data.get("result", [])}
    _cache["categories"]["data"] = categorias
    _cache["categories"]["timestamp"] = now
    return categorias

def get_stages(cat_id):
    now = time.time()
    if cat_id in _cache["stages"]:
        if now - _cache["stages"][cat_id]["timestamp"] < _CACHE_TTL:
            return _cache["stages"][cat_id]["data"]
    
    params = {"id": cat_id}
    data = fetch_with_retry(WEBHOOK_STAGES, params=params)
    stages = {s["STATUS_ID"]: s["NAME"] for s in data.get("result", [])}
    _cache["stages"][cat_id] = {"data": stages, "timestamp": now}
    return stages

def process_deal(deal, categorias, estagios_por_categoria, operadora_map):
    # Datas
    if "DATE_CREATE" in deal:
        deal["DATE_CREATE"] = format_date(deal["DATE_CREATE"])
    if "UF_CRM_1698761151613" in deal:
        deal["UF_CRM_1698761151613"] = format_date(deal["UF_CRM_1698761151613"])

    # Categoria e estágio
    cat_id = deal.get("CATEGORY_ID")
    stage_id = deal.get("STAGE_ID")
    if cat_id in categorias:
        deal["CATEGORY_ID"] = categorias[cat_id]
    if cat_id in estagios_por_categoria and stage_id in estagios_por_categoria[cat_id]:
        deal["STAGE_ID"] = estagios_por_categoria[cat_id][stage_id]

    # Operadora
    ids = deal.get("UF_CRM_1699452141037", [])
    if not isinstance(ids, list):
        ids = []
    nomes = [operadora_map.get(str(i)) for i in ids if str(i) in operadora_map]
    nomes_filtrados = [n for n in nomes if isinstance(n, str) and n.strip()]
    deal["UF_CRM_1699452141037"] = ", ".join(nomes_filtrados) if nomes_filtrados else ""

    return deal

def main():
    print("🔁 Iniciando leitura paginada dos deals...")
    start = 0
    total = 0

    categorias = get_categories()
    estagios_por_categoria = {cat: get_stages(cat) for cat in categorias}
    operadora_map = get_operadora_map()

    conn = get_conn()
    
    while True:
        params = {
            "start": start,
            "select[]": [
                "ID", "TITLE", "CATEGORY_ID", "STAGE_ID", "DATE_CREATE",
                "UF_CRM_1698761151613", "UF_CRM_1699452141037"
            ]
        }
        data = fetch_with_retry(BITRIX_WEBHOOK, params=params)

        deals = data.get("result", [])
        if not deals:
            break

        print(f"📥 Página com start={start} — {len(deals)} negócios")

        for d in deals:
            deal_id = d["ID"]
            try:
                d = process_deal(d, categorias, estagios_por_categoria, operadora_map)
                upsert_deal(conn, d)
                print(f"✅ Inserido/atualizado: ID {deal_id}")
                total += 1
            except Exception as e:
                print(f"❌ Erro ao inserir ID {deal_id}: {e}")

        if "next" not in data:
            break
        start = data["next"]

    conn.commit()
    conn.close()
    print(f"🎉 Concluído! {total} negócios processados.")

if __name__ == "__main__":
    main()
