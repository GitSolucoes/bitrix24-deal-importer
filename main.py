import requests
import time
from atualizar_cache import get_conn, upsert_deal, format_date, get_operadora_map, WEBHOOKS, WEBHOOK_CATEGORIES, WEBHOOK_STAGES, MAX_RETRIES, RETRY_DELAY

# Cache simples
_cache = {"categories": {"data": None, "timestamp": 0}, "stages": {}}
_CACHE_TTL = 3600  # 1 hora

def fetch_with_retry(url, params=None, retries=MAX_RETRIES, backoff=RETRY_DELAY):
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            print(f"❌ Erro {attempt+1}/{retries} ao buscar {url}: {e}")
            time.sleep(backoff * (2 ** attempt))
    raise Exception(f"Erro após {retries} tentativas")

def get_categories(webhook):
    now = time.time()
    if _cache["categories"]["data"] and now - _cache["categories"]["timestamp"] < _CACHE_TTL:
        return _cache["categories"]["data"]
    
    data = fetch_with_retry(webhook)
    categorias = {c["ID"]: c["NAME"] for c in data.get("result", [])}
    _cache["categories"]["data"] = categorias
    _cache["categories"]["timestamp"] = now
    return categorias

def get_stages(cat_id, webhook):
    now = time.time()
    if cat_id in _cache["stages"]:
        if now - _cache["stages"][cat_id]["timestamp"] < _CACHE_TTL:
            return _cache["stages"][cat_id]["data"]
    
    params = {"id": cat_id}
    data = fetch_with_retry(webhook, params=params)
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
    cat_id = str(deal.get("CATEGORY_ID"))
    stage_id = str(deal.get("STAGE_ID"))
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

def process_webhook(webhook_deals, webhook_categories, webhook_stages):
    print(f"🔁 Iniciando leitura paginada dos deals para {webhook_deals}...")
    start = 0
    total = 0

    categorias = get_categories(webhook_categories)
    estagios_por_categoria = {cat: get_stages(cat, webhook_stages) for cat in categorias}
    operadora_map = get_operadora_map()

    conn = get_conn()
    
    try:
        while True:
            params = {
                "start": start,
                "select[]": [
                    "ID", "TITLE", "CATEGORY_ID", "STAGE_ID", "DATE_CREATE",
                    "UF_CRM_1698761151613", "UF_CRM_1699452141037",
                    "UF_CRM_1700661314351", "UF_CRM_1698698407472", "UF_CRM_1698698858832",
                    "UF_CRM_1697653896576", "UF_CRM_1697762313423", "UF_CRM_1697763267151",
                    "UF_CRM_1697764091406", "UF_CRM_1697807340141", "UF_CRM_1697807353336",
                    "UF_CRM_1697807372536", "UF_CRM_1697808018193", "UF_CRM_1698688252221",
                    "UF_CRM_1700661287551", "UF_CRM_1731588487", "UF_CRM_1700661252544",
                    "UF_CRM_1731589190"
                ]
            }
            data = fetch_with_retry(webhook_deals, params=params)

            deals = data.get("result", [])
            if not deals:
                break

            print(f"📥 Página com start={start} — {len(deals)} negócios")

            for d in deals:
                deal_id = d["ID"]
                try:
                    d = process_deal(d, categorias, estagios_por_categoria, operadora_map)
                    upsert_deal(conn, d)
                    total += 1
                except Exception as e:
                    print(f"❌ Erro ao processar deal ID {deal_id}: {e}")

            if "next" not in data:
                break
            start = data["next"]

        conn.commit()
    except Exception as e:
        print(f"❌ Erro geral ao processar webhook {webhook_deals}: {e}")
        conn.rollback()
    finally:
        conn.close()

    print(f"🎉 Concluído para {webhook_deals}! {total} negócios processados.")
    return total

def main():
    total_geral = 0
    for i, (webhook_deals, webhook_categories, webhook_stages) in enumerate(zip(WEBHOOKS, WEBHOOK_CATEGORIES, WEBHOOK_STAGES)):
        print(f"\n🌐 Processando instância {i+1}/{len(WEBHOOKS)}")
        total_geral += process_webhook(webhook_deals, webhook_categories, webhook_stages)
    print(f"\n🏁 Total geral: {total_geral} negócios processados em todas as instâncias.")

if __name__ == "__main__":
    main()
