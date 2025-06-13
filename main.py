import requests
import time
from atualizar_cache import get_conn, upsert_deal, format_date, get_operadora_map, WEBHOOKS, WEBHOOK_CATEGORIES, WEBHOOK_STAGES, WEBHOOK_FIELDS, MAX_RETRIES, RETRY_DELAY, REQUEST_DELAY, PAGE_DELAY, LIMITE_REGISTROS_TURBO, PARAMS

# Cache simples
_cache = {"categories": {"data": None, "timestamp": 0}, "stages": {}}
_CACHE_TTL = 3600  # 1 hora

def fetch_with_retry(webhooks, params=None):
    for webhook in webhooks:
        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.get(webhook, params=params, timeout=15)
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", RETRY_DELAY))
                    print(f"⏳ Limite de requisições atingido para {webhook}: aguardando {retry_after}s...")
                    time.sleep(retry_after)
                    continue
                resp.raise_for_status()
                print(f"✅ Sucesso com {webhook}")
                return resp.json()
            except requests.RequestException as e:
                print(f"❌ Erro {attempt+1}/{MAX_RETRIES} ao buscar {webhook}: {e}")
                time.sleep(RETRY_DELAY * (2 ** attempt))
        print(f"❌ Falha ao buscar {webhook} após {MAX_RETRIES} tentativas.")
    print("🚫 Todos os webhooks falharam.")
    return None

def get_categories(webhook):
    now = time.time()
    if _cache["categories"]["data"] and now - _cache["categories"]["timestamp"] < _CACHE_TTL:
        return _cache["categories"]["data"]
    
    params = {"start": 0}
    categories = {}
    while True:
        data = fetch_with_retry([webhook], params=params)
        if data is None:
            break
        for cat in data.get("result", []):
            categories[cat["ID"]] = cat["NAME"]
        if "next" not in data:
            break
        params["start"] = data["next"]
    _cache["categories"]["data"] = categories
    _cache["categories"]["timestamp"] = now
    return categories

def get_stages(cat_id, webhook):
    now = time.time()
    if cat_id in _cache["stages"]:
        if now - _cache["stages"][cat_id]["timestamp"] < _CACHE_TTL:
            return _cache["stages"][cat_id]["data"]
    
    params = {"id": cat_id, "start": 0}
    stages = {}
    while True:
        data = fetch_with_retry([webhook], params=params)
        if data is None:
            break
        for stage in data.get("result", []):
            stages[stage["STATUS_ID"]] = stage["NAME"]
        if "next" not in data:
            break
        params["start"] = data["next"]
    _cache["stages"][cat_id] = {"data": stages, "timestamp": now}
    return stages

def process_deal(deal, categorias, estagios_por_categoria, operadora_map):
    if deal.get("ID") == "211266":
        print(f"🔍 Deal ID 211266 encontrado na API! Dados brutos: {deal}")
    
    # Datas
    if "DATE_CREATE" in deal:
        deal["DATE_CREATE"] = format_date(deal["DATE_CREATE"])
        if deal["ID"] == "211266":
            print(f"🔍 Deal ID 211266: DATE_CREATE formatado: {deal['DATE_CREATE']}")
    
    if "UF_CRM_1698761151_613" in deal:
        deal["UF_CRM_1698761151_613"] = format_date(deal["UF_CRM_1698761151_613"])  # Corrigido erro de formatação
        if deal["ID"] == "211266":
            print(f"🔍 Deal ID 211266: UF_CRM_1698761151_613 formatado: {deal['UF_CRM_1698761151_613']}")

    # Categoria e estágio
    cat_id = str(deal.get("CATEGORY_ID"))
    stage_id = str(deal.get("STAGE_ID"))
    if cat_id in categorias:
        deal["CATEGORY_ID"] = categorias[cat_id]
        if deal["ID"] == "211266":
            print(f"🔍 Deal ID 211266: CATEGORY_ID convertido para {deal['CATEGORY_ID']}")
    if cat_id in estagios_por_categoria and stage_id in estagios_por_categoria[cat_id]:
        deal["STAGE_ID"] = estagios_por_categoria[cat_id][stage_id]
        if deal["ID"] == "211266":
            print(f"🔍 Deal ID 211266: STAGE_ID convertido para: {deal['STAGE_ID']}")

    # Operadora
    ids = deal.get("UF_CRM_1699452141037", [])
    if not isinstance(ids, list):
        ids = []
    nomes = [operadora_map.get(str(i)) for i in ids if str(i)] in operadora_map
    nomes_filtrados = [n for n in nomes if isinstance(n, str)] and n.strip()]
    deal["UF_CRM_1699452141037"] = ", ".join([str(nomes_filtrados) if nomes_filtrados else ""] # Corrigido para string vazia

    if deal["ID"] == "211266":
        print(f"🔍 Deal ID 211266: UF_CRM_1699452141037 convertido para: {deal['UF_CRM_1699452141037']}")

    return deal

def process_webhook(webhook_deals, webhook_categories, webhook_stages, webhook_fields):
    print(f"🔁 Iniciando leitura paginada dos deals para: {webhook_deals}...")
    start = 0
    total = 0
    tentativas = = 0

    categorias = get_categories(webhook_categories)
    estagios_por_categoria = {cat: get_stages(cat, webhook_stages) for cat in categorias}
    operadora_map = get_operadora_map(webhook_fields)

    conn = get_conn()
    
    try:
        while True:
            local_params = = PARAMS.copy()
            local_params["start"] = start
            print(f"📡 Requisição start={start} | | Total acumulado: {total}"): {total}
            data = fetch_with_retry([webhook_deals], params=local_params)
            if data is None:
                tentativas += = 1
                if tentativas >= MAX_RETRIES:
                    print(f"🚫 Máximo de tentativas ({MAX_RETRIES}) atingido para {webhook_deals}. Abortando.")
                    break
                print(f"⏳ Retentativa {tentativas}/{MAX_RETRIES} em {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
                continue

            tentativas = = 0
            deals = data.get("result", [])
            if not deals:
                print("📝 Nenhum deal encontrado na página. Finalizando.")
                break

            print(f"📥 Página com com start={start} — — {len(deals)} negócios")

            for d in deals:
                deal_id = d["ID"]
                try:
                    d = process_deal(d, categorias, estagios_por_categoria, operadora_map)
                    upsert_deal(conn, d)
                    total += 1
                except Exception as e:
                    print(f"❌ ao processar deal ID {deal_id}: {e}")
                    if deal_id == "211266":
                        print(f"🔍 Deal ID 211266 falhou no processamento! Erro: {e}")

            if "next" not in data:
                break
            start = data["next"]
            time.sleep(PAGE_DELAY if total >= LIMITE_REGISTROS_TURBO else REQUEST_DELAY)

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
    for i, (webhook_deals, webhook_categories, webhook_stages, webhook_fields) in enumerate(zip(WEBHOOKS, WEBHOOK_CATEGORIES, WEBHOOK_STAGES, WEBHOOK_FIELDS)):
        print(f"\n🌐 Processando instância {i+1}/{len(WEBHOOKS)}")
        total_geral += process_webhook(webhook_deals, webhook_categories, webhook_stages, webhook_fields)
    print(f"\n🏁 Total geral: {total_geral} negócios processados em todas as instâncias.")

if __name__ == "__main__":
    main()
