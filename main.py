# carga_antigos.py
from atualizar_cache import get_conn, upsert_deal, format_date, get_operadora_map
import requests, time

def get_categories():
    # Igual ao outro
    ...

def get_stages(cat_id):
    # Igual ao outro
    ...

def load_all_deals():
    print("🔁 Iniciando carga completa de negócios antigos...")
    all_deals = []
    start = 0
    url = "https://marketingsolucoes.bitrix24.com.br/rest/5332/8zyo7yj1ry4k59b5/crm.deal.list"

    while True:
        try:
            response = requests.post(url, json={
                "start": start,
                "order": {"ID": "ASC"},
                "filter": {">=DATE_CREATE": "2024-02-09T00:00:00Z"},
                "select": ["*"]
            }, timeout=300)
            response.raise_for_status()
            data = response.json()
            result = data.get("result", [])
            if not result:
                break

            all_deals.extend(result)
            start = data.get("next")
            print(f"📦 Total acumulado: {len(all_deals)} negócios")

            if not start:
                break
            time.sleep(1.5)

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

    for deal in all_deals:
    try:
        print(f"🔍 Processando deal ID: {deal.get('ID')}")  # <-- novo
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
    except Exception as e:
        print(f"⚠️ Erro ao processar deal {deal.get('ID')}: {e}")


    conn.commit()
    conn.close()
    print(f"✅ Inseridos {len(all_deals)} negócios antigos.")


if __name__ == "__main__":
    load_all_deals() 
