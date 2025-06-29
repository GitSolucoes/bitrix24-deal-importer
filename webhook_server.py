from flask import Flask, request, jsonify
from atualizar_cache import get_conn, upsert_deal, format_date, get_categories, get_stages, get_operadora_map
import requests
import time

app = Flask(__name__)

BITRIX_WEBHOOK = "https://marketingsolucoes.bitrix24.com.br/rest/5332/8zyo7yj1ry4k59b5/crm.deal.get"

# Cache simples para estágios em memória
stage_cache = {}

def get_stages_with_retry(cat_id, max_retries=5, base_wait=2):
    if cat_id in stage_cache:
        return stage_cache[cat_id]

    for attempt in range(1, max_retries + 1):
        try:
            stages = get_stages(cat_id)
            stage_cache[cat_id] = stages
            return stages
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 503:
                wait_time = base_wait * (2 ** (attempt - 1))
                print(f"⚠️ 503 Serviço indisponível para categoria {cat_id}. Tentativa {attempt}/{max_retries}, aguardando {wait_time}s...")
                if attempt == max_retries:
                    print(f"❌ Falha definitiva na categoria {cat_id} após {max_retries} tentativas.")
                    return {}
                time.sleep(wait_time)
            else:
                print(f"❌ Erro HTTP inesperado: {e}")
                raise
        except Exception as e:
            print(f"❌ Erro ao buscar estágios para categoria {cat_id}: {e}")
            return {}

@app.route("/bitrix-webhook", methods=["POST"])
def bitrix_webhook():
    print("🔔 Webhook recebido")

    conn = get_conn()

    try:
        form_data = request.form.to_dict(flat=False)
        print("📦 Form recebido:", form_data)

        deal_id = form_data.get("data[FIELDS][ID]", [None])[0]
        if not deal_id:
            return jsonify({"error": "ID do negócio não encontrado"}), 400

        resp = requests.get(BITRIX_WEBHOOK, params={"id": deal_id}, timeout=20)
        data = resp.json()
        if "result" not in data:
            return jsonify({"error": "Resposta inválida do Bitrix"}), 502

        deal = data["result"]

        # Converte datas
        if "DATE_CREATE" in deal:
            deal["DATE_CREATE"] = format_date(deal["DATE_CREATE"])
        if "UF_CRM_1698761151613" in deal:
            deal["UF_CRM_1698761151613"] = format_date(deal["UF_CRM_1698761151613"])

        # Pega mapa categorias e operadoras
        categorias = get_categories()
        operadora_map = get_operadora_map()

        # Pega a categoria do negócio e busca os estágios só para ela
        cat_id = deal.get("CATEGORY_ID")
        estagios_por_categoria = {}
        if cat_id in categorias:
            estagios_por_categoria[cat_id] = get_stages_with_retry(cat_id)

        # Converte categoria e estágio para nome
        if cat_id in categorias:
            deal["CATEGORY_ID"] = categorias[cat_id]
        stage_id = deal.get("STAGE_ID")
        if cat_id in estagios_por_categoria and stage_id in estagios_por_categoria[cat_id]:
            deal["STAGE_ID"] = estagios_por_categoria[cat_id][stage_id]

        # Converte lista de IDs de operadoras para nomes string
        ids = deal.get("UF_CRM_1699452141037", [])
        if not isinstance(ids, list):
            ids = []
        nomes = [operadora_map.get(str(i)) for i in ids if str(i) in operadora_map]
        nomes_filtrados = [n for n in nomes if isinstance(n, str) and n.strip()]
        deal["UF_CRM_1699452141037"] = ", ".join(nomes_filtrados) if nomes_filtrados else ""

        # Upsert no banco
        conn = get_conn()
        upsert_deal(conn, deal)
        conn.commit()
        conn.close()

        print(f"✅ Deal {deal_id} atualizado com sucesso")
        return jsonify({"status": "ok", "deal_id": deal_id}), 200

    except Exception as e:
        print(f"❌ Erro ao processar webhook: {e}")
        return jsonify({"error": str(e)}), 500
    finally :
        if conn :
            conn.close()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=1321)
