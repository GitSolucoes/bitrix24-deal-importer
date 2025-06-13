# 🔄 Carga e Atualização de Negócios Antigos no Bitrix24  
(Scroll down for English version 🇺🇸)

Este projeto é um bot Python que realiza a carga inicial e atualizações dos negócios antigos (deals) no Bitrix24, sincronizando dados completos via API e armazenando no banco local para consultas e integrações.

✅ **O que ele faz?**  
- Carrega todos os negócios antigos a partir de uma data definida.  
- Atualiza negócios específicos ao receber notificações do webhook do Bitrix24.  
- Converte e formata campos (datas, categorias, estágios, operadoras).  
- Armazena os dados no banco PostgreSQL, com upsert para evitar duplicatas.  

🔧 **Como funciona?**  
- Script Python com função de carga inicial paginada usando API oficial.  
- Serviço Flask para receber webhooks e atualizar negócios em tempo real.  
- Cache local para categorias e estágios para reduzir chamadas externas.  
- Uso de tratamento de erros, retries e limites de requisição.  

🛡️ **Segurança**  
- Timeout e tratamento de erros para chamadas à API do Bitrix24.  
- Logs de execução e falhas para auditoria.  

📈 **Benefícios**  
- Sincronização confiável e automatizada dos dados antigos e atuais.  
- Facilita análises, relatórios e integrações com outros sistemas.  
- Reduz retrabalho manual e inconsistências nos dados.  

Quer manter seus negócios do Bitrix24 sempre atualizados e sincronizados? Fale com a gente! 😉

---

# 🔄 Historic Deals Load and Update for Bitrix24  

This project is a Python bot that performs initial load and updates of historic deals in Bitrix24, syncing full data via API and storing it locally for queries and integrations.

✅ **What does it do?**  
- Loads all historic deals from a specified date.  
- Updates specific deals on receiving Bitrix24 webhook notifications.  
- Converts and formats fields (dates, categories, stages, operators).  
- Stores data in PostgreSQL with upsert logic.  

🔧 **How does it work?**  
- Python script for paginated initial load using official API.  
- Flask service to receive webhooks and update deals in real time.  
- Local cache for categories and stages to reduce API calls.  
- Error handling, retries, and rate limit management.  

🛡️ **Security**  
- Timeouts and error handling for API requests.  
- Execution logs for auditing and troubleshooting.  

📈 **Benefits**  
- Reliable automated synchronization of historic and current deals.  
- Enables analytics, reporting, and system integrations.  
- Minimizes manual work and data inconsistencies.  

Want to keep your Bitrix24 deals always up to date and synced? Let’s talk! 😉
