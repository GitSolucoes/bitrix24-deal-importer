# Dockerfile
FROM python:3.10-slim

# Diretório de trabalho
WORKDIR /app

# Copia tudo pro container
COPY . .

# Instala dependências
RUN pip install --no-cache-dir -r requirements.txt

# Porta opcional para expor se usar API
EXPOSE 1421

# Comando para rodar o script
CMD ["python", "main.py"]
