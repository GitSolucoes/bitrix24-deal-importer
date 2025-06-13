# Use uma imagem oficial do Python como base
FROM python:3.10-slim

# Define o diretório de trabalho dentro do container
WORKDIR /app

# Copia os arquivos de requirements para dentro do container
COPY requirements.txt .

# Instala as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo o código da aplicação para o container
COPY . .

# Expõe a porta que seu app vai rodar (mude conforme sua app)
EXPOSE 1421

# Comando para rodar sua aplicação (mude conforme seu entrypoint)
CMD ["python", "carga_antigos.py"]
