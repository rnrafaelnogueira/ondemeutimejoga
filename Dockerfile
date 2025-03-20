# Use a imagem base Python 3.11 (Debian/Ubuntu)
FROM python:3.11

# Defina variáveis de ambiente para evitar problemas de cache e buffering
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Atualize pacotes e instale dependências do sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    libssl-dev \
    build-essential \
    libpq-dev \
    curl \
    bash && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Definir o diretório de trabalho dentro do container
WORKDIR /app

# Copiar o arquivo requirements.txt para o container
COPY requirements.txt .

# Instalar as dependências
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

# Copiar o código da aplicação para o container
COPY . .

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]