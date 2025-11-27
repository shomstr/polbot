FROM python:3.11-slim

WORKDIR /app

# Устанавливаем системные зависимости
RUN apt-get update && apt-get install -y \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

# Копируем requirements и устанавливаем Python зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем исходный код
COPY . .

# Создаем volume для базы данных и сессий
VOLUME ["/app/sessions", "/app/data"]

# Создаем директории для данных
RUN mkdir -p sessions data

# Запускаем бота
CMD ["python", "bot_manager_server.py"]