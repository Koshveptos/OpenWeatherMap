FROM python:3.11-slim


ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1


WORKDIR /app


RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*


RUN pip install --no-cache-dir poetry


RUN poetry config virtualenvs.create false


COPY pyproject.toml poetry.lock ./


RUN poetry install --no-interaction --no-ansi --no-root


COPY . .


EXPOSE 8501


CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0"]
