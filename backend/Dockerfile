FROM python:3.11-slim

# Java is required by pyspark (spark-submit client) and papermill notebook kernels.
# libxmlsec1/libxml2 + build tools are required by python3-saml's xmlsec dependency
# (SAML assertions are XML-signed; this is the real crypto library, not optional).
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    curl \
    git \
    build-essential \
    libxml2-dev \
    libxmlsec1-dev \
    libxmlsec1-openssl \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

ENV PYTHONUNBUFFERED=1
EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
