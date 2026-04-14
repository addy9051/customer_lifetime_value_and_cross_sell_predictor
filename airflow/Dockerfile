FROM apache/airflow:2.8.1-python3.11

USER root
# Install build dependencies for ML libraries (shap, hdbscan, umap)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
# Copy requirements and install
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "sqlalchemy<2.0.0" "pandas>=2.1.0"
RUN pip install --no-cache-dir -r /requirements.txt
