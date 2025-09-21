import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import logging
from datetime import datetime, timedelta

# === Logging Setup ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# === Configuration ===
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "2626",  # Update if different
    "host": "localhost",
    "port": "5432"
}

# === Extract ===
def fetch_huggingface_models(limit=500):
    logging.info(f"📦 Fetching {limit} models from Hugging Face API...")
    url = "https://huggingfacgit e.co/api/models"
    params = {"limit": limit}
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

# === Transform ===
def transform_models_data(models):
    logging.info("🔧 Transforming and cleaning data...")

    df = pd.DataFrame([{
        "modelname":model.get("name",""),
        "modelId": model.get("modelId", ""),
        "author": model.get("author", "unknown"),
        "downloads": model.get("downloads") or 0,
        "likes": model.get("likes") or 0,
        "pipeline_tag": model.get("pipeline_tag", ""),
        "library_name": model.get("library_name", ""),
        "model_type": model.get("model_type", ""),
        "license": model.get("license", "unknown"),
        "private": model.get("private", False),
        "last_modified": pd.to_datetime(model.get("lastModified", pd.NaT), errors='coerce')
    } for model in models])

    # Replace NaT with current timestamp
    df["last_modified"] = df["last_modified"].fillna(pd.Timestamp.now())

    # Add fetch time
    fetch_time = datetime.now()
    df["fetch_time"] = fetch_time

    # Add recent flag (Boolean)
    df["is_recent"] = df["last_modified"].apply(lambda x: bool(x >= fetch_time - timedelta(days=30)))

    return df

# === Load ===
def load_to_postgres(df):
    logging.info("🛢️ Loading data into PostgreSQL...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Create table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS huggingface_models (
                modelId TEXT PRIMARY KEY,
                author TEXT,
                downloads INTEGER,
                likes INTEGER,
                pipeline_tag TEXT,
                library_name TEXT,
                model_type TEXT,
                license TEXT,
                private BOOLEAN,
                last_modified TIMESTAMP,
                is_recent BOOLEAN,
                fetch_time TIMESTAMP
            );
        """)

        # Prepare data
        records = df[[
            "modelId", "author", "downloads", "likes", "pipeline_tag",
            "library_name", "model_type", "license", "private",
            "last_modified", "is_recent", "fetch_time"
        ]].values.tolist()

        # Insert or update (upsert)
        insert_query = """
            INSERT INTO huggingface_models (
                modelId, author, downloads, likes, pipeline_tag, library_name,
                model_type, license, private, last_modified, is_recent, fetch_time
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (modelId) DO UPDATE SET
                downloads = EXCLUDED.downloads,
                likes = EXCLUDED.likes,
                pipeline_tag = EXCLUDED.pipeline_tag,
                library_name = EXCLUDED.library_name,
                model_type = EXCLUDED.model_type,
                license = EXCLUDED.license,
                private = EXCLUDED.private,
                last_modified = EXCLUDED.last_modified,
                is_recent = EXCLUDED.is_recent,
                fetch_time = EXCLUDED.fetch_time;
        """

        execute_batch(cursor, insert_query, records)
        conn.commit()
        cursor.close()
        conn.close()
        logging.info("✅ Data successfully loaded into PostgreSQL.")

    except Exception as e:
        logging.error(f"❌ Database error: {e}")
        raise

# === Run ETL ===
if __name__ == "__main__":
    try:
        models = fetch_huggingface_models(limit=500)
        df = transform_models_data(models)
        load_to_postgres(df)
        logging.info("🎉 ETL process completed successfully.")
    except Exception as e:
        logging.error(f"ETL process failed: {e}")
