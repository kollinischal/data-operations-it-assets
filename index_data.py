from elasticsearch import Elasticsearch, helpers
import pandas as pd
import os
from datetime import datetime
import json
import urllib3
import time

# Suppress insecure request warnings (for local testing only)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# === CONFIGURATION ===
ES_ENDPOINT = "https://my-elasticsearch-d182dc.es.asia-south1.gcp.elastic.cloud:443"
ES_API_KEY = "Tk9XS1Jab0J0VmYzWVh2a1FJbTk6cXJMM2NWNmxQMTAyY1VSZ2tXamV5UQ=="  # Replace with your actual API key
TARGET_INDEX = "mini_project"
CSV_FILE = "C:/Users/KolliparaNischal/OneDrive - kyndryl/AIOPS/it_asset_inventory_cleaned.csv"

# === CONNECT TO ELASTICSEARCH ===
es = Elasticsearch(
    ES_ENDPOINT,
    api_key=ES_API_KEY,
    verify_certs=False  # Use ca_certs="path/to/ca.crt" for production
)

if not es.ping():
    print("❌ Connection failed! Please check endpoint or API key.")
    exit()
else:
    print("✅ Connected to Elasticsearch!")

# === READ CSV FILE ===
if not os.path.exists(CSV_FILE):
    print(f"❌ CSV file not found: {CSV_FILE}")
    exit()

df = pd.read_csv(CSV_FILE)
print(f"📄 Loaded {len(df)} records from {CSV_FILE}")

# === DATA VALIDATION AND CLEANING ===
print("🧹 Validating and cleaning data...")

initial_count = len(df)
df = df.dropna(subset=['hostname'])
df = df[df['hostname'].str.strip() != '']
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

# Handle duplicate hostnames
duplicates = df['hostname'].duplicated(keep=False)
if duplicates.any():
    print(f"⚠️ Found {duplicates.sum()} duplicate hostnames, adding unique suffixes...")
    for idx in df[duplicates].index:
        df.loc[idx, 'hostname'] = f"{df.loc[idx, 'hostname']}_dup_{idx}"

# Convert date format
if 'operating_system_installation_date' in df.columns:
    print("📅 Converting date format from DD-MM-YYYY to YYYY-MM-DD...")

    def convert_date_format(date_str):
        if pd.isna(date_str) or str(date_str).strip() == '':
            return None
        try:
            parsed_date = datetime.strptime(str(date_str).strip(), '%d-%m-%Y')
            return parsed_date.strftime('%Y-%m-%d')
        except:
            return None

    df['operating_system_installation_date'] = df['operating_system_installation_date'].apply(convert_date_format)

cleaned_count = len(df)
print(f"✅ Data cleaned: {initial_count} → {cleaned_count} records")

# === INDEX MAPPING ===
index_mapping = {
    "mappings": {
        "properties": {
            "hostname": {"type": "keyword"},
            "country": {"type": "keyword"},
            "operating_system_name": {"type": "keyword"},
            "operating_system_provider": {"type": "keyword"},
            "operating_system_installation_date": {
                "type": "date",
                "format": "yyyy-MM-dd||strict_date_optional_time||epoch_millis"
            },
            "operating_system_lifecycle_status": {"type": "keyword"},
            "os_is_virtual": {"type": "keyword"},
            "is_internet_facing": {"type": "keyword"},
            "image_purpose": {"type": "keyword"},
            "os_system_id": {"type": "keyword"},
            "performance_score": {"type": "float"},
            "indexed_at": {"type": "date"}
        }
    }
}

# === DELETE AND RECREATE INDEX ===
try:
    if es.indices.exists(index=TARGET_INDEX):
        es.indices.delete(index=TARGET_INDEX)
        print(f"🗑️ Deleted existing index '{TARGET_INDEX}'")
        time.sleep(2)
        while es.indices.exists(index=TARGET_INDEX):
            time.sleep(1)

    es.indices.create(index=TARGET_INDEX, body=index_mapping)
    print(f"✅ Created index '{TARGET_INDEX}' with proper mapping")
except Exception as e:
    print(f"⚠️ Index management error: {e}")

# === PREPARE BULK DATA ===
current_time = datetime.now().isoformat()
actions = []

for idx, row in df.iterrows():
    doc = row.to_dict()
    for key, value in doc.items():
        if pd.isna(value):
            doc[key] = None
    doc['indexed_at'] = current_time
    doc['record_id'] = idx

    action = {
        "_index": TARGET_INDEX,
        "_id": doc['hostname'],
        "_source": doc
    }
    actions.append(action)

# === BULK UPLOAD ===
try:
    es_with_options = es.options(request_timeout=60)
    success_count, failed_items = helpers.bulk(
        es_with_options,
        actions,
        chunk_size=100
    )

    print(f"✅ Successfully uploaded {success_count} documents to index '{TARGET_INDEX}'")

    time.sleep(2)
    doc_count = es.count(index=TARGET_INDEX)['count']
    print(f"🔍 Verification: Index contains {doc_count} documents")

except helpers.BulkIndexError as e:
    print(f"❌ Bulk indexing errors occurred:")
    for error in e.errors[:5]:
        print(f"   - {error}")
    print(f"   Total errors: {len(e.errors)}")

except Exception as e:
    print(f"❌ Bulk upload failed: {e}")
