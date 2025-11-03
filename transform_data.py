from elasticsearch import Elasticsearch, helpers

from datetime import datetime

import os

# === CONFIGURATION ===


ES_ENDPOINT = "https://my-elasticsearch-d182dc.es.asia-south1.gcp.elastic.cloud:443"

ES_API_KEY = "Tk9XS1Jab0J0VmYzWVh2a1FJbTk6cXJMM2NWNmxQMTAyY1VSZ2tXamV5UQ=="

SOURCE_INDEX = "mini_project"

TARGET_INDEX = "mini_project_transformed"

# === CONNECT TO ELASTICSEARCH ===


es = Elasticsearch(

    ES_ENDPOINT,

    api_key=ES_API_KEY,

    verify_certs=True

)

if not es.ping():

    print("❌ Connection failed!")

    exit()


else:

    print("✅ Connected to Elasticsearch!")

# === STEP 1: Reindex Data ===


print("🔄 Reindexing data...")

es.reindex(

    body={

        "source": {"index": SOURCE_INDEX},

        "dest": {"index": TARGET_INDEX}

    },

    wait_for_completion=True

)

print("✅ Reindexing complete.")

# === STEP 2: Add Derived Field 'risk_level' ===


print("🛡️ Adding risk_level field...")

es.update_by_query(

    index=TARGET_INDEX,

    body={

        "script": {

            "source": """


                if (ctx._source.operating_system_lifecycle_status == 'EOL' || 


                    ctx._source.operating_system_lifecycle_status == 'EOS') {


                    ctx._source.risk_level = 'High';


                } else {


                    ctx._source.risk_level = 'Low';


                }


            """

        },

        "query": {

            "exists": {"field": "operating_system_lifecycle_status"}

        }

    }

)

print("✅ risk_level field added.")

# === STEP 3: Calculate System Age ===


print("📅 Calculating system age...")

query = {

    "size": 1000,

    "query": {

        "exists": {

            "field": "operating_system_installation_date"

        }

    }

}

results = es.search(index=TARGET_INDEX, body=query)

updates = []

current_year = datetime.now().year

for doc in results['hits']['hits']:

    doc_id = doc['_id']

    source = doc['_source']

    install_date = source.get('operating_system_installation_date')

    try:

        install_year = int(install_date[:4])

        system_age = current_year - install_year

        updates.append({

            "_op_type": "update",

            "_index": TARGET_INDEX,

            "_id": doc_id,

            "doc": {"system_age": system_age}

        })


    except Exception as e:

        print(f"Skipping document {doc_id} due to error: {e}")

if updates:

    helpers.bulk(es, updates)

    print(f"✅ Updated {len(updates)} documents with system_age.")


else:

    print("⚠️ No documents updated.")

# === STEP 4: Delete Records with Missing Hostnames or Unknown Providers ===


print("🗑️ Deleting invalid records...")

es.delete_by_query(

    index=TARGET_INDEX,

    body={

        "query": {

            "bool": {

                "should": [

                    {"bool": {"must_not": {"exists": {"field": "hostname"}}}},

                    {"match": {"provider": "Unknown"}}

                ]

            }

        }

    }

)

print("✅ Invalid records deleted.")
