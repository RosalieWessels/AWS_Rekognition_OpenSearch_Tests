from opensearchpy import OpenSearch
from dotenv import load_dotenv
import os

# Load environment variables from .env.local file
load_dotenv('.env.local')

# Get AWS credentials and region from environment variables
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_session_token = os.getenv('AWS_SESSION_TOKEN')
aws_region = os.getenv('AWS_REGION')

opensearch_endpoint = os.getenv('OPENSEARCH_ENDPOINT')

# Initialize OpenSearch client
opensearch_client = OpenSearch(
    hosts=[opensearch_endpoint],
    http_auth=(os.getenv('OPENSEARCH_USERNAME'), os.getenv('OPENSEARCH_PASSWORD')),
    use_ssl=True,
    verify_certs=True,
)

index_name = "images_index"

# List all indexes and document counts
response = opensearch_client.cat.indices(format='json')
for index in response:
    print(f"Index: {index['index']}, Document Count: {index['docs.count']}")


# Search query
query = {
    "query": {
        "match": {
        "field_name": {
            "query": "food",
            "fuzziness": "AUTO"
        }
        }
    }
}

# Execute search
response = opensearch_client.search(index=index_name, body=query)
print("Search results:", response['hits']['hits'])

# Query to get all documents from the index
query = {
    "query": {
        "match": {
            "description" : "A picture with a man in it"
        }
    },
    "size": 100  # Adjust size as needed, default is 10, but you can increase it to get more results
}

# # Execute search query
# response = opensearch_client.search(
#     index=index_name,
#     body=query
# )

# # Print out documents in the index
# for doc in response['hits']['hits']:
#     print(f"Document ID: {doc['_id']}, Contents: {doc['_source']}")