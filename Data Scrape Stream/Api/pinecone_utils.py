import os
from dotenv import load_dotenv
from pinecone import Pinecone, ServerlessSpec

# Load environment variables from .env file
load_dotenv()

# Fetch API key and environment from environment variables
pinecone_api_key = os.getenv("PINECONE_API_KEY")
pinecone_environment = os.getenv("PINECONE_ENVIRONMENT", "us-east-1")
INDEX_NAME = os.getenv("PINECONE_INDEX_NAME", "pdf-embeddings-index")
EMBEDDING_DIMENSION = int(os.getenv("EMBEDDING_DIMENSION", 768))  # Default dimension set to 768

# Initialize the Pinecone client
pinecone_client = Pinecone(
    api_key=pinecone_api_key
)

# Function to create or retrieve Pinecone index
def get_pinecone_index():
    if INDEX_NAME not in pinecone_client.list_indexes().names:
        # Specify serverless spec with cloud provider and region
        pinecone_client.create_index(
            name=INDEX_NAME,
            dimension=EMBEDDING_DIMENSION,
            metric="cosine",
            spec=ServerlessSpec(
                cloud="aws",
                region=pinecone_environment
            )
        )
    return pinecone_client.index(INDEX_NAME)

# Function to store embeddings in Pinecone
def store_embeddings(metadata, embeddings):
    index = get_pinecone_index()
    document_id = metadata.get("title", "unknown_id")  # Use title or other unique ID as document_id
    index.upsert(vectors=[(document_id, embeddings, metadata)])

# Function to retrieve embeddings from Pinecone
def get_embeddings(document_id):
    index = get_pinecone_index()
    response = index.query(top_k=1, include_values=True, namespace="", id=document_id)
    if response and response['matches']:
        return response['matches'][0]['values']
    return None

# Optional function to delete all data from the Pinecone index (for testing)
def delete_all_data():
    index = get_pinecone_index()
    index.delete(delete_all=True)