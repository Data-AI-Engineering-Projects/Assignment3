import base64
import hashlib
import os
import time
import requests
import tempfile
import re
import fitz  # PyMuPDF for PDF extraction
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer
from langchain.text_splitter import RecursiveCharacterTextSplitter
from pinecone import Pinecone, ServerlessSpec
from .document_processors import parse_all_tables, parse_all_images, load_multimodal_data, load_data_from_directory
from .helper_utils import describe_image, is_graph, process_graph, extract_text_around_item, process_text_blocks, save_uploaded_file
from huggingface_hub import hf_hub_download

# Load environment variables
load_dotenv()

# Initialize API keys and variables
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
INDEX_NAME = "pdf-embeddings-index"
PINECONE_ENVIRONMENT = "us-east-1"

# Validate environment variables
if not PINECONE_API_KEY:
    raise ValueError("PINECONE_API_KEY is not set in the environment.")

# Initialize Pinecone client
pinecone_client = Pinecone(api_key=PINECONE_API_KEY)

# Check if the index exists; create it if it doesn't
if INDEX_NAME not in [index.name for index in pinecone_client.list_indexes()]:
    pinecone_client.create_index(
        name=INDEX_NAME,
        dimension=384,  # Dimension varies by embedding model
        metric='cosine',
        spec=ServerlessSpec(cloud='aws', region=PINECONE_ENVIRONMENT)
    )

# Initialize models
sentence_model = SentenceTransformer('all-MiniLM-L6-v2')

# User choice function for embedding models
def initialize_embedding_model(choice):
    if choice == 'sentence-transformers':
        return sentence_model
    else:
        raise ValueError("Invalid embedding model choice. Choose 'sentence-transformers'")

# Function to create embeddings based on user-selected model
def create_embeddings(chunks, model_choice):
    model = initialize_embedding_model(model_choice)
    embeddings = [model.encode(chunk).tolist() for chunk in chunks]
    return embeddings

# Utility functions

def get_document_index_name(document_path):
    """Generate a valid Pinecone index name based on document content."""
    with open(document_path, "rb") as file:
        document_hash = hashlib.md5(file.read()).hexdigest()
    # Create a valid index name
    index_name = f"index_{document_hash}"
    index_name = re.sub(r'[^a-z0-9-]', '-', index_name.lower())  # Sanitize name for Pinecone
    return index_name

# Modify get_or_create_pinecone_index function to include spec
index_registry = {}

def get_or_create_pinecone_index(document_path, dimension=384):
    """Fetch or create a Pinecone index for the document."""
    index_name = get_document_index_name(document_path)
    
    if index_name in index_registry:
        print(f"Using existing index for document: {index_name}")
    else:
        # Check if index exists in Pinecone
        if index_name not in [index.name for index in pinecone_client.list_indexes()]:
            print(f"Creating new index: {index_name}")
            pinecone_client.create_index(
                name=index_name,
                dimension=dimension,
                metric="cosine",
                spec=ServerlessSpec(cloud="aws", region=PINECONE_ENVIRONMENT)
            )
            time.sleep(2)  # Wait briefly for the index to be available
        else:
            print(f"Index already exists on Pinecone: {index_name}")
        
        index_registry[document_path] = index_name

    return pinecone_client.Index(index_name)

def download_pdf(pdf_url):
    """Download PDF from a URL and save to a temporary file."""
    response = requests.get(pdf_url)
    if response.status_code == 200:
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
        with open(temp_file.name, 'wb') as f:
            f.write(response.content)
        return temp_file.name
    else:
        raise ValueError("Failed to download PDF. Check the URL.")

def extract_text_from_pdf(pdf_path):
    """Extract and clean text from each page of a PDF file."""
    text = ""
    with fitz.open(pdf_path) as pdf:
        for page_num in range(pdf.page_count):
            page_text = pdf[page_num].get_text()
            cleaned_text = clean_text(page_text)  # Clean each page's text
            text += cleaned_text + " "  # Add space between pages for readability
    return text.strip()

def chunk_data(text, chunk_size=600, chunk_overlap=50):
    """Split cleaned text into chunks with specified overlap."""
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=chunk_overlap)
    chunks = text_splitter.split_text(text)
    return [clean_text(chunk) for chunk in chunks]  

def store_embeddings_in_pinecone(chunks, chunk_embeddings, pinecone_index):
    """Store embeddings in Pinecone vector index with metadata."""
    data = [{"id": f"chunk-{i}", "values": embedding, "metadata": {"text": chunk}}
            for i, (embedding, chunk) in enumerate(zip(chunk_embeddings, chunks)) if embedding and chunk]
    
    if data:
        print(f"Upserting {len(data)} items to Pinecone...")
        response = pinecone_index.upsert(vectors=data)
        print("Upsert response:", response)

def clean_text(text):
    # Remove non-ASCII characters and replace multiple spaces or newlines with a single space
    text = re.sub(r'[^\x00-\x7F]+', ' ', text)  # Remove non-ASCII characters
    text = re.sub(r'\s+', ' ', text)  # Replace multiple whitespace with a single space
    return text.strip()

# Improved retrieve_answer function to return a coherent response
def retrieve_answer(query, pinecone_index, model_choice):
    """Retrieve answer based on query embedding."""
    model = initialize_embedding_model(model_choice)
    query_embedding = model.encode(query).tolist()
    
    # Query Pinecone for matches
    results = pinecone_index.query(vector=query_embedding, top_k=5, include_metadata=True)

    # Check and collect the results
    if results and "matches" in results:
        answer_text = ""
        for match in results["matches"]:
            match_text = match.get("metadata", {}).get("text", "").strip()
            
            # Check if match_text exists and is not empty
            if match_text:
                answer_text += match_text + "\n\n"
        
        # Return aggregated answer or fallback message
        return answer_text.strip() if answer_text else "No relevant answer found."
    else:
        return "No matches found in Pinecone index."

# Main RAG process function
def main_rag_process(pdf_link, query, model_choice='sentence-transformers'):
    """End-to-end Retrieval-Augmented Generation (RAG) process for PDF."""
    pdf_path = download_pdf(pdf_link)
    text = extract_text_from_pdf(pdf_path)
    chunks = chunk_data(text)
    chunk_embeddings = create_embeddings(chunks, model_choice)

    pinecone_index = get_or_create_pinecone_index(pdf_path)
    store_embeddings_in_pinecone(chunks, chunk_embeddings, pinecone_index)

    answer = retrieve_answer(query, pinecone_index, model_choice)
    os.remove(pdf_path)
    return answer