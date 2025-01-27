import nest_asyncio
import os
import re
import requests
import streamlit as st
from llama_index.core import Settings, StorageContext, SummaryIndex, load_index_from_storage
from llama_index.llms.openai import OpenAI
from llama_index.embeddings.openai import OpenAIEmbedding
from llama_parse import LlamaParse
from pathlib import Path
from typing import List
from pydantic import BaseModel, Field
from IPython.display import display, Markdown, Image
from dotenv import load_dotenv

# Enable asyncio
nest_asyncio.apply()

# Load environment variables
load_dotenv()

# Set up OpenAI and Arize Phoenix API keys
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
PHOENIX_API_KEY = os.getenv("PHOENIX_API_KEY")
os.environ["OTEL_EXPORTER_OTLP_HEADERS"] = f"api_key={PHOENIX_API_KEY}"

# Configure OpenAI models
Settings.embed_model = OpenAIEmbedding(model="text-embedding-3-large")
Settings.llm = OpenAI(model="gpt-4o")
def download_pdf(pdf_url):
    """Download PDF from a URL and save to a temporary file."""
    response = requests.get(pdf_url)
    if response.status_code == 200:
        # Create a temporary file to store the PDF
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
        with open(temp_file.name, 'wb') as f:
            f.write(response.content)
        return temp_file.name  # Return the path to the temporary file
    else:
        raise ValueError("Failed to download PDF. Check the URL.")

def parse_pdf(pdf_url):
    """Download and parse PDF to get text and image data."""
    # Download the PDF to a temporary file
    pdf_path = download_pdf(pdf_url)
    try:
        md_json_list = []
        image_dir = tempfile.mkdtemp()  # Create a temporary directory for images

        with fitz.open(pdf_path) as pdf:
            if pdf.page_count == 0:
                raise ValueError("PDF has no pages.")
            
            # Parse each page (assuming you use LlamaParse or other libraries)
            for page_num in range(pdf.page_count):
                page = pdf[page_num]
                text = page.get_text()
                md_json_list.append({"page": page_num + 1, "text": text})
                
                # You could also save images from each page if required
                for img_index, img in enumerate(page.get_images(full=True)):
                    xref = img[0]
                    image = pdf.extract_image(xref)
                    image_data = image["image"]
                    image_path = f"{image_dir}/page_{page_num + 1}_img_{img_index}.png"
                    with open(image_path, "wb") as img_file:
                        img_file.write(image_data)

        return md_json_list, image_dir
    finally:
        # Clean up by deleting the downloaded PDF file
        os.remove(pdf_path)
# Initialize PDF Parser
def parse_pdf(pdf_url, data_dir="data", image_dir="data_images"):
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(image_dir, exist_ok=True)
    pdf_path = os.path.join(data_dir, "document.pdf")
    
    # Download PDF if not already downloaded
    if not os.path.exists(pdf_path):
        response = requests.get(pdf_url)
        with open(pdf_path, "wb") as file:
            file.write(response.content)
    
    # Parse PDF to extract text and images
def parse_pdf(pdf_url):
    try:
        parser = LlamaParse(
            api_key=os.getenv("PHOENIX_API_KEY"),
            result_type="markdown",
            use_vendor_multimodal_model=True,
            vendor_multimodal_model_name="anthropic-sonnet-3.5",
        )
        md_json_objs = parser.get_json_result(pdf_url)
        
        # Check if parsing was successful
        if not md_json_objs or not md_json_objs[0].get("pages"):
            print("Parsing failed or no pages found.")
            return [], "data_images"  # Return empty list and default image directory

        # Extract pages and image directory
        md_json_list = md_json_objs[0]["pages"]
        image_dir = "data_images"  # Directory where images are saved

        return md_json_list, image_dir
    
    except Exception as e:
        print(f"Error during PDF parsing: {e}")
        return [], "data_images"

# Define data model for structured report output
class TextBlock(BaseModel):
    text: str = Field(..., description="The text for this block.")

class ImageBlock(BaseModel):
    file_path: str = Field(..., description="File path to the image.")

class ReportOutput(BaseModel):
    blocks: List[TextBlock | ImageBlock] = Field(
        ..., description="A list of text and image blocks."
    )
    def render(self):
        for b in self.blocks:
            if isinstance(b, TextBlock):
                display(Markdown(b.text))
            else:
                display(Image(filename=b.file_path))

# Helper functions to create report nodes with metadata
def get_page_number(file_name):
    match = re.search(r"-page-(\d+)\.jpg$", str(file_name))
    return int(match.group(1)) if match else 0

def _get_sorted_image_files(image_dir):
    raw_files = [f for f in list(Path(image_dir).iterdir()) if f.is_file()]
    return sorted(raw_files, key=get_page_number)

def get_text_nodes(json_dicts, image_dir=None):
    nodes = []
    image_files = _get_sorted_image_files(image_dir) if image_dir is not None else None
    md_texts = [d["md"] for d in json_dicts]

    for idx, md_text in enumerate(md_texts):
        chunk_metadata = {"page_num": idx + 1}
        if image_files and idx < len(image_files):
            chunk_metadata["image_path"] = str(image_files[idx])
        chunk_metadata["parsed_text_markdown"] = md_text
        node = TextNode(text="", metadata=chunk_metadata)
        nodes.append(node)

    return nodes

# Set up and build the report index
def setup_summary_index(text_nodes):
    if not os.path.exists("storage_nodes_summary"):
        index = SummaryIndex(text_nodes)
        index.set_index_id("summary_index")
        index.storage_context.persist("./storage_nodes_summary")
    else:
        storage_context = StorageContext.from_defaults(persist_dir="storage_nodes_summary")
        index = load_index_from_storage(storage_context, index_id="summary_index")
    return index

# Build query engine for structured report generation
def build_query_engine(index):
    system_prompt = """
    You are a report generation assistant tasked with producing a well-formatted report.
    Generate a report with interleaving text and images using parsed document content.
    Include at least one image in the output.
    """
    llm = OpenAI(model="gpt-4o", system_prompt=system_prompt)
    sllm = llm.as_structured_llm(output_cls=ReportOutput)

    query_engine = index.as_query_engine(
        similarity_top_k=10,
        llm=sllm,
        response_mode="compact"  # Ensure correct response mode
    )
    return query_engine

# Generate a report based on a specific query
def generate_report(query_engine, query):
    response = query_engine.query(query)
    
    # Check if response.response has a render method
    if hasattr(response.response, "render"):
        # If response is structured, render it
        response.response.render()
    else:
        # If response is plain text, display it directly
        st.write(response.response)

# Main function to execute the pipeline
def report(pdf_url, query):
    # Parse PDF and load images
    md_json_list, image_dir = parse_pdf(pdf_url)
    
    # Generate text nodes with metadata
    text_nodes = get_text_nodes(md_json_list, image_dir)
    
    # Setup summary index
    index = setup_summary_index(text_nodes)
    
    # Build query engine and generate report
    query_engine = build_query_engine(index)
    generate_report(query_engine, query)

