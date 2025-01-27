from fastapi import FastAPI, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
from fastapi.middleware.cors import CORSMiddleware
from .jwtauth import router as auth_router
from .pinecone_utils import store_embeddings
from dotenv import load_dotenv

# Initialize environment variables
load_dotenv()

app = FastAPI()

# Include the JWT auth router
app.include_router(auth_router, prefix="/auth")

# Define the OAuth2 scheme
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Add CORS middleware if needed
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"message": "Welcome to the FastAPI JWT Authentication Application!"}

@app.post("/process-pdf")
async def process_pdf_endpoint(pdf_link: str):
    # Use the extraction_utils function to process the PDF
    metadata, embeddings = process_pdf(pdf_link)
    # Store embeddings in Pinecone
    store_embeddings(metadata, embeddings)
    return {"message": "PDF processed successfully"}