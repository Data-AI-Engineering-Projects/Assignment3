import streamlit as st
import requests
import snowflake.connector
from dotenv import load_dotenv
import os
from Api.rag_model import main_rag_process
from Api.summarizer_model import show_process_pdf_page
from Api.report_generator import report
import sys


project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# Load environment variables
load_dotenv()

# FastAPI endpoint URLs for user login and PDF list retrieval
FASTAPI_URL = os.getenv("FASTAPI_URL", "http://127.0.0.1:8000")
REGISTER_URL = f"{FASTAPI_URL}/auth/register"
LOGIN_URL = f"{FASTAPI_URL}/auth/login"

# Set up Streamlit page configuration with a wide layout
st.set_page_config(page_title="PDF Text Extraction Application", layout="wide")

# Initialize session state variables
if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False
if 'access_token' not in st.session_state:
    st.session_state['access_token'] = None
if 'pdf_data' not in st.session_state:
    st.session_state['pdf_data'] = []
if 'selected_pdf' not in st.session_state:
    st.session_state['selected_pdf'] = None
if 'view_mode' not in st.session_state:
    st.session_state['view_mode'] = 'list'  # default view is list

# Snowflake connection setup
def create_snowflake_connection():
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA")
        )
        return conn
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {e}")
        return None

# Function to fetch PDFs and their corresponding image links from Snowflake
def fetch_pdf_data_from_snowflake():
    conn = create_snowflake_connection()
    if not conn:
        return []
    
    cursor = conn.cursor()

    # Fetch title, image link, and PDF link from the Snowflake PUBLICATIONS table
    query = "SELECT title, brief_summary, image_link, pdf_link FROM PUBLIC.PUBLICATIONS"
    cursor.execute(query)
    result = cursor.fetchall()
    
    cursor.close()
    conn.close()

    return result  # Returns a list of tuples (title, brief_summary, image_link, pdf_link)

# Function to display PDF details
def display_pdf_details(pdf_data):
    pdf_name, brief_summary, image_link, pdf_link = pdf_data
    st.title(f"Details of {pdf_name}")
    st.image(image_link, width=400)
    st.write(f"**Summary**: {brief_summary}")
    st.markdown(f"[Open PDF]({pdf_link})", unsafe_allow_html=True)
    if 'selected_pdf' in st.session_state:
        pdf_link = st.session_state['selected_pdf'][3]  # Assumes PDF link is the fourth element
        show_process_pdf_page(pdf_link)
            
        user_query = st.text_input("Enter your question:")
        
        if st.button("Get Answer"):
            answer = main_rag_process(pdf_link, user_query)
            st.write("Answer:", answer)
        elif st.button("Generate Report"):
            report(pdf_link,user_query)
        else:
            if st.button("Back to Main"):
                st.session_state['selected_pdf'] = None
                st.rerun()

# Main Application
def main_app():
    # Custom CSS for orange buttons
    st.markdown("""
        <style>
        .stButton button {
            background-color: orange;
            color: white;
            border: none;
            padding: 8px 16px;
            border-radius: 5px;
            font-size: 14px;
            font-weight: bold;
        }
        .centered-title {
            font-size: 40px;
            font-weight: bold;
            text-align: center;
            border-bottom: 2px solid black;
            padding-bottom: 10px;
            margin-bottom: 30px;
        }
        </style>
    """, unsafe_allow_html=True)

    # Logout button on the upper-left corner
    st.sidebar.button("Logout", on_click=logout, help="Logout", key="logout_button")

    st.markdown("<h1 class='centered-title'>PDF Text Extraction Application</h1>", unsafe_allow_html=True)

    if st.session_state['selected_pdf']:
        display_pdf_details(st.session_state['selected_pdf'])
    else:
        # View mode selector
        view_mode = st.radio("Select view mode", ["List View", "Grid View"], index=0 if st.session_state['view_mode'] == 'list' else 1)

        # Update session state based on view mode
        if view_mode == "List View":
            st.session_state['view_mode'] = 'list'
        else:
            st.session_state['view_mode'] = 'grid'

        # Fetch PDF data from Snowflake if not already fetched
        if not st.session_state['pdf_data']:
            st.session_state['pdf_data'] = fetch_pdf_data_from_snowflake()

        # Display PDFs based on selected view mode
        if st.session_state['view_mode'] == 'list':
            display_pdfs_list_view()
        else:
            display_pdfs_grid_view()

# Function to display PDFs in list view
def display_pdfs_list_view():
    st.subheader("PDF Files (List View)")
    for i, pdf_data in enumerate(st.session_state['pdf_data']):
        pdf_name, brief_summary, image_link, pdf_link = pdf_data
        if st.button(f"{pdf_name}", key=f"list_{i}"):
            st.session_state['selected_pdf'] = pdf_data
            st.rerun()  # This will reload the app and navigate to the next page

# Function to display PDFs in grid view with hover effect and larger images
def display_pdfs_grid_view():
    st.subheader("PDF Files (Grid View)")

    # Custom CSS for hover effect and padding between columns
    st.markdown("""
        <style>
        .pdf-container {
            position: relative;
            width: 300px;
            height: 400px;
            margin: 20px;
        }
        .pdf-image {
            width: 100%;
            height: 100%;
            object-fit: cover;
            border-radius: 10px;
            transition: transform 0.3s ease;
        }
        .pdf-container:hover .pdf-image {
            transform: scale(1.05);
        }
        .pdf-details {
            position: absolute;
            bottom: 0;
            left: 0;
            right: 0;
            background-color: rgba(0, 0, 0, 0.7);
            color: white;
            padding: 10px;
            border-bottom-left-radius: 10px;
            border-bottom-right-radius: 10px;
            display: none;
        }
        .pdf-container:hover .pdf-details {
            display: block;
        }
        </style>
    """, unsafe_allow_html=True)

    # Adjusted columns to fit well with spacing
    cols = st.columns([1, 1, 1], gap="large")  # Adjusted column width and gap between columns
    for i, pdf_data in enumerate(st.session_state['pdf_data']):
        pdf_name, brief_summary, image_link, pdf_link = pdf_data
        with cols[i % 3]:
            # Display the PDF image with hover effect
            st.markdown(f"""
                <div class="pdf-container">
                    <img class="pdf-image" src="{image_link}" alt="{pdf_name}">
                    <div class="pdf-details">
                        <h4>{pdf_name}</h4>
                        <p>{brief_summary}</p>
                    </div>
                </div>
            """, unsafe_allow_html=True)

            if st.button(f"Open {pdf_name}", key=f"grid_{i}"):
                st.session_state['selected_pdf'] = pdf_data
                st.rerun()  # This will reload the app and navigate to the next page

# Logout function
def logout():
    st.session_state['logged_in'] = False
    st.session_state['access_token'] = None

# Login Page
def login_page():
    st.header("Login / Signup")  # Add a header for login/signup
    option = st.selectbox("Select Login or Signup", ("Login", "Signup"))

    if option == "Login":
        st.subheader("Login")
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        if st.button("Login"):
            login(username, password)

    elif option == "Signup":
        st.subheader("Signup")
        username = st.text_input("Username")
        email = st.text_input("Email")
        password = st.text_input("Password", type="password")
        if st.button("Signup"):
            signup(username, email, password)

# Signup function
def signup(username, email, password):
    response = requests.post(REGISTER_URL, json={
        "username": username,
        "email": email,
        "password": password
    })
    if response.status_code == 200:
        st.success("Account created successfully! Please login.")
    else:
        st.error(f"Signup failed: {response.json().get('detail', 'Unknown error occurred')}")

# Login function
def login(username, password):
    response = requests.post(LOGIN_URL, json={
        "username": username,
        "password": password
    })
    if response.status_code == 200:
        token_data = response.json()
        st.session_state['access_token'] = token_data['access_token']
        st.session_state['logged_in'] = True
        st.success("Logged in successfully!")
    else:
        st.error("Invalid username or password. Please try again.")

# Main Interface depending on login state
if st.session_state['logged_in']:
    main_app()
else:
    login_page()