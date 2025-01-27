from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import requests
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
import boto3
from dotenv import load_dotenv
import time

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Initialize the S3 client using the credentials from the .env file
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# Ensure directories exist on local system
os.makedirs('images', exist_ok=True)
os.makedirs('pdfs', exist_ok=True)

# Configure local Selenium WebDriver
def create_webdriver():
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    service = Service("/usr/local/bin/chromedriver")  # Update to the correct chromedriver path
    logger.info("Creating Selenium WebDriver...")
    driver = webdriver.Chrome(service=service, options=chrome_options)
    logger.info("WebDriver created successfully!")
    return driver

# Helper function to find an element using multiple selectors (CSS or XPath as backup)
def find_element_with_fallback(driver, css_selectors, xpath=None):
    for selector in css_selectors:
        try:
            element = driver.find_element(By.CSS_SELECTOR, selector)
            return element
        except NoSuchElementException:
            logger.warning(f"Element with selector '{selector}' not found, trying next.")
    
    if xpath:
        try:
            element = driver.find_element(By.XPATH, xpath)
            return element
        except NoSuchElementException:
            logger.warning(f"Element with XPath '{xpath}' not found.")
    
    logger.error("Element not found using any provided selectors.")
    return None

# Main scraping function
def scrape_and_upload_to_s3():
    driver = create_webdriver()

    try:
        base_url = "https://rpc.cfainstitute.org/en/research-foundation/publications"
        driver.get(base_url)
        logger.info(f"Navigating to {base_url}")
        wait = WebDriverWait(driver, 10)
        
        # Process each page until no more pages are found
        for page_num in range(1, 11):  # Adjust number of pages as needed
            logger.info(f"Processing page {page_num}...")
            
            # Wait for the book elements on each page
            logger.info("Waiting for the book titles and images...")
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "h4.coveo-title a")))

            # Re-locate the books on each iteration
            books = driver.find_elements(By.CSS_SELECTOR, "div.coveo-result-frame")
            
            # Extract image, title, and PDF for each book on the page
            for index in range(len(books)):
                try:
                    # Re-locate the book elements again after each interaction to avoid stale references
                    books = driver.find_elements(By.CSS_SELECTOR, "div.coveo-result-frame")
                    book = books[index]
                    
                    # Extract image URL
                    image = find_element_with_fallback(
                        book, 
                        ["img", ".book-cover-image"],  # Add more CSS selectors if needed
                        xpath="//img[@class='book-cover-image']"
                    )
                    
                    if image:
                        image_url = image.get_attribute("src")
                        logger.info(f"Image URL ({index + 1}): {image_url}")
                        
                        # Download image
                        image_response = requests.get(image_url)
                        image_filename = image_url.split('/')[-1].split('?')[0]  # Adjust to take only the base filename
                        with open(os.path.join('images', image_filename), 'wb') as f:
                            f.write(image_response.content)
                        logger.info(f"Image {index + 1} downloaded successfully.")

                        # Upload image to S3
                        s3_key_image = f'images/{image_filename}'
                        s3.upload_file(os.path.join('images', image_filename), S3_BUCKET_NAME, s3_key_image)
                        logger.info(f"Successfully uploaded {s3_key_image} to S3 bucket {S3_BUCKET_NAME}.")
                    
                    # Extract book page link
                    book_page_link_element = find_element_with_fallback(
                        book,
                        ["h4.coveo-title a", ".book-link"],  # Add more CSS selectors if needed
                        xpath="//h4/a"
                    )
                    
                    if book_page_link_element:
                        book_page_link = book_page_link_element.get_attribute("href")
                        logger.info(f"Book Page Link ({index + 1}): {book_page_link}")
                        driver.get(book_page_link)
                        
                        # Extract the title from nested spans
                        pdf_title_element = driver.find_element(
                            By.CSS_SELECTOR,
                            "span.content-asset__title span.content-asset__emphasis"
                        )
                        pdf_title = pdf_title_element.text.strip().replace(" ", "_")
                        pdf_filename = f"{pdf_title}.pdf"
                        
                        # Extract the PDF link
                        pdf_link_element = find_element_with_fallback(
                            driver,
                            ["a.content-asset", ".pdf-download-link"],  # Add more CSS selectors if needed
                            xpath="//a[contains(@href, '.pdf')]"
                        )
                        
                        if pdf_link_element:
                            pdf_url = pdf_link_element.get_attribute("href")
                            logger.info(f"PDF URL ({index + 1}): {pdf_url}")
                            
                            # Download PDF with the title as filename
                            pdf_response = requests.get(pdf_url)
                            with open(os.path.join('pdfs', pdf_filename), 'wb') as f:
                                f.write(pdf_response.content)
                            logger.info(f"PDF {index + 1} downloaded successfully as {pdf_filename}.")

                            # Upload PDF to S3 with the new title-based filename
                            s3_key_pdf = f'pdfs/{pdf_filename}'
                            s3.upload_file(os.path.join('pdfs', pdf_filename), S3_BUCKET_NAME, s3_key_pdf)
                            logger.info(f"Successfully uploaded {s3_key_pdf} to S3 bucket {S3_BUCKET_NAME}.")
                            
                        driver.back()  # Navigate back to the main page after processing the book
                        logger.info(f"Navigated back to the main page after book {index + 1}.")
                        time.sleep(2)  # Short delay to allow the page to load after returning
                    else:
                        logger.error(f"Book Page Link not found for book {index + 1}. Skipping.")
                
                except (NoSuchElementException, StaleElementReferenceException) as e:
                    logger.error(f"Error processing book {index + 1}: {e}")
                    continue

            # Move to the next page using the pagination numbers
            try:
                # Find the page number button and click it
                pagination_buttons = driver.find_elements(By.CSS_SELECTOR, "li.coveo-pager-list-item a")
                if page_num < len(pagination_buttons):
                    next_page_button = pagination_buttons[page_num]
                    next_page_button.click()
                    logger.info(f"Navigating to page {page_num + 1}...")
                    time.sleep(3)  # Add delay to allow the next page to load
                else:
                    logger.info("No more pages found, stopping.")
                    break
            except NoSuchElementException:
                logger.info("No more pages found, stopping.")
                break

    except TimeoutException as e:
        logger.error(f"Timeout waiting for page elements: {e}")
    finally:
        driver.quit()
        logger.info("WebDriver session closed.")

# Airflow DAG configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'scrape_and_upload_to_s3_dag',
    default_args=default_args,
    description='DAG to scrape and upload data to S3',
    schedule_interval='@daily',
    start_date=datetime(2024, 10, 30),
    catchup=False,
) as dag:
    scrape_and_upload_task = PythonOperator(
        task_id='scrape_and_upload_to_s3',
        python_callable=scrape_and_upload_to_s3,
    )