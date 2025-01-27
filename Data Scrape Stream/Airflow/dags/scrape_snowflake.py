from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
import snowflake.connector
from dotenv import load_dotenv
import time

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Initialize Snowflake credentials
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_ROLE = os.getenv('SNOWFLAKE_ROLE')

PLACEHOLDER_IMAGE_URL = "https://upload.wikimedia.org/wikipedia/commons/a/ac/No_image_available.svg"

# Configure local Selenium WebDriver
def create_webdriver():
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    service = Service("/usr/local/bin/chromedriver")  # Update this path to your chromedriver path
    logger.info("Creating Selenium WebDriver...")
    driver = webdriver.Chrome(service=service, options=chrome_options)
    logger.info("WebDriver created successfully!")
    return driver

# Helper function to connect to Snowflake and insert data
def insert_into_snowflake(title, summary, image_link, pdf_link):
    conn = None
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            role=SNOWFLAKE_ROLE
        )
        cur = conn.cursor()

        # Define the insert query
        insert_query = """
            INSERT INTO PUBLIC.PUBLICATIONS (TITLE, BRIEF_SUMMARY, IMAGE_LINK, PDF_LINK)
            VALUES (%s, %s, %s, %s)
        """
        # Execute the query
        cur.execute(insert_query, (title, summary, image_link, pdf_link))
        conn.commit()
        logger.info(f"Data inserted into Snowflake for title: {title}")
    except Exception as e:
        logger.error(f"Failed to insert data into Snowflake: {e}")
    finally:
        if conn:
            conn.close()

# Helper function to find elements with fallback logic
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

# Function to scrape the title, summary, and image/PDF links, and insert into Snowflake
def scrape_and_upload_to_snowflake():
    driver = create_webdriver()
    try:
        base_url = "https://rpc.cfainstitute.org/en/research-foundation/publications"
        driver.get(base_url)
        logger.info(f"Navigating to {base_url}")
        wait = WebDriverWait(driver, 10)

        # Process each page (set how many pages to process)
        for page_num in range(1, 11):  # Adjust number of pages as needed (up to 10)
            logger.info(f"Processing page {page_num}...")

            # Wait for the books on the page
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "h4.coveo-title a")))

            # Re-fetch the book elements on every loop iteration
            book_elements = driver.find_elements(By.CSS_SELECTOR, "div.coveo-result-frame")

            for index in range(len(book_elements)):
                try:
                    # Re-fetch the book elements to avoid stale element reference
                    book_elements = driver.find_elements(By.CSS_SELECTOR, "div.coveo-result-frame")
                    book = book_elements[index]

                    # Extract the title
                    title_element = book.find_element(By.CSS_SELECTOR, "a.CoveoResultLink")
                    title = title_element.text.strip()
                    logger.info(f"Title ({index + 1}): {title}")

                    # Extract the brief summary
                    summary_element = book.find_element(By.CSS_SELECTOR, "div.result-body")
                    summary = summary_element.text.strip()
                    logger.info(f"Summary ({index + 1}): {summary}")

                    # Extract image URL
                    image = find_element_with_fallback(
                        book, 
                        ["img.coveo-result-image"],  # Correct CSS selector for image source
                        xpath="//img[@class='coveo-result-image']"
                    )
                    if image:
                        image_url = image.get_attribute("src")
                    else:
                        image_url = PLACEHOLDER_IMAGE_URL  # Use the placeholder if no image is found
                    logger.info(f"Image URL ({index + 1}): {image_url}")

                    # Extract book page link to get PDF link
                    book_page_link_element = find_element_with_fallback(
                        book,
                        ["h4.coveo-title a", ".book-link"],  # Add more CSS selectors if needed
                        xpath="//h4/a"
                    )

                    if book_page_link_element:
                        book_page_link = book_page_link_element.get_attribute("href")
                        logger.info(f"Book Page Link ({index + 1}): {book_page_link}")
                        driver.get(book_page_link)

                        # Extract the PDF link
                        pdf_link_element = find_element_with_fallback(
                            driver,
                            ["a.content-asset", ".pdf-download-link"],  # Add more CSS selectors if needed
                            xpath="//a[contains(@href, '.pdf')]"
                        )
                        if pdf_link_element:
                            pdf_url = pdf_link_element.get_attribute("href")
                            logger.info(f"PDF URL ({index + 1}): {pdf_url}")
                        
                        driver.back()  # Navigate back to the main page after processing the book
                        logger.info(f"Navigated back to the main page after book {index + 1}.")
                        time.sleep(2)  # Short delay to allow the page to load after returning

                        # Insert data into Snowflake
                        insert_into_snowflake(title, summary, image_url, pdf_url)

                    else:
                        logger.error(f"Book Page Link not found for book {index + 1}. Skipping.")
                
                except (NoSuchElementException, StaleElementReferenceException) as e:
                    logger.error(f"Error processing book {index + 1}: {e}")
                    continue

            # Move to the next page using the pagination numbers
            try:
                pagination_buttons = driver.find_elements(By.CSS_SELECTOR, "li.coveo-pager-list-item a")
                if page_num < len(pagination_buttons):
                    next_page_button = pagination_buttons[page_num]
                    next_page_button.click()
                    logger.info(f"Navigating to page {page_num + 1}...")
                    time.sleep(3)  # Wait for the next page to load
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

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'scrape_and_upload_to_snowflake_dag',
    default_args=default_args,
    description='Scrapes publication data and uploads it to Snowflake',
    schedule_interval='@daily',  # Run daily; adjust as needed
    start_date=datetime(2024, 10, 30),
    catchup=False,
) as dag:

    # Define the task
    scrape_and_upload_task = PythonOperator(
        task_id='scrape_and_upload_to_snowflake',
        python_callable=scrape_and_upload_to_snowflake,
    )