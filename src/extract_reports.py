import requests
from bs4 import BeautifulSoup
from pathlib import Path
from urllib.parse import urljoin
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def _static_get_request(url, max_retries = 3, delay = 5):
    """
    Sends a GET request to the specified URL with retry logic for server-side errors.

    Parameters:
    ----------
    url : str
        The URL to send the GET request to.
    max_retries : int, optional
        Maximum number of retry attempts for server-side errors (default is 3).
    delay : int or float, optional
        Delay in seconds between retries (default is 5).

    Returns:
    -------
    requests.Response or None
        The response object if the request is successful.
        Returns None if the request fails due to client-side errors, request exceptions,
        or if all retry attempts for server-side errors fail.
    """
    
    print("\tSending a get request to the server...")
    for attempt in range(max_retries):
        try:
            response = requests.get(url)
            status = response.status_code
            response.raise_for_status()  # Raise any HTTP errors

            print("\tRequest successful!")
            return response

        except requests.exceptions.HTTPError as e:
            # Retry only with server-side errors
            is_server_error = (500 <= status < 600)

            if not is_server_error:
                print(f"\033[91mAttempt {attempt + 1} failed with HTTP error: {e}\033[0m")
                return None
            elif attempt < max_retries - 1:
                print(f"\033[93mAttempt {attempt + 1} failed with server-side error error:\033[0m")
                print(e)
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print(f"\033[93mAttempt {attempt + 1} failed with server-side error error:\033[0m")
                print(e)
                print("\033[91mAll retries failed due to server errors.\033[0m")
                return None

        except requests.exceptions.RequestException as e:
            # Catch other request-related issues (e.g., DNS failure, timeout)
            print("\033[91mRequest failed due to GET request exception:\033[0m")
            print(e)
            return None


def _wait_for_all_pdfs(driver, timeout=20, check_interval=2):
    end_time = time.time() + timeout
    previous_count = 0

    while time.time() < end_time:
        pdf_elements = driver.find_elements(By.XPATH, "//a[contains(@href, '.pdf')]")
        current_count = len(pdf_elements)
        if current_count == previous_count and current_count > 0:
            return pdf_elements
        previous_count = current_count
        time.sleep(check_interval)
    return pdf_elements  # Return whatever was found


def web_scrap_reports():
    """
    Scrapes aviation accident report PDFs from the National Transportation 
    Safety Board (U.S.) website and saves them locally.

    Notes:
    -------
    This function:
    1. Creates a local directory named 'NTSB_Aviation_Reports' (if it doesn't exist) to store the reports.
    2. Downloads each PDF file unless it has already been downloaded.
    """
    # Create a folder to store PDFs
    parent_dir = Path(__file__).parent.parent
    report_folder = parent_dir / "NTSB_Aviation_Reports" # "/" operator of the pathlib module
    report_folder.mkdir(exist_ok=True)

    print("\n\n\033[94mFetching accident reports from NTSB...\033[0m")
    # Base URL of the NTSB aviation reports page
    base_url = "https://www.ntsb.gov/investigations/AccidentReports/Pages/Reports.aspx?mode=Aviation"

    # Check connection with website (through HTTP status)
    response = _static_get_request(base_url)
    if response is None:
        print("Could not download pdfs because the HTTP get request did not come through.")
        return

    # Setup Chrome WebDriver
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # Run in background
    service = Service(ChromeDriverManager().install()) # Handles driver download and installation
    driver = webdriver.Chrome(service=service, options=options)

    driver.get(base_url)

    # Find all links that point to PDFs
    print("Finding report PDF links...")
    pdf_elements = _wait_for_all_pdfs(driver) # Wait for JavaScript to load content and extract it
    pdf_links = [elem.get_attribute("href") for elem in pdf_elements]

    if len(pdf_links) == 0:
        print(f"\033[91mNo PDFs were found in:\033[0m {base_url}")
        return   
    



    # TODO: get also report date, number and title




    # Download each PDF
    print("Downloading PDFs...")
    for pdf_url in pdf_links:
        filename = pdf_url.split("/")[-1]
        filepath = report_folder / filename
        if filepath.exists():
                print(f"Already downloaded: {filename}")
                continue 
        
        print(f"Downloading: {filename}")
        pdf_response = _static_get_request(pdf_url)
        if pdf_response is None:
            print(f"\033[91mFailed to fetch {pdf_url}:\033[0m")
            continue

        try:
            with open(filepath, "wb") as f:
                f.write(pdf_response.content)
        except Exception as e:
            print(f"\033[91mFailed to save {filename}:\033[0m")
            print(f"\t{e}")


if __name__ == "__main__":
    web_scrap_reports()