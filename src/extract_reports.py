import requests
from pathlib import Path
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd


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


def _wait_for_all_blocks(driver, timeout=20, check_interval=2):
    """
    Waits for the list of report blocks on the NTSB aviation reports page to stabilize,
    indicating that all dynamically loaded content has likely finished rendering.

    This function repeatedly checks the number of <div class="block"> elements inside
    the <div id="investigation_reports"> container. It returns the list of elements
    once the count stops changing between checks, or when the timeout is reached.

    Parameters:
    ----------
    driver : selenium.webdriver.Chrome
        The active Selenium WebDriver instance controlling the browser.
    timeout : int, optional
        Maximum time to wait (in seconds) before returning the current list of blocks.
        Default is 20 seconds.
    check_interval : int, optional
        Time interval (in seconds) between consecutive checks. Default is 2 seconds.

    Returns:
    -------
    list of selenium.webdriver.remote.webelement.WebElement
        A list of <div class="block"> elements found inside the investigation_reports container.
        May be incomplete if the timeout is reached before stabilization.
    """
    end_time = time.time() + timeout
    previous_count = 0

    while time.time() < end_time:
        block_elements = driver.find_elements(By.XPATH, "//div[@id='investigation_reports']//div[contains(@class, 'block')]")
        current_count = len(block_elements)
        if current_count == previous_count and current_count > 0:
            return block_elements
        previous_count = current_count
        time.sleep(check_interval)
    return block_elements  # Return whatever was found


def web_scrap_reports():
    """
    Scrapes aviation accident reports from the National Transportation 
    Safety Board (U.S.) website and returns them as a dataframe after saving 
    each reports PDF locally.

    Returns:
    -------
        pandas.DataFrame: All the reports information available on the website.

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
    print("Setting chrome driver up...")
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # Run in background
    service = Service(ChromeDriverManager().install()) # Handles driver download and installation
    driver = webdriver.Chrome(service=service, options=options)

    print("Opening the URL in the browser...")
    driver.get(base_url)

    # Find all blocks that point to reports
    print("Finding report links...")
    report_blocks = _wait_for_all_blocks(driver) # Wait for JavaScript to load content and extract it
    
    if len(report_blocks) == 0:
        print(f"\033[91mNo reports were found in:\033[0m {base_url}")
        return   

    # Extract and print the data
    print("\n\n\033[94mFound accident reports:\033[0m")
    print("-" * 40)

    data = []
    pdf_links = []
    for i in range(len(report_blocks)):
        try:
            # Re-fetch the block to avoid staleness
            block = driver.find_elements(By.XPATH, "//div[@id='investigation_reports']//div[contains(@class, 'block')]")[i]

            # Extract data from this block
            pdf_link = block.find_element(By.XPATH, ".//div[@class='download']//a[contains(@href, '.pdf')]").get_attribute("href")
            title = block.find_element(By.XPATH, ".//div[@class='desc']//a").text
            location = block.find_element(By.XPATH, ".//p[@class='location']").text
            dates_text = block.find_element(By.XPATH, ".//p[@class='data']").text
            report_number = block.find_element(By.XPATH, ".//p[@class='report']").text

            print(f"Title: {title}")
            print(f"PDF: {pdf_link}")
            print(f"Location: {location}")
            print(dates_text)
            print(report_number)
            print("-" * 40)

            # Clean the necessary strings
            lines = dates_text.split('\n')
            accident_date = lines[0].replace("Accident Date: ", "").strip()
            report_date = lines[1].replace("Report Date: ", "").strip()

            report_number = report_number.replace("Report Number: ", "").strip()

            pdf_name = pdf_link.split("/")[-1]

            data.append({
                "Title": title,
                "PDF name": pdf_name,
                "Location": location,
                "Accident Date": accident_date,
                "Report Date": report_date,
                "Report Number": report_number
            })

            pdf_links.append(pdf_link)

        except Exception as e:
            print(f"\033[91mError processing block {i+1}:\033[0m\n {e}")

    driver.quit()

    df = pd.DataFrame(data)
    
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
    
    return df


if __name__ == "__main__":
    web_scrap_reports()