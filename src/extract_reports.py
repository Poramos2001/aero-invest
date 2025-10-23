import requests
from bs4 import BeautifulSoup
from pathlib import Path
from urllib.parse import urljoin
import time


def _get_request(url, max_retries = 3, delay = 5):
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

    print("\n\n\033[94mFetching accident reports from NTSB...\033[0m")
    # Base URL of the NTSB aviation reports page
    base_url = "https://www.ntsb.gov/investigations/AccidentReports/Pages/Reports.aspx?mode=Aviation"

    # Create a folder to store PDFs
    parent_dir = Path(__file__).parent.parent
    report_folder = parent_dir / "NTSB_Aviation_Reports" # "/" operator of the pathlib module
    report_folder.mkdir(exist_ok=True)

    # Get the HTML content of the page
    response = _get_request(base_url)
    if response is None:
        print("Could not download pdfs because the HTTP get request did not come through.")
        return
    
    soup = BeautifulSoup(response.content, "html.parser")

    # Find all links that point to PDFs
    print("Finding PDF links...")
    pdf_links = []
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if href.lower().endswith(".pdf"):
            full_url = urljoin("https://www.ntsb.gov", href)
            pdf_links.append(full_url)
    
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
        pdf_response = _get_request(pdf_url)
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
    base_url = "https://www.ntsb.gov/investigations/AccidentReports/Pages/Reports.aspx?mode=Aviation"

    # Get the HTML content of the page
    response = _get_request(base_url)
    with open("HTML.txt", "w") as f:
        f.write(response.text)