import json

def main():
    print("\033[91mThis is red text\033[0m")
    print("\033[92mSuccess!\033[0m")
    print("\033[93mWarning!\033[0m")
    print("\033[94mInfo message\033[0m")
    with open("companies.json", "r") as f:
        companies = json.load(f)
    
    print(companies)

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import time
import os
import requests

# Setup Chrome WebDriver
options = webdriver.ChromeOptions()
options.add_argument("--headless")  # Run in background
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# Target URL
url = "https://www.ntsb.gov/investigations/AccidentReports/Pages/Reports.aspx?mode=Aviation"
driver.get(url)

# Wait for page to load
time.sleep(5)

# Create folder to store PDFs
os.makedirs("NTSB_Aviation_Reports", exist_ok=True)

# Find all PDF links
pdf_elements = driver.find_elements(By.XPATH, "//a[contains(@href, '.pdf')]")
pdf_links = [elem.get_attribute("href") for elem in pdf_elements]

# Download PDFs
for link in pdf_links:
    filename = link.split("/")[-1]
    filepath = os.path.join("NTSB_Aviation_Reports", filename)
    print(f"Downloading: {filename}")
    try:
        response = requests.get(link)
        with open(filepath, "wb") as f:
            f.write(response.content)
    except Exception as e:
        print(f"Failed to download {link}: {e}")

driver.quit()


if __name__ == "__main__":
    main()
