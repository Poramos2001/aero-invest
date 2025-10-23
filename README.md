# ETL Pipeline Project: AeroInvest

This project is a simple **ETL (Extract, Transform, Load) Pipeline** implemented in Python. The pipeline is designed to:
### 1. Extract
Retrieves raw data from multiple sources:
- **Financial data** from **Yahoo Finance** (primary) and **Finnhub** (backup) for key aerospace companies.  
- **Safety data** from the **NTSB**, including aviation accident and incident reports.  
- **Aviation operations data** such as airports and flight statistics from public **CSV datasets** and APIs.  



### 2. Transform
Cleans and standardizes the data


### 3. Load
Stores the processed data into a **PostgreSQL** database for analytics and visualization.  

This project is a simplified prototype of a potential data infrastructure for a startup idea — an application that aims to deliver financial and operational insights on the aerospace industry. 



---
Link to the ASN [Database standards](https://asn.flightsafety.org/about/ASN-standards.pdf)

To set up **ChromeDriver** on Ubuntu, follow these steps:


### 🔍 Why you need Google Chrome to use ChromeDriver

**Yes**, you do need to have **Google Chrome installed** on your system to use **ChromeDriver** effectively. Here's why:

---

### ✅ ChromeDriver is tightly coupled with Chrome

- **ChromeDriver is a bridge** between Selenium and the Chrome browser.
- It **controls a real Chrome browser instance** (even in headless mode).
- It must **match the version** of Chrome installed on your system to work properly.

If Chrome is missing, ChromeDriver won’t have a browser to launch, and you’ll get errors like:

```
selenium.common.exceptions.WebDriverException: Message: unknown error: cannot find Chrome binary
```

---


### ✅ **Step 1: Install Google Chrome (if not already installed)**

If you don’t have Chrome yet:

```bash
sudo apt update
sudo apt install wget
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo apt install ./google-chrome-stable_current_amd64.deb
```
