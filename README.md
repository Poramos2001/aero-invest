<div align="center">

![AeroInvest Logo](AeroInvest_logo_resized.png)
</div>

# ETL Pipeline Project: AeroInvest

## Project Overview
This project is a simple **ETL (Extract, Transform, Load) Pipeline** implemented in Python. The pipeline is designed to:
#### 1. Extract
Retrieves raw data from multiple sources:
- **Financial data** from **Yahoo Finance** (primary) and **Finnhub** (backup) for key aerospace companies.  
- **Safety data** from the **NTSB**, including aviation accident and incident reports.  
- **Aviation operations data** such as airports and flight statistics from public **CSV datasets** and APIs.  



#### 2. Transform
Cleans and standardizes the data


#### 3. Load
Stores the processed data into a **PostgreSQL** database for analytics and visualization.  


This project is a simplified prototype of a potential data infrastructure for a startup idea — an application that aims to deliver financial and operational insights on the aerospace industry. 

---
## Project Structure
The project is organized as follows:
```
AeroInvest/
├── README.md                   # Project documentation and overview
├── pyproject.toml              # Project dependencies and build configuration
├── companies.json              # List of aerospace companies used for stock data extraction
├── main.py                     # Main ETL pipeline controller that orchestrates all stages
├── load.py                     # Loads cleaned and transformed data into PostgreSQL
│
├── NTSB_Aviation_Reports/      # Folder containing downloaded NTSB aviation reports (PDFs)
│
└── src/                        # Source code for data extraction and transformation
    ├── extract_flight_stats.py # Collects airport and flight statistics from online datasets
    ├── extract_reports.py      # Dynamically scrapes NTSB aviation reports using Selenium
    ├── extract_stock_data.py   # Fetches and preprocesses aerospace stock market data
    └── transform.py            # Cleans, normalizes, and integrates all extracted data
```

---

## Prerequisites
To run this ETL pipeline, you need to have the following installed on your system:
- Python 3.x
- Python package manager (Preferably uv, see more in Setup)
- Google Chrome

You will also need to install the required dependencies listed in the `pyproject.toml` file.

## Setup
### 1. Clone the repository
First, clone the repository to your local machine using the following code:
```bash
git clone https://github.com/Poramos2001/aero-invest.git
cd aero-invest
```
### 2. Create a virtual environment (optional but recommended)
Using `uv`, which is preferable (see 3.):

```
uv venv
```

Or directly with your python installation

```
python -m venv venv
```

Then, remember to activate it before installing the dependencies:

```
source venv/bin/activate  # For Linux/macOS
# or
venv\Scripts\activate  # For Windows

```
### 3. Install dependencies

#### 3.1 Python libraries

Using `uv` is highly recommendable once this project was created using it. To
install `uv` from `pip` is really straight forward:

```bash
pip install uv
```

Alternatively, you can look into the 
[uv documentation](https://docs.astral.sh/uv/getting-started/installation/)
to see how to install it through a standalone installer for your OS.

Then, to install all dependencies one must simply type:

```bash
uv sync
```

If you don't want to use uv, you can still download the required dependencies 
with your usual python package manager (pip, poetry, conda ...). For this, 
refer to the `dependencies` part of the `pyproject.toml` file to see what 
packages need to be downloaded.

#### 3.2 Google Chrome

The web scraping to download the accident and incident reports (see `src/extract_reports.py` for more information) uses `Selenium` because the NTSB website uses dynamic content.

In this project, the driver used was chrome, meaning that you will **need to have Google Chrome installed** to use ChromeDriver effectively.

If you don’t have Chrome yet:

```bash
sudo apt update
sudo apt install wget
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo apt install ./google-chrome-stable_current_amd64.deb
```
---

> If you are wondering why is Google chrome necessary for the drive, here is why:
> ChromeDriver is tightly coupled with Chrome
> - **ChromeDriver is a bridge** between Selenium and the Chrome browser.
> - It **controls a real Chrome browser instance** (even in headless mode).
> - It must **match the version** of Chrome installed on your system to work properly.
> 
> If Chrome is missing, ChromeDriver won’t have a browser to launch, and you’ll get errors like:

 ```
 selenium.common.exceptions.WebDriverException: Message: unknown error: cannot find Chrome binary
```

---

## Usage
To run the pipeline simply run the following command:
```python main.py```

