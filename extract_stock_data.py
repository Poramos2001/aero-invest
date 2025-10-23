from io import StringIO
import ssl, certifi

# 🔒 Força o Python a usar os certificados corretos
ssl_context = ssl.create_default_context(cafile=certifi.where())
ssl._create_default_https_context = lambda: ssl_context

import pandas as pd
import requests
import time
import os
from datetime import datetime
from dotenv import load_dotenv
import yfinance as yf
import finnhub
from bs4 import BeautifulSoup

# Load environment variables

load_dotenv()
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
ICAO_API_KEY = os.getenv("ICAO_API_KEY")
AMADEUS_API_KEY = os.getenv("AMADEUS_API_KEY")
AMADEUS_API_SECRET = os.getenv("AMADEUS_API_SECRET")


# Company List
companies = {
     # Aerospace & Defense
    "Boeing": "BA",
    "Airbus SE": "AIR.PA",
    "Airbus ADR": "EADSY",
    "Lockheed Martin": "LMT",
    "Northrop Grumman": "NOC",
    "Raytheon Technologies (RTX Corp)": "RTX",
    "General Dynamics": "GD",
    "Textron": "TXT",
    "Spirit AeroSystems": "SPR",
    "Rolls-Royce Holdings": "RR.L",
    "Safran": "SAF.PA",
    "Leonardo S.p.A.": "LDO.MI",
    "Honeywell International": "HON",
    "Embraer S.A.": "ERJ",
    "Virgin Galactic Holdings": "SPCE",
    "Rocket Lab USA": "RKLB",
    "L3Harris Technologies": "LHX",

    # Air Carriers
    "Delta Air Lines": "DAL",
    "United Airlines Holdings": "UAL",
    "Southwest Airlines": "LUV",
    "American Airlines Group": "AAL",
    "Alaska Air Group": "ALK",

    # Private Space Company (no public ticker)
    "SpaceX (Private)": "SPAX.PVT"
    }


# Yahoo Finance Extractor
def extract_yahoo():
    """
    Extract current stock information from Yahoo Finance
    
    Returns:
        pandas.DataFrame: Company stock information for the selected aerospace companies
    """
    

    data_list = []
    print("📈 Fetching live flight data from API...")
    for name, ticker in companies.items():
        try:
            tk=yf.Ticker(ticker)
            info = tk.info
            row = {
                    "Symbol": info.get("symbol", "--"),
                    "Name": name,
                    "Price": info.get("regularMarketPrice", "--"),
                    "Change": info.get("regularMarketChange", "--"),
                    "Change %": info.get("regularMarketChangePercent", "--"),
                    "Volume": info.get("volume", "--"),
                    "Avg Vol (3M)": info.get("averageVolume", "--"),
                    "Market Cap": info.get("marketCap", "--"),
                    "P/E Ratio (TTM)": info.get("trailingPE", "--"),
                    "52 Wk Change %": info.get("52WeekChange", "--"),
                    "52 Wk Range": f"{info.get('fiftyTwoWeekLow', '--')} - {info.get('fiftyTwoWeekHigh', '--')}",
                    "Fetched At": datetime.utcnow()
                } 
            data_list.append(row)
            print(f"{name}({ticker}) fetched successfully.")
            time.sleep(0.5)

        except Exception as e:
            print(f"⚠️ Yahoo fetch failed for {name} ({ticker}): {e}")
    df=pd.DataFrame(data_list)
    print(df)
    return df

# Finnhub Extractor

def extract_finhub():
    finnhub_client = finnhub.Client(api_key="d3rl529r01qopgh8udh0d3rl529r01qopgh8udhg")


    data_list = []

    print("📈 Fetching live stock data from Finnhub API (free plan)...")

    for name, symbol in companies.items():
        try:
            quote = finnhub_client.quote(symbol)
            row = {
                "Symbol": symbol,
                "Name": name,
                "Price": quote.get("c", "--"),
                "Change": quote.get("c", 0) - quote.get("pc", 0),
                "Change %": ((quote.get("c", 0) - quote.get("pc", 0)) / quote.get("pc", 1)) * 100 if quote.get("pc") else "--",
                "Volume": "--", # Not available on free plan
                "Avg Vol (3M)": "--",       # Not available on free plan
                "Market Cap": "--",          # Not available on free plan
                "P/E Ratio (TTM)": "--",    # Not available on free plan
                "52 Wk Change %": "--",      # Not available on free plan
                "52 Wk Range": "--",          # Not available on free plan
                "Fetched At": datetime.utcnow()

            }
            data_list.append(row)
            print(f"{name}({symbol}) fetched successfully.")
            time.sleep(0.5)
        except Exception as e:
            print(f"⚠️ Error fetching {name} ({symbol}): {e}")

    df = pd.DataFrame(data_list)
    print(df)

    return df

def run_stock_extraction():
    """Run Yahoo/Finnhub stock extraction."""
    df = extract_yahoo()
    if df.empty:
        print("⚠️ Yahoo failed — switching to Finnhub.")
        df = extract_finhub()
    df.to_csv(os.path.join(DATA_DIR, "stocks.csv"), index=False)
    print("💾 Saved stock data → data/stocks.csv")
    return df

# Airports list extractor

def extract_airports():
    #Fetch all airports from OurAirports open data.
    print("🛫 Fetching all airports from OurAirports open dataset...")
    url = "https://ourairports.com/data/airports.csv"

    import ssl
    ssl._create_default_https_context = ssl._create_unverified_context

    try:
        df = pd.read_csv(url)
        df["Fetched At"] = datetime.utcnow()
        df.to_csv(os.path.join(DATA_DIR, "airports.csv"), index=False)
        print(f"✅ Airports dataset fetched successfully ({len(df)} records).")
        print("💾 Saved airports → data/airports.csv")
        return df
    except Exception as e:
        print(f"⚠️ Failed to fetch airports: {e}")
        return pd.DataFrame()

# Air traffic statistics from USA

def extract_transtats():
    """
    📊 Extract monthly air traffic statistics from the TranStats (BTS) portal.
    The data includes domestic and international passenger volumes by month and year.
    """

    print("🌐 Fetching air traffic data from TranStats (Bureau of Transportation Statistics)...")

    # TranStats URL and parameters
    url = "https://www.transtats.bts.gov/Data_Elements.aspx"
    params = {"Data": "1"}  # Example dataset ID (air traffic summary)

    try:
        # Start session
        session = requests.Session()

        # Access the initial form page
        resp = session.get(url, params=params)
        soup = BeautifulSoup(resp.text, "html.parser")

        # Capture hidden ASP.NET form fields
        data = {i.get("name"): i.get("value", "") for i in soup.find_all("input", type="hidden")}

        # Add query parameters (example filter)
        data.update({
            "AirportList": "All",
            "CarrierList": "All",
            "Submit": "Submit"
        })

        # Submit form (POST request)
        resp2 = session.post(url, params=params, data=data)

        # Extract HTML table(s)
        dfs = pd.read_html(StringIO(resp2.text))
        df = dfs[-1]
        df["Fetched At"] = datetime.utcnow()

        # Save to CSV
        output_path = os.path.join(DATA_DIR, "transtats_air_traffic.csv")
        df.to_csv(output_path, index=False)

        print(f"✅ TranStats data fetched successfully ({len(df)} records).")
        print(f"💾 Saved dataset → {output_path}")

        return df

    except Exception as e:
        print(f"⚠️ Failed to fetch TranStats data: {e}")
        return pd.DataFrame()

# Amadeus endpoints
AMADEUS_TOKEN_URL = "https://test.api.amadeus.com/v1/security/oauth2/token"
AMADEUS_FLIGHTS_URL = "https://test.api.amadeus.com/v2/shopping/flight-offers"  

# Obtain API token
def get_amadeus_token():
    try:
        res = requests.post(
            AMADEUS_TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": AMADEUS_API_KEY,
                "client_secret": AMADEUS_API_SECRET,
            },
        )
        res.raise_for_status()
        token = res.json().get("access_token")
        print("✅ Amadeus token obtained.")
        return token
    except Exception as e:
        print(f"⚠️ Amadeus token request failed: {e}")
        return None

# Extract flight offers
def extract_flight_offers(origin, destination, date):
    token = get_amadeus_token()
    if not token:
        return pd.DataFrame()

    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "originLocationCode": origin,
        "destinationLocationCode": destination,
        "departureDate": date,
        "adults": 1,
        "currencyCode": "EUR",
        "max": 20,  # limit to 20 offers
    }

    try:
        res = requests.get(AMADEUS_FLIGHTS_URL, headers=headers, params=params)
        res.raise_for_status()
        data = res.json()

        flights = []
        for offer in data.get("data", []):
            price = offer.get("price", {}).get("total")
            for itinerary in offer.get("itineraries", []):
                for segment in itinerary.get("segments", []):
                    flights.append({
                        "origin": segment.get("departure", {}).get("iataCode"),
                        "destination": segment.get("arrival", {}).get("iataCode"),
                        "departure": segment.get("departure", {}).get("at"),
                        "arrival": segment.get("arrival", {}).get("at"),
                        "carrier_code": segment.get("carrierCode"),
                        "flight_number": segment.get("number"),
                        "duration": segment.get("duration"),
                        "price_EUR": price,
                        "Fetched At": datetime.utcnow(),
                    })

        df = pd.DataFrame(flights)
        if not df.empty:
            name = f"flights_{origin}_{destination}.csv"
            df.to_csv(os.path.join(DATA_DIR, name), index=False)
            print(f"💾 Saved Amadeus flight offers → {name} ({len(df)} records)")
        else:
            print(f"⚠️ No flight offers found for {origin}-{destination} on {date}")
        return df

    except Exception as e:
        print(f"⚠️ Amadeus flight fetch failed: {e}")
        return pd.DataFrame()

# Run extraction for multiple routes
def run_amadeus_extraction():
    print("✈️ Starting Amadeus extraction...")
    routes = [("CDG", "JFK", "2025-10-25"), ("LHR", "LAX", "2025-10-26")]
    for origin, dest, date in routes:
        extract_flight_offers(origin, dest, date)
    print("✅ Amadeus extraction complete.")


def run_all_extractions():
    """Runs all extractors and saves outputs to /data."""
    print("\n🚀 Starting Global Extraction Pipeline...\n")
    run_stock_extraction()
    extract_airports()
    extract_transtats()
    run_amadeus_extraction()
    print("\n✅ All data sources extracted successfully.\n")


if __name__ == "__main__":
     run_all_extractions()