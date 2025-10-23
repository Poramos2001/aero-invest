from io import StringIO
import ssl, certifi
import pandas as pd
import requests
import os
from datetime import datetime
from dotenv import load_dotenv
import yfinance as yf
import finnhub
from bs4 import BeautifulSoup
import json


# Create SSL context that uses the certificate authority (CA) bundle
# This ensures that HTTPS requests trust certificates signed by recognized authorities
ssl_context = ssl.create_default_context(cafile=certifi.where())
# Overrides the default HTTPS context used by Python
ssl._create_default_https_context = lambda: ssl_context

# Load environment variables
load_dotenv()
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
ICAO_API_KEY = os.getenv("ICAO_API_KEY")
AMADEUS_API_KEY = os.getenv("AMADEUS_API_KEY")
AMADEUS_API_SECRET = os.getenv("AMADEUS_API_SECRET")

# Company List
with open("companies.json", "r") as f:
    companies = json.load(f)


def extract_yahoo():
    """
    Extract current stock information from Yahoo Finance.
    
    Attention: the function requires the companies names and tickers to be in 
    a `companies` variable.
    
    Returns:
        pandas.DataFrame: Company stock information for the selected aerospace companies
    """
    # ANSI coded for blue
    print("\n\n\033[94mFetching stock data from Yahoo Finance...\033[0m")

    data_list = []
    today = datetime.utcnow().replace(second=0, microsecond=0)

    for name, ticker in companies.items():
        try:
            tk=yf.Ticker(ticker)
            info = tk.info
            row = {
                "Symbol": info.get("symbol", None),
                "Name": name,
                "Previous Open": info.get("regularMarketOpen", None),
                "Previous Close": info.get("previousClose", None),
                "Daily % Change": info.get("regularMarketChangePercent", None),
                "Volume": info.get("volume", None),
                "Avg Volume (3M)": info.get("averageVolume", None),
                "Market Cap": info.get("marketCap", None),
                "P/E Ratio (TTM)": info.get("trailingPE", None),
                "EPS (TTM)": info.get("trailingEps", None),
                "52 Wk Change %": info.get("52WeekChange", None),
                "52 Wk Range": f"{info.get('fiftyTwoWeekLow', None)} - {info.get('fiftyTwoWeekHigh', None)}",
                "Dividend Yield": info.get("dividendYield", None),
                "Forward Dividend": info.get("dividendRate", None),
                "Next Earnings Date": info.get("earningsDate", None),
                "YTD Return": info.get("ytdReturn", None),
                "Fetched At": today
            }
            data_list.append(row)
            print(f"{name}({ticker}) fetched successfully.")

        except Exception as e:
            # \033[91m is the ANSI escape code for red
            print(f"\033[91m[ERROR] Yahoo fetch failed for {name} ({ticker}): {e}\033[0m")
    
    df=pd.DataFrame(data_list)
    print(df)
    return df


def extract_finnhub():
    """
    Extract current stock information from Finnhub.
    
    Attention: the function requires the companies names and tickers to be in 
    a `companies` variable.
    
    Returns:
        pandas.DataFrame: Company stock information for the selected aerospace companies
    """
    finnhub_client = finnhub.Client(api_key=FINNHUB_API_KEY)

    # ANSI coded for blue
    print("\n\n\033[94mFetching stock data from Finnhub API (free plan)...\033[0m")

    data_list = []
    today = datetime.utcnow().replace(second=0, microsecond=0)

    for name, symbol in companies.items():
        try:
            quote = finnhub_client.quote(symbol)
            profile = finnhub_client.company_profile2(symbol=symbol)
            metrics = finnhub_client.company_basic_financials(symbol, 'all')

            row = {
                "Symbol": symbol,
                "Name": name,
                "Previous Open": quote.get("o", None),
                "Previous Close": quote.get("pc", None),
                "Daily % Change": quote.get("dp", None),
                "Volume": quote.get("v", None),
                "Avg Volume (3M)": None,  # Not available in free plan
                "Market Cap": profile.get("marketCapitalization", None),
                "P/E Ratio (TTM)": metrics.get("metric", {}).get("peTTM", None),
                "EPS (TTM)": metrics.get("metric", {}).get("epsTTM", None),
                "52 Wk Change %": None,  # Not available in free plan
                "52 Wk Range": None,     # Not available in free plan
                "Dividend Yield": metrics.get("metric", {}).get("dividendYieldIndicatedAnnual", None),
                "Forward Dividend": None,  # Not available in free plan
                "Next Earnings Date": metrics.get("metric", {}).get("nextEarningsDate", None),
                "YTD Return": None,  # Not available in free plan
                "Fetched At": today
            }
            data_list.append(row)
            print(f"{name}({symbol}) fetched successfully.")

        except Exception as e:
            # \033[91m is the ANSI escape code for red
            print(f"\033[91m[ERROR] Finnhub fetch failed for {name} ({symbol}): {e}\033[0m")

    df = pd.DataFrame(data_list)
    print(df)
    
    return df   


def _missing_companies(companies, df):
    """Find the companies in `companies` that are not in the dataframe"""
    if not isinstance(companies, set):
        dict_tuples = set(companies.items())
    else:
        dict_tuples = companies
    df_tuples = set(zip(df["name"], df["symbol"]))

    # Find tuples in dict but not in DataFrame
    missing_tuples_set = dict_tuples - df_tuples
    return missing_tuples_set


def run_stock_extraction():
    """Run Yahoo/Finnhub stock extraction."""
    df = extract_yahoo()

    missing_companies_set = _missing_companies(companies, df)
    num_not_found = len(missing_companies_set)

    if num_not_found != 0:
        print(f"\033[93mYahoo failed to extract {num_not_found} companies.\033[0m")
        print("\033[91mSwitching to Finnhub.\033[0m")
        df2 = extract_finnhub()

        # Filter df2 to only include rows in missing_companies_set
        filtered_df2 = df2[df2.apply(lambda row: (row["name"], row["symbol"]) 
                                     in missing_companies_set, axis=1)]

        # Check if all stocks are now in the df
        missing_companies_set = _missing_companies(missing_companies_set, df2)
        num_not_found = len(missing_companies_set)

        if num_not_found != 0:
            print(f"\033[91m[ERROR] Extraction still failed for {num_not_found} stocks.\033[0m")
            print("The affected stocks are:")
            for name, symbol in missing_companies_set:
                print(f"\t{name} ({symbol})")
        else:
            print("\033[92mSuccessfully extracted the remaining companies.\033[0m")

        df = pd.concat([df, filtered_df2], ignore_index=True)

    df.to_csv(os.path.join(DATA_DIR, "stocks.csv"), index=False)
    print("Saved stock data → data/stocks.csv")
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
