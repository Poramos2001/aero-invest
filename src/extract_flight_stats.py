from bs4 import BeautifulSoup
from io import StringIO
import ssl, certifi
import requests
from datetime import datetime
from dotenv import load_dotenv
import os
import pandas as pd


# Create SSL context that uses the certificate authority (CA) bundle
# This ensures that HTTPS requests trust certificates signed by recognized authorities
ssl_context = ssl.create_default_context(cafile=certifi.where())
# Overrides the default HTTPS context used by Python
ssl._create_default_https_context = lambda: ssl_context

# Load environment variables
load_dotenv()

ICAO_API_KEY = os.getenv("ICAO_API_KEY")
AMADEUS_API_KEY = os.getenv("AMADEUS_API_KEY")
AMADEUS_API_SECRET = os.getenv("AMADEUS_API_SECRET")

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

        print(f"✅ TranStats data fetched successfully ({len(df)} records).")
        print("Returned dataset")

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
            print(f"💾 Fetched Amadeus flight offers  ({len(df)} records)")
        else:
            print(f"⚠️ No flight offers found for {origin}-{destination} on {date}")
        return df

    except Exception as e:
        print(f"⚠️ Amadeus flight fetch failed: {e}")
        return pd.DataFrame()

# Run extraction for multiple routes
def run_amadeus_extraction():
    df = pd.DataFrame(columns=["origin","destination","departure", "arrival",
                               "carrier_code", "flight_number", "duration",
                               "price_EUR", "Fetched At"])
    
    print("✈️ Starting Amadeus extraction...")
    routes = [("CDG", "JFK", "2025-10-25"), ("LHR", "LAX", "2025-10-26")]
    for origin, dest, date in routes:
        df_aux = extract_flight_offers(origin, dest, date)
        df = pd.concat([df, df_aux], ignore_index=True)
    
    print("✅ Amadeus extraction complete.")
    return df
