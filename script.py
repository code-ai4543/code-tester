import os
import time
import logging
from datetime import datetime
import requests
import pandas as pd

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK")
TRACKED_SYMBOLS = [
    "RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK",
    "TATAMOTORS", "SBIN", "BHARTIARTL", "ITC", "HINDUNILVR",
    "LTIM", "AXISBANK", "KOTAKBANK", "M&M", "MARUTI",
    "TATASTEEL", "NTPC", "POWERGRID", "SUNPHARMA", "ASIANPAINT",
    "TITAN", "ULTRACEMCO", "WIPRO", "HCLTECH", "ONGC",
    "JIOFIN", "ADANIENT", "COALINDIA", "BAJAJFINSV", "NIFTYBEES"
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def fetch_symbol_data(session, symbol, headers):
    api_url = "https://www.nseindia.com/api/corporate-announcements"
    query_params = {"index": "equities", "symbol": symbol}
    try:
        response = session.get(api_url, headers=headers, params=query_params, timeout=8)
        if response.status_code == 200:
            return response.json()
        else:
            logging.warning(f"Non-200 response for {symbol}: {response.status_code}")
            return None
    except Exception as e:
        logging.error(f"Network request failed for {symbol}: {e}")
        return None

def process_and_upload():
    if not DISCORD_WEBHOOK_URL:
        logging.error("Missing DISCORD_WEBHOOK environment variable.")
        return
    
    logging.info("Pulling market snapshot...")
    all_records = []
    base_url = "https://www.nseindia.com"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-announcements"
    }
    
    try:
        session = requests.Session()
        session.get(base_url, headers=headers, timeout=10)
        time.sleep(1)
    except Exception as session_err:
        logging.error(f"Session initialization failed completely: {session_err}")
        return

    # Process each symbol safely
    for symbol in TRACKED_SYMBOLS:
        logging.info(f"Pulling data for structure: {symbol}")
        raw_data = fetch_symbol_data(session, symbol, headers)
        has_announcements = False
        
        # If the request worked and returned data
        if raw_data is not None and isinstance(raw_data, list) and len(raw_data) > 0:
            for item in raw_data:
                if isinstance(item, dict) and item.get("desc"):
                    has_announcements = True
                    comp_name = item.get("sm_name", "N/A")
                    date_time = item.get("an_dt", "N/A")
                    subject = item.get("desc", "None")
                    details = item.get("attchmntText", "None")
                    file_pdf = item.get("attchmntFile", "")
                    pdf_link = f"https://nsearchives.nseindia.com/corporate/{file_pdf}" if file_pdf else "None"
                    
                    all_records.append({
                        "Symbol": symbol,
                        "Company Name": comp_name,
                        "Broadcast Date/Time": date_time,
                        "Subject": subject,
                        "Details": details,
                        "Attachment Link": pdf_link
                    })
                    
        # Fallback if no announcements are found or if the network request for this stock failed
        if not has_announcements:
            all_records.append({
                "Symbol": symbol,
                "Company Name": "N/A",
                "Broadcast Date/Time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "Subject": "No announcements",
                "Details": "No corporate updates filed in this window",
                "Attachment Link": "None"
            })
        time.sleep(0.45)

    if len(all_records) == 0:
        logging.warning("No records compiled. Table matrix empty.")
        return

    df = pd.DataFrame(all_records)
    df = df.fillna("None")
    
    # Sort notices dynamically so active updates bubble to the very top rows
    df["is_active"] = df["Subject"] != "No announcements"
    df = df.sort_values(by="is_active", ascending=False).drop(columns=["is_active"])
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f"nse_announcements_{timestamp}.csv"
    df.to_csv(csv_filename, index=False)

    try:
        payload = {
            "content": f"📊 NSE Corporate Announcements Report (30-Symbol Live Feed)\nGenerated At: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} IST"
        }
        with open(csv_filename, "rb") as file_to_upload:
            files = {"file": (csv_filename, file_to_upload, "text/csv")}
            response = requests.post(DISCORD_WEBHOOK_URL, data=payload, files=files, timeout=15)
            if response.status_code < 300:
                logging.info("CSV snapshot successfully delivered to Discord.")
            else:
                logging.error(f"Discord upload rejected payload. Server responded with: {response.status_code}")
    except Exception as e:
        logging.error(f"Failed to transmit data to Discord: {e}")
    finally:
        if os.path.exists(csv_filename):
            os.remove(csv_filename)

if __name__ == "__main__":
    logging.info("Persistent Scraper Engine activated.")
    for i in range(36):
        logging.info(f"Executing cycle loop number: {i + 1} of 36")
        process_and_upload()
        logging.info("Cycle complete. Waiting exactly 5 minutes...")
        time.sleep(300)
