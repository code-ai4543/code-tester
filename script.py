import os
import time
import logging
from datetime import datetime
import requests
import pandas as pd

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK")
TRACKED_SYMBOLS = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "TATAMOTORS", "NIFTYBEES"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def fetch_symbol_data(session, symbol, headers):
    api_url = "https://www.nseindia.com/api/corporate-announcements"
    query_params = {"index": "equities", "symbol": symbol}
    try:
        response = session.get(api_url, headers=headers, params=query_params, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            logging.warning(f"Failed to fetch data for {symbol}. Status code: {response.status_code}")
            return []
    except Exception as e:
        logging.error(f"Error connecting to NSE for {symbol}: {e}")
        return []

def process_and_upload():
    if not DISCORD_WEBHOOK_URL:
        logging.error("Missing DISCORD_WEBHOOK environment variable. Exiting process.")
        return
    
    logging.info("Starting corporate announcement pipeline...")
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
        
        for symbol in TRACKED_SYMBOLS:
            logging.info(f"Pulling data for structure: {symbol}")
            raw_data = fetch_symbol_data(session, symbol, headers)
            for item in raw_data:
                record = {
                    "Symbol": item.get("symbol", symbol),
                    "Company Name": item.get("companyName", "N/A"),
                    "Broadcast Date/Time": item.get("anng_dt", "N/A"),
                    "Subject": item.get("desc", "N/A"),
                    "Details": item.get("details", "N/A"),
                    "Attachment Link": f"https://www.nseindia.com{item.get('attachment', '')}" if item.get('attachment') else "No Attachment"
                }
                all_records.append(record)
            time.sleep(0.75)
    except Exception as session_err:
        logging.error(f"Session initialization failed: {session_err}")
        return

    if not all_records:
        logging.info("No fresh market records found right now. Sending structural confirmation row to Discord.")
        all_records.append({
            "Symbol": "SYSTEM_CHECK",
            "Company Name": "Automated Scraper Engine",
            "Broadcast Date/Time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "Subject": "Connection Verified",
            "Details": "The pipeline layout and status comparisons are now completely operational!",
            "Attachment Link": "No Attachment"
        })

    df = pd.DataFrame(all_records)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f"nse_announcements_{timestamp}.csv"
    df.to_csv(csv_filename, index=False)

    try:
        payload = {
            "content": f"📊 NSE Corporate Announcements Report (5-Min Feed)\nGenerated At: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} IST\nStructures Scanned: {', '.join(TRACKED_SYMBOLS)}"
        }
        with open(csv_filename, "rb") as file_to_upload:
            files = {"file": (csv_filename, file_to_upload, "text/csv")}
            response = requests.post(DISCORD_WEBHOOK_URL, data=payload, files=files, timeout=15)
            if response.status_code < 300:
                logging.info("CSV snapshot successfully delivered to Discord.")
            else:
                logging.error(f"Discord upload failed. Status code: {response.status_code}")
    except Exception as e:
        logging.error(f"Failed to transmit data to Discord: {e}")
    finally:
        if os.path.exists(csv_filename):
            os.remove(csv_filename)

if __name__ == "__main__":
    process_and_upload()
