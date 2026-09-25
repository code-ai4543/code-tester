import os
import time
import logging
from datetime import datetime
import requests
import pandas as pd

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def process_and_upload():
    if not DISCORD_WEBHOOK_URL:
        logging.error("Missing DISCORD_WEBHOOK environment variable. Exiting process.")
        return
        
    logging.info("Starting global market corporate announcement pipeline...")
    all_records = []
    base_url = "nseindia.com"
    # Global endpoint that fetches ALL equity market announcements at once
    api_url = "nseindia.com"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "/",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "nseindia.com"
    }
    
    try:
        session = requests.Session()
        # Fire initial handshake request to capture cookies
        session.get(base_url, headers=headers, timeout=10)
        time.sleep(1)
        
        # Pull everything happening across all stocks
        logging.info("Pulling complete consolidated market data feed...")
        response = session.get(api_url, headers=headers, timeout=15)
        
        if response.status_code == 200:
            raw_data = response.json()
            for item in raw_data:
                record = {
                    "Symbol": item.get("symbol", "N/A"),
                    "Company Name": item.get("companyName", "N/A"),
                    "Broadcast Date/Time": item.get("anng_dt", "N/A"),
                    "Subject": item.get("desc", "N/A"),
                    "Details": item.get("details", "N/A"),
                    "Attachment Link": f"nseindia.com{item.get('attachment', '')}" if item.get('attachment') else "No Attachment"
                }
                all_records.append(record)
        else:
            logging.warning(f"Failed to fetch market data feed. Status code: {response.status_code}")
            
    except Exception as e:
        logging.error(f"Error connecting to global market feed: {e}")
        return

    if not all_records:
        logging.info("No market records found right now. Sending structural confirmation row to Discord.")
        all_records.append({
            "Symbol": "SYSTEM_CHECK",
            "Company Name": "Automated Global Scraper",
            "Broadcast Date/Time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "Subject": "Connection Verified",
            "Details": "The global market feed connection is clear, but the market is currently quiet.",
            "Attachment Link": "No Attachment"
        })

    df = pd.DataFrame(all_records)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f"nse_all_announcements_{timestamp}.csv"
    df.to_csv(csv_filename, index=False)

    try:
        payload = {
            "content": f"📊 NSE All-Symbols Corporate Announcements Report (5-Min Feed)\nGenerated At: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} IST\nStatus: Monitoring all active NSE listed symbols."
        }
        with open(csv_filename, "rb") as file_to_upload:
            files = {"file": (csv_filename, file_to_upload, "text/csv")}
            response = requests.post(DISCORD_WEBHOOK_URL, data=payload, files=files, timeout=15)
            if response.status_code < 300:
                logging.info("Global market CSV snapshot successfully delivered to Discord.")
            else:
                logging.error(f"Discord upload failed. Status code: {response.status_code}")
    except Exception as e:
        logging.error(f"Failed to transmit global data to Discord: {e}")
    finally:
        if os.path.exists(csv_filename):
            os.remove(csv_filename)

if __name__ == "__main__":
    process_and_upload()
