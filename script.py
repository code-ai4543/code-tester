import os
import time
import logging
from datetime import datetime
import requests
import pandas as pd

# --- CONFIGURATION FROM GITHUB SECRETS ---
# Safely pulls the secret variable injected by GitHub Actions environment
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK")

# List of distinct symbol structures you want to monitor
TRACKED_SYMBOLS = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "TATAMOTORS", "NIFTYBEES"]

# Setup Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def fetch_symbol_data(session, symbol, headers):
    """Fetches real-time announcements for a specific symbol using an active session."""
    api_url = f"https://nseindia.com{symbol}"
    try:
        response = session.get(api_url, headers=headers, timeout=5)
        if response.status_code == 200:
            return response.json()
        else:
            logging.warning(f"Failed to fetch data for {symbol}. Status code: {response.status_code}")
            return []
    except Exception as e:
        logging.error(f"Error connecting to NSE for {symbol}: {e}")
        return []

def process_and_upload():
    """Aggregates symbol data, compiles the CSV, and pushes to Discord."""
    if not DISCORD_WEBHOOK_URL:
        logging.error("Missing DISCORD_WEBHOOK environment variable. Exiting process.")
        return

    logging.info("Starting single-run corporate announcement pipeline...")
    all_records = []
    
    base_url = "https://nseindia.com"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://nseindia.com/companies-listing/corporate-filings-announcements"
    }
    
    try:
        session = requests.Session()
        # Initialize session cookies once
        session.get(base_url, headers=headers, timeout=5)
        
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
                    "Attachment Link": f"https://nseindia.com{item.get('attachment', '')}" if item.get('attachment') else "No Attachment"
                }
                all_records.append(record)
            
            time.sleep(0.45) # Maintain safe buffer delay
            
    except Exception as session_err:
        logging.error(f"Session initialization failed: {session_err}")
        return

    if not all_records:
        logging.info("No records found across targets. Skipping upload.")
        return

    # Convert to structured DataFrame
    df = pd.DataFrame(all_records)
    
    # Save file locally
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f"nse_announcements_{timestamp}.csv"
    df.to_csv(csv_filename, index=False)
    logging.info(f"Successfully generated: {csv_filename}")

    # Transmit via Discord Webhook
    try:
        payload = {
            "content": f"📊 **NSE Corporate Announcements Report**\n**Generated At:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} IST\n**Structures Scanned:** {', '.join(TRACKED_SYMBOLS)}"
        }
        
        with open(csv_filename, "rb") as file_to_upload:
            files = {"file": (csv_filename, file_to_upload, "text/csv")}
            response = requests.post(DISCORD_WEBHOOK_URL, data=payload, files=files, timeout=15)
            
            if response.status_code in [200, 204]:
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
