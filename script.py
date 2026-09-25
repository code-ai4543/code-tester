import os
import io
import time
import logging
from datetime import datetime
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK")

# How many symbols to hit NSE with at once. NSE rate-limits aggressively,
# so this is intentionally conservative rather than maxed out.
MAX_WORKERS = 35

# How many times to retry a symbol if the request fails or gets rate-limited,
# and how long to wait between retries (seconds, doubles each attempt).
MAX_RETRIES = 2
RETRY_BACKOFF_BASE = 2.5

# Official NSE equity symbol master file (SYMBOL, NAME OF COMPANY, ...)
SYMBOL_MASTER_URL = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"

# The GitHub Actions runner is thrown away after every job, so the message ID
# has to be written to a file and restored (via actions/cache in the workflow)
# rather than just kept in memory, or a new job would always start a fresh message.
MESSAGE_ID_FILE = "discord_message_id.txt"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Global tracking variable to maintain the active Discord message ID across loops
ACTIVE_MESSAGE_ID = None


def load_message_id():
    try:
        if os.path.exists(MESSAGE_ID_FILE):
            with open(MESSAGE_ID_FILE, "r") as f:
                content = f.read().strip()
                if content:
                    logging.info(f"Restored cached Discord message ID: {content}")
                    return content
    except Exception as e:
        logging.warning(f"Could not read cached message ID: {e}")
    return None


def save_message_id(message_id):
    try:
        with open(MESSAGE_ID_FILE, "w") as f:
            f.write(str(message_id))
    except Exception as e:
        logging.warning(f"Could not save message ID to cache file: {e}")


def get_all_symbols(session, headers):
    """Fetch the full list of currently listed NSE equity symbols."""
    try:
        response = session.get(SYMBOL_MASTER_URL, headers=headers, timeout=15)
        if response.status_code != 200:
            logging.error(f"Failed to fetch symbol master. Status: {response.status_code}")
            return []
        df = pd.read_csv(io.StringIO(response.text))
        df.columns = [c.strip() for c in df.columns]
        symbols = df["SYMBOL"].dropna().astype(str).str.strip().tolist()
        logging.info(f"Fetched {len(symbols)} listed symbols from NSE.")
        return symbols
    except Exception as e:
        logging.error(f"Error fetching symbol master: {e}")
        return []


def fetch_symbol_data(session, symbol, headers):
    api_url = "https://www.nseindia.com/api/corporate-announcements"
    query_params = {"index": "equities", "symbol": symbol}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(api_url, headers=headers, params=query_params, timeout=5)
            if response.status_code == 200:
                return symbol, response.json()

            # 401/403/429 from NSE usually means it flagged the session as a bot,
            # or you're being rate-limited. A short backoff and retry often clears it.
            if response.status_code in (401, 403, 429) and attempt < MAX_RETRIES:
                wait_time = RETRY_BACKOFF_BASE ** attempt
                time.sleep(wait_time)
                continue

            return symbol, None
        except Exception:
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_BASE ** attempt)
                continue
            return symbol, None

    return symbol, None


def process_and_upload():
    global ACTIVE_MESSAGE_ID
    if not DISCORD_WEBHOOK_URL:
        logging.error("Missing DISCORD_WEBHOOK environment variable.")
        return

    logging.info("Starting full-market announcement scan...")
    base_url = "https://www.nseindia.com"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-announcements"
    }

    try:
        session = requests.Session()
        # Default pool holds 10 connections; with MAX_WORKERS threads hitting it
        # concurrently, a smaller pool forces requests to keep discarding and
        # reopening connections instead of reusing them.
        adapter = requests.adapters.HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.get(base_url, headers=headers, timeout=8)
        time.sleep(0.5)
    except Exception as session_err:
        logging.error(f"Session initialization failed completely: {session_err}")
        return

    tracked_symbols = get_all_symbols(session, headers)
    if not tracked_symbols:
        logging.error("No symbols retrieved, aborting this cycle.")
        return

    today_str = datetime.now().strftime("%d-%b-%Y")
    symbol_results = {}
    all_records = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_symbol_data, session, sym, headers): sym for sym in tracked_symbols}
        completed = 0
        for future in as_completed(futures):
            sym, res = future.result()
            symbol_results[sym] = res
            completed += 1
            if completed % 200 == 0:
                logging.info(f"Checked {completed}/{len(tracked_symbols)} symbols so far...")

    for symbol in tracked_symbols:
        raw_data = symbol_results.get(symbol)
        if raw_data is None or not isinstance(raw_data, list):
            continue

        for item in raw_data:
            if not (isinstance(item, dict) and item.get("desc")):
                continue
            date_time = item.get("an_dt", "N/A")
            if today_str not in str(date_time):
                continue

            comp_name = item.get("sm_name", "N/A")
            subject = item.get("desc", "None")
            details = item.get("attchmntText", "None")
            file_pdf = item.get("attchmntFile", "")

            if not file_pdf:
                pdf_link = "None"
            elif "http" in str(file_pdf):
                pdf_link = file_pdf
            else:
                pdf_link = f"https://nsearchives.nseindia.com/corporate/{file_pdf}"

            all_records.append({
                "Symbol": symbol,
                "Company Name": comp_name,
                "Broadcast Date/Time": date_time,
                "Subject": subject,
                "Details": details,
                "Attachment Link": pdf_link
            })

    if len(all_records) == 0:
        logging.info("No symbols had announcements today. Skipping upload.")
        return

    df = pd.DataFrame(all_records).fillna("None")
    df = df.sort_values(by="Broadcast Date/Time", ascending=False)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f"nse_announcements_{timestamp}.csv"
    df.to_csv(csv_filename, index=False)

    unique_symbols = df["Symbol"].unique().tolist()
    content_text = (
        f"📊 NSE Daily Corporate Announcements Report (Latest Snapshot)\n"
        f"Updated At: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} IST\n"
        f"Symbols with announcements today ({len(unique_symbols)} of {len(tracked_symbols)} scanned): "
        f"{', '.join(unique_symbols)}\n"
        f"Note: This message automatically updates every 4.5 minutes to keep your channel clean."
    )

    try:
        if ACTIVE_MESSAGE_ID:
            edit_url = f"{DISCORD_WEBHOOK_URL}/messages/{ACTIVE_MESSAGE_ID}"
            payload = {"content": content_text}
            with open(csv_filename, "rb") as file_to_upload:
                files = {"file": (csv_filename, file_to_upload, "text/csv")}
                response = requests.patch(edit_url, data=payload, files=files, timeout=15)
                if response.status_code < 300:
                    save_message_id(ACTIVE_MESSAGE_ID)
                    logging.info("Existing Discord message successfully updated and replaced.")
                    return
                else:
                    # A 404 here usually means the message was deleted manually in Discord.
                    # Clear the cached ID so we fall through to posting a fresh one below.
                    if response.status_code == 404:
                        ACTIVE_MESSAGE_ID = None
                    logging.warning(f"Failed to edit message. Attempting a clean repost. Status: {response.status_code}")

        post_url = f"{DISCORD_WEBHOOK_URL}?wait=true"
        payload = {"content": content_text}
        with open(csv_filename, "rb") as file_to_upload:
            files = {"file": (csv_filename, file_to_upload, "text/csv")}
            response = requests.post(post_url, data=payload, files=files, timeout=15)
            if response.status_code < 300:
                ACTIVE_MESSAGE_ID = response.json().get("id")
                save_message_id(ACTIVE_MESSAGE_ID)
                logging.info(f"Initial snapshot posted. Captured message ID: {ACTIVE_MESSAGE_ID}")
            else:
                logging.error(f"Discord upload failed. Status code: {response.status_code}")

    except Exception as e:
        logging.error(f"Failed to transmit data to Discord: {e}")
    finally:
        if os.path.exists(csv_filename):
            os.remove(csv_filename)


if __name__ == "__main__":
    logging.info("Persistent full-market announcement scanner active.")
    ACTIVE_MESSAGE_ID = load_message_id()

    # 4.5 minutes per cycle x 40 cycles = 180 minutes, exactly matching the
    # 3-hour window before your workflow's cron restarts the job.
    TOTAL_CYCLES = 40
    CYCLE_SECONDS = 270  # 4.5 minutes

    for i in range(TOTAL_CYCLES):
        logging.info(f"Executing cycle loop number: {i + 1} of {TOTAL_CYCLES}")
        process_and_upload()
        logging.info("Cycle complete. Waiting 4.5 minutes...")
        time.sleep(CYCLE_SECONDS)
