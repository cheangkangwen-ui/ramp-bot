"""
Serverless runner — called by GitHub Actions.
Reads asset + optional file info from env vars, gathers data, generates report, sends to Telegram.
"""

import os
import sys
import requests
from pathlib import Path

BOT_TOKEN     = os.environ["BOT_TOKEN"]
CHAT_ID       = os.environ["CHAT_ID"]
ASSET         = os.environ["ASSET"]
STAGED_FILES_JSON = os.environ.get("STAGED_FILES", "[]")  # JSON array of {file_id, file_name, caption}

REPORTS_DIR = Path("reports")
STAGED_DIR  = Path("staged_files")
REPORTS_DIR.mkdir(exist_ok=True)
STAGED_DIR.mkdir(exist_ok=True)


def send_message(text: str):
    requests.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
        json={"chat_id": CHAT_ID, "text": text},
    )


def send_document(file_path: str, caption: str = ""):
    with open(file_path, "rb") as f:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument",
            data={"chat_id": CHAT_ID, "caption": caption},
            files={"document": f},
        )


def download_files() -> tuple[list, dict]:
    """Download all staged files from Telegram."""
    import json
    staged = json.loads(STAGED_FILES_JSON)
    if not staged:
        return [], {}
    local_paths = []
    captions = {}
    for f in staged:
        file_id   = f.get("file_id", "")
        file_name = f.get("file_name", "upload")
        caption   = f.get("caption", "")
        if not file_id:
            continue
        try:
            r = requests.get(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getFile",
                params={"file_id": file_id},
            )
            tg_path = r.json()["result"]["file_path"]
            content = requests.get(
                f"https://api.telegram.org/file/bot{BOT_TOKEN}/{tg_path}"
            ).content
            local_path = STAGED_DIR / file_name
            local_path.write_bytes(content)
            local_paths.append(str(local_path))
            if caption:
                captions[str(local_path)] = caption
            print(f"  Downloaded: {file_name} ({len(content):,} bytes)")
        except Exception as e:
            print(f"  Failed to download {file_name}: {e}")
    return local_paths, captions


def detect_mode(asset: str) -> str:
    from gather_macro import ASSET_MAP
    key = asset.upper().replace("/", "").replace("-", "").replace(".", "")
    return "macro" if key in ASSET_MAP else "company"


def resolve_ticker(query: str) -> str | None:
    """Try to resolve a company name to a ticker via yfinance search."""
    import yfinance as yf
    try:
        results = yf.Search(query)
        quotes = results.quotes if hasattr(results, 'quotes') else []
        if quotes:
            return quotes[0].get("symbol")
    except Exception:
        pass
    return None


def validate_company_ticker(ticker: str) -> bool:
    """Check if a ticker is a real company on yfinance."""
    import yfinance as yf
    try:
        info = yf.Ticker(ticker).info
        return bool(info.get("marketCap") or info.get("currentPrice") or info.get("longName"))
    except Exception:
        return False


def main():
    global ASSET
    print(f"Asset: {ASSET}")
    staged_files, staged_captions = download_files()
    mode = detect_mode(ASSET)
    print(f"Mode: {mode}")

    # If not a known macro asset, check if it's a valid ticker — if not, try resolving as company name
    if mode == "company" and not validate_company_ticker(ASSET):
        print(f"  '{ASSET}' not a valid ticker, searching as company name...")
        resolved = resolve_ticker(ASSET)
        if resolved and validate_company_ticker(resolved):
            send_message(f"Resolved '{ASSET}' → {resolved}. Generating report...")
            print(f"  Resolved to: {resolved}")
            ASSET = resolved
        else:
            send_message(
                f"'{ASSET}' is not a recognized macro asset or valid company ticker.\n\n"
                f"Examples:\n"
                f"  Macro: GOLD, WTI, SPX, BTC, EURUSD, US10Y\n"
                f"  Company: NVDA, AAPL, TSLA, MSFT\n\n"
                f"You can also try the full company name (e.g. 'nvidia', 'toyota')."
            )
            print(f"Invalid asset: {ASSET}")
            sys.exit(0)

    if mode == "macro":
        from gather_macro import gather_all, resolve_asset
        from report_macro import generate_report, export_docx, export_pdf
        from tg_group import send_to_group

        gathered     = gather_all(ASSET, staged_files, staged_captions)
        display_name = gathered.get("display_name", ASSET)
        asset_type   = gathered.get("asset_type", "")

        send_message(f"Data loaded for {display_name}. Generating report...")

        text     = generate_report(gathered)
        doc_path = export_docx(ASSET, display_name, text, str(REPORTS_DIR))
        pdf_path = export_pdf(doc_path)

        send_to_group(text, doc_path, pdf_path, display_name, ASSET,
                      "📊 Macro Ramp", "Macro ramp research reports")
        send_document(doc_path, f"Macro report: {display_name} ({ASSET})")
        send_document(pdf_path, "PDF version")

    else:
        from gather_company import gather_all
        from report_company import generate_report, export_docx, export_pdf
        from tg_group import send_to_group

        gathered     = gather_all(ASSET, staged_files, staged_captions)
        company_name = gathered.get("company_name", ASSET)

        send_message(f"Data loaded for {company_name}. Generating report...")

        text     = generate_report(ASSET, gathered)
        doc_path = export_docx(ASSET, company_name, text, str(REPORTS_DIR))
        pdf_path = export_pdf(doc_path)

        send_to_group(text, doc_path, pdf_path, company_name, ASSET,
                      "📊 Company Ramp", "Company ramp research reports")
        send_document(doc_path, f"Company report: {company_name} ({ASSET})")
        send_document(pdf_path, "PDF version")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Fatal error: {e}", file=sys.stderr)
        send_message(f"Report failed for {ASSET}: {e}")
        sys.exit(1)
