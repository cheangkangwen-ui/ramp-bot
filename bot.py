"""
Ramp Bot — unified company + macro research bot.
Commands: /load ASSET_OR_TICKER  /ramp  /status  /clear

Auto-detects mode:
  - Known macro asset (GOLD, EURUSD, SPX, US10Y, BTC, ...) → macro mode
  - Anything else (NVDA, AAPL, MSFT, ...) → company mode
"""

import asyncio
import json
import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID   = int(os.environ.get("TELEGRAM_CHAT_ID", "0"))

BASE_DIR    = Path(__file__).parent
STAGED_DIR  = BASE_DIR / "staged_files"
REPORTS_DIR = BASE_DIR / "reports"
STATE_FILE  = BASE_DIR / "state.json"

STAGED_DIR.mkdir(exist_ok=True)
REPORTS_DIR.mkdir(exist_ok=True)

COMPANY_GROUP = "📊 Company Ramp"
MACRO_GROUP   = "📊 Macro Ramp"


# ── Mode detection ───────────────────────────────────────────────────────────────

def detect_mode(asset_input: str) -> str:
    """Return 'macro' if asset_input matches a known macro asset, else 'company'."""
    from gather_macro import ASSET_MAP
    key = asset_input.upper().replace("/", "").replace("-", "").replace(".", "")
    return "macro" if key in ASSET_MAP else "company"


# ── State helpers ────────────────────────────────────────────────────────────────

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {"mode": None, "asset": None, "staged_files": [], "staged_captions": {}, "context": None}


def save_state(state: dict):
    STATE_FILE.write_text(json.dumps(state, indent=2))


def is_authorized(update: Update) -> bool:
    return CHAT_ID == 0 or update.effective_chat.id == CHAT_ID


# ── Commands ─────────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    await update.message.reply_text(
        "Ramp Bot\n\n"
        "/load ASSET — load data for a company or macro asset\n"
        "/ramp       — generate report (~3-6 min)\n"
        "/status     — show current state\n"
        "/clear      — reset state\n\n"
        "COMPANIES: any ticker (NVDA, AAPL, TSLA, MSFT, ...)\n\n"
        "PRECIOUS METALS: GOLD SILVER PLATINUM PALLADIUM GDX GDXJ\n"
        "ENERGY: WTI BRENT NATGAS RBOB HEATINGOIL\n"
        "BASE METALS: COPPER ALUMINUM\n"
        "AGRICULTURAL: WHEAT CORN SOYBEAN COFFEE SUGAR COCOA COTTON CATTLE HOGS\n\n"
        "US RATES: US2Y US5Y US10Y US30Y TLT IEF SHY HYG LQD TIPS\n"
        "GLOBAL RATES: BUND GILT JGB\n\n"
        "G10 FX: EURUSD USDJPY GBPUSD AUDUSD NZDUSD USDCAD USDCHF DXY\n"
        "CROSSES: EURJPY EURGBP GBPJPY AUDJPY\n"
        "EM FX: USDCNY USDINR USDBRL USDMXN USDZAR USDTRY USDKRW USDSGD\n\n"
        "US INDEXES: SPX NDX DOW RUSSELL VIX\n"
        "EUROPE: FTSE DAX CAC IBEX SMI AEX STOXX50\n"
        "ASIA-PAC: NIKKEI HSI CSI300 KOSPI ASX200 SENSEX NIFTY\n"
        "EM: BOVESPA MEXBOL EEM\n\n"
        "CRYPTO: BTC ETH SOL XRP\n\n"
        "Any yfinance ticker also works (e.g. /load GC=F, /load ^TNX)"
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    state = load_state()
    asset  = state.get("asset") or "None"
    mode   = state.get("mode") or "None"
    staged = state.get("staged_files", [])
    loaded = "Yes" if state.get("context") else "No"
    ctx    = state.get("context", {})

    msg = f"Mode: {mode}\nAsset: {asset}\nData loaded: {loaded}\nStaged files: {len(staged)}"
    if ctx:
        display = ctx.get("display_name") or ctx.get("company_name", "")
        if display:
            msg += f"\nDisplay name: {display}"
    for f in staged:
        msg += f"\n  - {Path(f).name}"
    await update.message.reply_text(msg)


async def cmd_clear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    for f in STAGED_DIR.iterdir():
        try:
            f.unlink()
        except Exception:
            pass
    save_state({"mode": None, "asset": None, "staged_files": [], "staged_captions": {}, "context": None})
    await update.message.reply_text("Cleared. State and staged files reset.")


async def cmd_load(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return

    args = context.args
    if not args:
        await update.message.reply_text("Usage: /load ASSET  (e.g. /load NVDA, /load GOLD, /load EURUSD)")
        return

    asset_input = args[0].upper().strip()
    mode = detect_mode(asset_input)
    state = load_state()
    staged_files    = state.get("staged_files", [])
    staged_captions = state.get("staged_captions", {})

    if mode == "macro":
        from gather_macro import resolve_asset
        yf_ticker, display_name, asset_type = resolve_asset(asset_input)
        await update.message.reply_text(
            f"Mode: MACRO\n"
            f"Loading {display_name} ({yf_ticker}) [{asset_type}]...\n"
            f"Sources: yfinance, web search, News Digest (Telegram)"
            + (f", {len(staged_files)} uploaded file(s)" if staged_files else "") + "\n"
            "~15 seconds."
        )
        loop = asyncio.get_event_loop()
        def _gather():
            from gather_macro import gather_all
            return gather_all(asset_input, staged_files, staged_captions)
    else:
        await update.message.reply_text(
            f"Mode: COMPANY\n"
            f"Loading {asset_input}...\n"
            f"Sources: yfinance, SEC EDGAR, web search, Stock Digest (Telegram)"
            + (f", {len(staged_files)} uploaded file(s)" if staged_files else "") + "\n"
            "~30 seconds."
        )
        loop = asyncio.get_event_loop()
        def _gather():
            from gather_company import gather_all
            return gather_all(asset_input, staged_files, staged_captions)

    try:
        gathered = await loop.run_in_executor(None, _gather)
        state["mode"]    = mode
        state["asset"]   = asset_input
        state["context"] = gathered
        save_state(state)

        if mode == "macro":
            display_name = gathered.get("display_name", asset_input)
            asset_type   = gathered.get("asset_type", "")
            await update.message.reply_text(
                f"Loaded: {display_name} ({asset_input}) [{asset_type}]\n"
                "Send /ramp to generate the report."
            )
        else:
            company_name = gathered.get("company_name", asset_input)
            await update.message.reply_text(
                f"Loaded: {company_name} ({asset_input})\n"
                "Send /ramp to generate the report."
            )
    except Exception as e:
        logger.exception("Load failed")
        await update.message.reply_text(f"Load failed: {e}")


async def cmd_ramp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return

    state = load_state()
    if not state.get("context"):
        await update.message.reply_text("No data loaded. Run /load ASSET first.")
        return

    gathered    = state["context"]
    asset_input = state["asset"]
    mode        = state["mode"]

    if mode == "macro":
        display_name = gathered.get("display_name", asset_input)
        asset_type   = gathered.get("asset_type", "")
        await update.message.reply_text(
            f"Generating macro ramp report for {display_name} ({asset_input}) [{asset_type}]...\n"
            "Claude is researching with web search. 3-6 minutes."
        )
    else:
        company_name = gathered.get("company_name", asset_input)
        await update.message.reply_text(
            f"Generating company ramp report for {company_name} ({asset_input})...\n"
            "Claude is researching with web search. 3-6 minutes."
        )

    loop = asyncio.get_event_loop()

    def _generate():
        from tg_group import send_to_group
        if mode == "macro":
            from report_macro import generate_report, export_docx, export_pdf
            display_name = gathered.get("display_name", asset_input)
            asset_type   = gathered.get("asset_type", "")
            text     = generate_report(gathered)
            doc_path = export_docx(asset_input, display_name, text, str(REPORTS_DIR))
            pdf_path = export_pdf(doc_path)
            send_to_group(text, doc_path, pdf_path, display_name, asset_input,
                          MACRO_GROUP, "Macro asset ramp research reports")
            return text, doc_path, pdf_path
        else:
            from report_company import generate_report, export_docx, export_pdf
            company_name = gathered.get("company_name", asset_input)
            text     = generate_report(gathered)
            doc_path = export_docx(asset_input, company_name, text, str(REPORTS_DIR))
            pdf_path = export_pdf(doc_path)
            send_to_group(text, doc_path, pdf_path, company_name, asset_input,
                          COMPANY_GROUP, "Company ramp research reports")
            return text, doc_path, pdf_path

    try:
        report_text, doc_path, pdf_path = await loop.run_in_executor(None, _generate)

        with open(doc_path, "rb") as f:
            await update.message.reply_document(
                document=f,
                filename=Path(doc_path).name,
                caption="Report sent to group. Word doc attached.",
            )
        with open(pdf_path, "rb") as f:
            await update.message.reply_document(
                document=f,
                filename=Path(pdf_path).name,
                caption="PDF version.",
            )

        logger.info(f"Report sent for {asset_input} ({mode}), doc: {doc_path}")

    except Exception as e:
        logger.exception("Report generation failed")
        await update.message.reply_text(f"Report generation failed: {e}")


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Stage uploaded PDF, Excel, or CSV files."""
    if not is_authorized(update):
        return

    doc = update.message.document
    if not doc:
        return

    fname = doc.file_name or f"upload_{doc.file_id}"
    ext   = Path(fname).suffix.lower()
    supported = {".pdf", ".xlsx", ".xls", ".csv"}

    if ext not in supported:
        await update.message.reply_text(
            f"Unsupported file type: {ext}\nSupported: PDF, Excel (.xlsx/.xls), CSV"
        )
        return

    save_path = STAGED_DIR / fname
    tg_file   = await context.bot.get_file(doc.file_id)
    await tg_file.download_to_drive(str(save_path))

    caption = update.message.caption or ""

    state    = load_state()
    staged   = state.get("staged_files", [])
    captions = state.get("staged_captions", {})

    if str(save_path) not in staged:
        staged.append(str(save_path))
    if caption:
        captions[str(save_path)] = caption

    state["staged_files"]    = staged
    state["staged_captions"] = captions
    save_state(state)

    await update.message.reply_text(
        f"Staged: {fname}"
        + (f"\nContext: {caption}" if caption else "") +
        f"\nTotal staged: {len(staged)} file(s)\n\n"
        "Run /load ASSET to include these in the data pull."
    )


# ── Main ─────────────────────────────────────────────────────────────────────────

def main():
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN not set in .env")

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start",  cmd_start))
    app.add_handler(CommandHandler("help",   cmd_start))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("clear",  cmd_clear))
    app.add_handler(CommandHandler("load",   cmd_load))
    app.add_handler(CommandHandler("ramp",   cmd_ramp))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))

    logger.info("Ramp Bot started.")
    app.run_polling()


if __name__ == "__main__":
    main()
