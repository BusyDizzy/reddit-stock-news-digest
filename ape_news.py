#!/usr/bin/env python3
"""
ape_news.py - daily news digest for the most discussed stocks on Reddit.

Pipeline:
  1. ApeWisdom API     -> today's most-mentioned tickers
  2. Filter            -> drop ETFs, duplicates and "common word" tickers
  3. SerpApi           -> last-24h Google News headlines per ticker
  4. Telegram Bot API  -> one formatted digest post to a channel

Usage:
  python ape_news.py --dry-run        # print the digest, don't post
  python ape_news.py                  # fetch and post to Telegram
  python ape_news.py --tickers 5      # override number of tickers
"""

from __future__ import annotations

import argparse
import html
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

APEWISDOM_URL = "https://apewisdom.io/api/v1.0/filter/all-stocks/page/1"
SERPAPI_URL = "https://serpapi.com/search.json"
TELEGRAM_URL = "https://api.telegram.org/bot{token}/sendMessage"
TELEGRAM_LIMIT = 4096  # max characters per Telegram message

# Words in an instrument's name that mark it as an ETF / fund.
ETF_KEYWORDS = re.compile(
    r"\b(ETF|Exchange[- ]Traded|Fund|iShares|ProShares|Direxion|SPDR)\b", re.I
)
# Well-known ETFs whose names don't always contain the keywords above.
ETF_TICKERS = {
    "SPY", "QQQ", "QQQM", "VOO", "VTI", "VXUS", "IWM", "DIA", "GLD", "SLV",
    "USO", "SGOV", "SMH", "SOXX", "SOXL", "SOXS", "TQQQ", "SQQQ", "SPMO",
    "ARKK", "TLT", "UVXY",
}
# Tickers that are also everyday words: ApeWisdom counts them from normal
# Reddit text, so their "mentions" are mostly noise.
COMMON_WORD_TICKERS = {
    "A", "I", "IT", "YOU", "API", "ON", "ALL", "UP", "GO", "EU", "CD", "CC",
    "TP", "OPEN", "LINK", "NOW", "BE", "IQ", "ES", "IP", "ET", "PR", "RR",
    "FOR", "UI", "OI", "IG", "AM", "ARE", "DD", "CEO", "USA", "EV", "AI",
}

log = logging.getLogger("ape_news")


# ---------------------------------------------------------------- config ---

def env(name: str, default: str | None = None, required: bool = False) -> str | None:
    value = os.getenv(name, default)
    if required and not value:
        sys.exit(f"Missing required environment variable: {name}")
    return value


def load_dotenv(path: str = ".env") -> None:
    """Minimal .env loader so the script has no extra dependencies."""
    p = Path(path)
    if not p.exists():
        return
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


# ------------------------------------------------------------ ApeWisdom ---

def fetch_trending(session: requests.Session) -> list[dict]:
    resp = session.get(APEWISDOM_URL, timeout=20)
    resp.raise_for_status()
    return resp.json().get("results", [])


def is_etf(item: dict) -> bool:
    return item["ticker"] in ETF_TICKERS or bool(ETF_KEYWORDS.search(item["name"]))


def clean_name(name: str) -> str:
    """'Meta Platforms (Facebook)' -> 'Meta Platforms'; decode '&amp;'."""
    name = html.unescape(name)
    return re.sub(r"\s*\(.*?\)", "", name).strip()


def select_tickers(results: list[dict], limit: int, min_mentions: int) -> list[dict]:
    picked, seen_names = [], set()
    for item in results:  # already sorted by rank
        name = clean_name(item["name"])
        if is_etf(item):
            log.debug("skip ETF %s", item["ticker"])
            continue
        if item["ticker"] in COMMON_WORD_TICKERS:
            log.debug("skip common-word ticker %s", item["ticker"])
            continue
        if name.lower() in seen_names:  # GOOG / GOOGL -> one company
            continue
        if (item.get("mentions") or 0) < min_mentions:
            break
        seen_names.add(name.lower())
        picked.append({**item, "name": name})
        if len(picked) >= limit:
            break
    return picked


# --------------------------------------------------------------- SerpApi ---

def parse_news_date(raw: str | None) -> datetime | None:
    """Google News engine returns dates like '09/22/2026, 07:00 AM, +0000 UTC'."""
    if not raw:
        return None
    try:
        return datetime.strptime(raw.replace(" UTC", ""), "%m/%d/%Y, %I:%M %p, %z")
    except ValueError:
        return None


def flatten_news(news_results: list[dict]) -> list[dict]:
    """Google News groups some articles into story clusters; flatten them."""
    flat = []
    for item in news_results:
        if item.get("link"):
            flat.append(item)
        if item.get("highlight", {}).get("link"):
            flat.append(item["highlight"])
        flat.extend(s for s in item.get("stories", []) if s.get("link"))
    return flat


def fetch_news(session: requests.Session, api_key: str, company: dict,
               per_ticker: int, max_age_hours: int) -> list[dict]:
    params = {
        "engine": "google_news",
        "q": f"{company['name']} stock when:1d",  # company name is less ambiguous than ticker
        "gl": "us",
        "hl": "en",
        "api_key": api_key,
    }
    resp = session.get(SERPAPI_URL, params=params, timeout=60)
    if resp.status_code == 429:
        raise RuntimeError("SerpApi: rate limit or monthly search quota reached")
    resp.raise_for_status()
    data = resp.json()
    if data.get("error"):
        # SerpApi returns e.g. "Google hasn't returned any results for this query."
        log.warning("SerpApi for %s: %s", company["ticker"], data["error"])
        return []

    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    articles, seen = [], set()
    for art in flatten_news(data.get("news_results", [])):
        published = parse_news_date(art.get("date"))
        if published and published < cutoff:
            continue
        key = art["link"]
        title_key = re.sub(r"\W+", "", art.get("title", "").lower())[:60]
        if key in seen or title_key in seen:
            continue
        seen.update({key, title_key})
        source = art.get("source")
        articles.append({
            "title": art.get("title", "").strip(),
            "link": art["link"],
            "source": source.get("name") if isinstance(source, dict) else source,
            "published": published,
        })
    articles.sort(key=lambda a: a["published"] or cutoff, reverse=True)
    return articles[:per_ticker]


# -------------------------------------------------------------- Telegram ---

def rank_change(item: dict) -> str:
    prev = item.get("rank_24h_ago")
    if not prev:
        return "new"
    diff = prev - item["rank"]
    return f"▲{diff}" if diff > 0 else (f"▼{-diff}" if diff < 0 else "=")


def build_blocks(companies: list[dict], header: str) -> list[str]:
    e = html.escape
    blocks = [f"<b>{e(header)}</b>\n<i>{datetime.now(timezone.utc):%d %b %Y}</i>"]
    for i, c in enumerate(companies, 1):
        lines = [
            f"<b>{i}. ${e(c['ticker'])} · {e(c['name'])}</b>\n"
            f"Reddit #{c['rank']} ({rank_change(c)}) · {c['mentions']} mentions"
        ]
        if not c["news"]:
            lines.append("  no fresh headlines")
        for a in c["news"]:
            src = f" — {e(a['source'], quote=False)}" if a["source"] else ""
            lines.append(f'• <a href="{e(a["link"], quote=True)}">{e(a["title"], quote=False)}</a>{src}')
        blocks.append("\n".join(lines))
    blocks.append("<i>Mentions: Reddit via ApeWisdom · News: Google News via SerpApi. Not investment advice.</i>")
    return blocks


def split_messages(blocks: list[str]) -> list[str]:
    """Pack blocks into messages under Telegram's 4096-char limit, never splitting a block."""
    messages, current = [], ""
    for block in blocks:
        candidate = f"{current}\n\n{block}" if current else block
        if len(candidate) > TELEGRAM_LIMIT and current:
            messages.append(current)
            current = block
        else:
            current = candidate
    if current:
        messages.append(current)
    return messages


def send_telegram(session: requests.Session, token: str, chat_id: str, text: str) -> None:
    resp = session.post(TELEGRAM_URL.format(token=token), json={
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "link_preview_options": {"is_disabled": True},
    }, timeout=20)
    if not resp.ok:
        raise RuntimeError(f"Telegram error {resp.status_code}: {resp.text}")


# ------------------------------------------------------------------ main ---

def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="print the digest instead of posting")
    parser.add_argument("--tickers", type=int, default=int(env("MAX_TICKERS", "8")))
    parser.add_argument("--per-ticker", type=int, default=int(env("NEWS_PER_TICKER", "3")))
    parser.add_argument("--min-mentions", type=int, default=int(env("MIN_MENTIONS", "20")))
    parser.add_argument("--save-json", help="also write the collected data to this file")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    # urllib3 logs full request URLs at DEBUG, and SerpApi takes the key as a
    # query parameter, so keep it quiet to avoid leaking the key into logs.
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    api_key = env("SERPAPI_API_KEY", required=True)
    session = requests.Session()
    session.headers["User-Agent"] = "ape-news-digest/1.0"

    companies = select_tickers(fetch_trending(session), args.tickers, args.min_mentions)
    log.info("Selected: %s", ", ".join(c["ticker"] for c in companies))

    for c in companies:
        try:
            c["news"] = fetch_news(session, api_key, c, args.per_ticker, max_age_hours=30)
        except requests.RequestException as exc:
            log.error("News for %s failed: %s", c["ticker"], exc)
            c["news"] = []
        time.sleep(1)  # be gentle; SerpApi also has hourly throughput limits

    if args.save_json:
        Path(args.save_json).write_text(json.dumps(companies, default=str, indent=2), encoding="utf-8")

    header = env("DIGEST_HEADER", "🔥 Most discussed stocks on Reddit — news digest")
    messages = split_messages(build_blocks(companies, header))

    if args.dry_run:
        print("\n\n----- message break -----\n\n".join(messages))
        return

    token = env("TELEGRAM_BOT_TOKEN", required=True)
    chat_id = env("TELEGRAM_CHAT_ID", required=True)
    for msg in messages:
        send_telegram(session, token, chat_id, msg)
    log.info("Posted %d message(s) to %s", len(messages), chat_id)


if __name__ == "__main__":
    main()