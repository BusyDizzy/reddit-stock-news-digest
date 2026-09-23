# Reddit Trending Stocks → News Digest

A daily pipeline that answers one question: **the stocks Reddit is talking about most today — what is actually in the news about them, and how solid is the evidence?**

It combines four APIs:

1. **[ApeWisdom](https://apewisdom.io)** returns the most-mentioned tickers across Reddit investing communities.
2. **[SerpApi Google News API](https://serpapi.com/google-news-api)** returns the last 24 hours of headlines for each company.
3. **[OpenAI](https://platform.openai.com)** (optional) turns the collected evidence into a short explanation of why the stock is being discussed.
4. **Telegram Bot API** posts the digest to a channel once a day.

Results are stored in PostgreSQL, which also makes the daily post idempotent and gives the project a history to analyse later.

Built as a news module for [TickerForge](https://github.com/BusyDizzy), my investing analytics project.

## Example output

```
🔥 Most discussed stocks on Reddit — news digest
22 Sep 2026

1. $META · Meta Platforms
Reddit #1 (▲9) · 429 mentions
Evidence confidence: MEDIUM
<2–3 sentence summary of the likely catalyst, with disagreement preserved>
Sources: Barchart.com · Investor's Business Daily · Seeking Alpha
```

## How it works

**Ticker selection.** ApeWisdom's `all-stocks` list mixes stocks and ETFs, and it also counts tickers that are ordinary English words (`IT`, `YOU`, `ON`, `ALL`), which inflates their mentions. The script skips ETFs (by name keywords such as *ETF*, *Fund*, *iShares*, plus a list of common ETF tickers), skips common-word tickers, merges share classes of the same company (`GOOG`/`GOOGL`) and keeps the top N by mentions.

**News search.** Each company is queried by name rather than ticker (`"Micron Technology" stock when:1d`), because names are far less ambiguous. `when:1d` is a Google News operator that the SerpApi `google_news` engine passes through. Results are flattened first: Google News groups some articles into story clusters with `highlight` and `stories` members.

**Entity matching.** A headline is kept only if it actually refers to the company. The scorer looks for the ticker (case-sensitively, so `MU` matches but the word *mu* does not; `$mu` is accepted either way), the company name and its aliases, and — from an optional profile file — products, executives and exchange symbols. The publisher name is deliberately excluded from matching, so an article from *Apple Insider* is not treated as an Apple mention.

**Deduplication.** URLs are canonicalised first: scheme and host normalised, `www.` and default ports dropped, tracking parameters (`utm_*`, `fbclid`, `gclid`, `ref`, …) removed, remaining parameters sorted. Headlines are then compared fuzzily — sequence ratio plus token overlap and containment — so reworded copies of the same article collapse into one record. Every dropped copy is remembered as a duplicate source family rather than being silently discarded.

**Event clustering and confidence.** Surviving articles are grouped into probable events. Two headlines belong to the same event if they are near-identical or share distinctive vocabulary — the company's own name and generic market words (*stock*, *rally*, *analyst*, *earnings*, …) are stripped first, so grouping relies on what is actually specific to the story, such as a product or partner name. Each event is then scored by how many **independent source families** carry it (three syndicated copies of one wire story count once), by average entity relevance and by freshness:

| Confidence | Condition |
|---|---|
| `high` | ≥3 independent source families and average relevance ≥0.70 |
| `medium` | ≥2 independent families, or average relevance ≥0.85 |
| `low` | anything else |

**AI summary (optional).** The model receives the numbered evidence grouped by event, each event's confidence, and the Reddit attention data. The prompt restricts it to that evidence, forbids claiming that news caused the Reddit attention, requires preserving disagreement, and asks it to name the strongest evidence items — those become the source links under the summary. If the key isn't set, `--no-ai` is passed, or a call fails, that company falls back to a plain headline list.

**Storage and idempotency.** Before spending any SerpApi credits, the run takes a PostgreSQL advisory lock and looks up today's digest. If it was already posted, the run exits; if delivery was interrupted, it resumes by sending only the messages that have no delivery record. Snapshots (ticker, Reddit rank and mentions, events, sources, confidence, summary) are written per run, which is what makes historical analysis possible — for example, whether Reddit attention leads price moves.

**Reliability.** Every outbound call (ApeWisdom, SerpApi, OpenAI, Telegram) goes through one retry helper with bounded exponential backoff, jitter and `Retry-After` support. Authentication and quota failures from SerpApi are treated as fatal instead of being retried into a half-empty digest. Note the one case the Bot API cannot solve: if Telegram times out *after* accepting a message, the retry can duplicate that message — Telegram has no idempotency key.

**Quota awareness.** The free SerpApi plan currently includes 250 searches per month. Each run spends one search per ticker, so eight tickers on weekdays is roughly 175 searches a month. Before collecting, the run calls the free Account API and aborts if fewer searches remain than the run needs. Identical searches repeated within an hour are served from SerpApi's cache and are not counted.

## Setup

```bash
git clone <repo> && cd ape-news
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env        # add your keys
.venv/bin/python ape_news.py --dry-run --tickers 1   # preview; no database needed
.venv/bin/python ape_news.py                          # collect, store and post
```

Telegram: create a bot with @BotFather, add it to your channel as an administrator with permission to post, and set `TELEGRAM_CHAT_ID` to `@channel_name`.

PostgreSQL: point `DATABASE_URL` at any database; the `tickerforge_news` schema and its tables are created automatically on first run.

## Entity profiles

`ENTITY_PROFILES_FILE` points to a JSON file that extends the built-in name matching with brands, products, executives and exchanges:

```json
{
  "NVDA": {
    "aliases": ["NVIDIA"],
    "products": ["CUDA", "GeForce", "Blackwell"],
    "ceos": ["Jensen Huang"],
    "exchanges": ["NASDAQ"]
  }
}
```

## Daily schedule

cron (server time in UTC; 00:30 UTC is 07:30 in Phuket, after the US close):

```
30 0 * * 2-6 root cd /home/prod/ape-news && .venv/bin/python ape_news.py >> digest.log 2>&1
```

Because delivery is idempotent per channel per day, a retry after a failure re-sends only what never arrived.

## Options

| Flag / env var | Default | Meaning |
|---|---|---|
| `--tickers` / `MAX_TICKERS` | 8 | Companies in the digest (also the search budget per run) |
| `--per-ticker` / `NEWS_PER_TICKER` | 3 | Source links shown per company |
| `--ai-headlines` / `AI_HEADLINES` | 10 | Entity-matched headlines kept per company (same search, no extra cost) |
| `--min-mentions` / `MIN_MENTIONS` | 20 | Stop once mentions drop below this |
| `--max-age-hours` / `MAX_NEWS_AGE_HOURS` | 30 | Reject older articles; undated articles are always rejected |
| `--no-ai` | off | Headlines only, skip the LLM step |
| `OPENAI_MODEL` | none | Model for summaries (required if `OPENAI_API_KEY` is set) |
| `SUMMARY_LANGUAGE` | English | Language of the summaries |
| `DIGEST_TIMEZONE` | UTC | Timezone that decides which calendar day a run belongs to |
| `ENTITY_PROFILES_FILE` | none | JSON file with aliases, products, executives, exchanges |
| `--save-json FILE` | none | Save the normalised collection for debugging |
| `--dry-run` | off | Print instead of storing and posting; no database required |

## Schema

- `tickerforge_news.digest_run` — one row per channel per day: status, quota snapshot, rendered messages.
- `tickerforge_news.ticker_snapshot` — per run and ticker: Reddit rank and mentions (now and 24h ago), confidence, summary, events and sources as JSONB.
- `tickerforge_news.delivery` — which message of which run reached Telegram, with its message id.

Not investment advice.