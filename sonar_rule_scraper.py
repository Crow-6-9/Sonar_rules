import asyncio
import aiohttp
from aiohttp import ClientTimeout
from bs4 import BeautifulSoup
import json
import re
import time
import os
from datetime import datetime
from colorama import Fore, Style, init

init(autoreset=True)

# ---------------- Logger ----------------
class ColorLogger:
    def __init__(self, path="scraper.log"):
        self.path = path
        with open(self.path, "w", encoding="utf-8") as f:
            f.write(f"=== Sonar Scraper Log started at {datetime.now()} ===\n")

    def _write(self, level: str, msg: str):
        stamp = datetime.now().isoformat(timespec="seconds")
        line = f"[{stamp}] [{level}] {msg}"
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
        color = {
            "INFO": Fore.CYAN,
            "OK": Fore.GREEN,
            "WARN": Fore.YELLOW,
            "ERR": Fore.RED
        }.get(level, "")
        print(f"{color}{line}{Style.RESET_ALL}")

    def info(self, msg): self._write("INFO", msg)
    def ok(self, msg): self._write("OK", msg)
    def warn(self, msg): self._write("WARN", msg)
    def err(self, msg): self._write("ERR", msg)


# ---------------- Scraper Class ----------------
class SonarRuleScraper:
    BASE = "https://rules.sonarsource.com"
    RULE_LINK_CSS = "a[href^='/{lang}/RSPEC-']"
    DETAILS_DESC_CSS = "section.RuleDetailsstyles__StyledDescription-sc-r16ye-7 p"
    STYLED_TAB_CSS = "div.RuleDescriptionstyles__StyledTab-sc-4203wg-4 p"
    RULE_TYPE_CSS = ".RuleDetailsstyles__StyledType-sc-r16ye-3"
    IMPACT_CSS = ".Impactstyles__StyledContainer-sc-1kgw359-0"

    def __init__(self, language: str, concurrency: int = 15, save_every: int = 20):
        self.language = language.strip().lower()
        self.concurrency = concurrency
        self.save_every = save_every
        self.logger = ColorLogger()
        self.session: aiohttp.ClientSession | None = None
        self.semaphore = asyncio.Semaphore(concurrency)
        self.results = []
        self.seen_urls = set()
        self.start_time = None
        self.timeout = ClientTimeout(total=20)

    async def _fetch(self, url: str):
        """Fetch HTML text or return None on failure."""
        try:
            async with self.semaphore:
                async with self.session.get(url) as resp:
                    if resp.status == 200:
                        return await resp.text()
                    self.logger.warn(f"{url} -> HTTP {resp.status}")
                    return None
        except asyncio.TimeoutError:
            self.logger.warn(f"{url} -> Timeout")
            return None
        except Exception as e:
            self.logger.err(f"{url} -> Exception: {e}")
            return None

    async def get_rule_links(self):
        """Collect all rule detail page URLs from the listing pages."""
        page = 1
        links = []
        self.logger.info(f"Start collecting rule links for '{self.language}'")
        while True:
            list_url = f"{self.BASE}/{self.language}/?page={page}"
            html = await self._fetch(list_url)
            if not html:
                self.logger.info(f"No response for page {page} (stopping).")
                break

            soup = BeautifulSoup(html, "html.parser")
            css = self.RULE_LINK_CSS.format(lang=self.language)
            found = []
            for a in soup.select(css):
                if (href := a.get("href")) and href not in self.seen_urls:
                    full = href if href.startswith("http") else (self.BASE + href)
                    self.seen_urls.add(href)
                    found.append(full)

            if not found:
                self.logger.info(f"No new links on page {page}. Stopping pagination.")
                break

            links.extend(found)
            self.logger.ok(f"Page {page}: {len(found)} new links (total {len(links)})")

            # heuristic: small last page
            if len(found) < 10:
                break
            page += 1
            await asyncio.sleep(0.35)

        # dedup + normalize to path (we want relative paths for parse)
        normalized = sorted({re.sub(r"^https?://[^/]+", "", u) for u in links})
        self.logger.info(f"Collected {len(normalized)} rule URLs.")
        return normalized

    def _parse_rule_html(self, html: str, url_path: str):
        """Parse HTML and produce dict with requested fields."""
        soup = BeautifulSoup(html, "html.parser")

        # rule_id from path
        rule_id = re.search(r"RSPEC-\d+", url_path)
        rule_id = rule_id.group(0) if rule_id else url_path.strip("/").split("/")[-1]

        # Title from h1
        title = (soup.find("h1") and soup.find("h1").get_text(strip=True)) or ""

        # description: main desc paragraphs + styled tab paragraphs
        desc_parts = [p.get_text(strip=True) for p in soup.select(self.DETAILS_DESC_CSS)]
        styled_parts = [p.get_text(strip=True) for p in soup.select(self.STYLED_TAB_CSS)]
        description = " ".join(desc_parts + styled_parts).strip()

        # fallback: if no styled parts, description stays as main; if still empty, set placeholder
        if not description:
            description = "Description not available."

        # rule_type
        rule_type = (soup.select_one(self.RULE_TYPE_CSS) and soup.select_one(self.RULE_TYPE_CSS).get_text(strip=True)) or "Unknown"

        # impacts
        impact = []
        for div in soup.select(self.IMPACT_CSS):
            if (txt := div.get_text(strip=True)):
                # use walrus operator to append non-empty
                impact.append(txt.capitalize())

        # keep unique impacts, preserve order
        seen = set()
        impact = [x for x in impact if not (x in seen or seen.add(x))]

        return {
            "rule_id": rule_id,
            "url": f"{self.BASE}{url_path}",
            "title": title,
            "description": description,
            "rule_type": rule_type,
            "impact": impact or ["Unspecified"]
        }

    async def process_one(self, url_path: str, idx: int, total: int):
        """Fetch + parse single rule and append to results with logging."""
        full = f"{self.BASE}{url_path}"
        self.logger.info(f"[{idx}/{total}] START {url_path}")
        html = await self._fetch(full)
        if not html:
            self.logger.warn(f"[{idx}/{total}] SKIP {url_path} (no html)")
            return None

        parsed = self._parse_rule_html(html, url_path)
        if parsed:
            self.results.append(parsed)
            self.logger.ok(f"[{idx}/{total}] DONE {parsed['rule_id']}")
            return parsed
        else:
            self.logger.warn(f"[{idx}/{total}] PARSE FAILED {url_path}")
            return None

    async def run(self, resume: bool = True):
        """Main runner: collect links, then process them in parallel, saving periodically."""
        self.start_time = time.time()
        connector = aiohttp.TCPConnector(limit_per_host=self.concurrency)
        async with aiohttp.ClientSession(timeout=self.timeout, connector=connector) as self.session:
            # resume if existing file present
            out_file = f"{self.language}_rules.json"
            if resume and os.path.exists(out_file):
                try:
                    with open(out_file, "r", encoding="utf-8") as f:
                        existing = json.load(f)
                    self.results = existing
                    self.logger.info(f"Resuming: loaded {len(existing)} existing rules from {out_file}")
                except Exception:
                    self.logger.warn("Failed to load existing file, starting fresh.")
                    self.results = []

            links = await self.get_rule_links()
            # determine which to process
            done_urls = {item["url"] for item in self.results}
            pending = [u for u in links if (self.BASE + u) not in done_urls]

            total = len(pending)
            self.logger.info(f"Will process {total} pending rules (concurrency={self.concurrency})")

            # process in parallel, but with controlled concurrency via semaphore in _fetch
            tasks = []
            for i, url_path in enumerate(pending, start=1):
                tasks.append(self.process_one(url_path, i, total))

            # run tasks in batches using gather with concurrency control inside _fetch
            # this lets the semaphore limit simultaneous HTTP requests
            for chunk_start in range(0, len(tasks), self.concurrency * 2):
                chunk = tasks[chunk_start: chunk_start + (self.concurrency * 2)]
                results = await asyncio.gather(*chunk)
                # periodic save
                if len(self.results) >= self.save_every:
                    self._save(out_file)
                    self.logger.info(f"Partial save: {len(self.results)} rules saved.")

            # final save
            self._save(out_file)
            elapsed = time.time() - self.start_time
            self.logger.ok(f"Completed. {len(self.results)} rules saved to {out_file} in {elapsed:.2f}s ({elapsed/60:.2f} min).")

    def _save(self, path: str):
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False)
        os.replace(tmp, path)


# ---------------- Entrypoint ----------------
def run_scraper(language: str, concurrency: int = 15, save_every: int = 20):
    scraper = SonarRuleScraper(language, concurrency=concurrency, save_every=save_every)
    # handle environments where loop already exists (Jupyter/Streamlit)
    try:
        asyncio.run(scraper.run(resume=True))
    except RuntimeError as e:
        if "asyncio.run() cannot be called from a running event loop" in str(e):
            loop = asyncio.get_event_loop()
            loop.run_until_complete(scraper.run(resume=True))
        else:
            raise


if __name__ == "__main__":
    lang = input("Enter language (e.g., ansible, java, python): ").strip()
    run_scraper(lang, concurrency=15, save_every=20)

