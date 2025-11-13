#!/usr/bin/env python3
"""
pmd_scraper.py

Usage:
    python pmd_scraper.py apex
or
    python pmd_scraper.py
    (then it will ask: Enter language name:)
"""
from __future__ import annotations
import asyncio
import aiohttp
import sys
import json
import logging
import re
from typing import List, Dict, Optional
from bs4 import BeautifulSoup, Tag, NavigableString
from colorama import Fore, Style, init as colorama_init

# -----------------------
# Configuration
# -----------------------
BASE_URL = "https://pmd.github.io/pmd/"
INDEX_TEMPLATE = BASE_URL + "pmd_rules_{language}.html"
RULESET_TEMPLATE = BASE_URL + "pmd_rules_{language}_{ruleset}.html"
# fallback/order to ensure keys are present
FALLBACK_RULESETS = [
    "bestpractices", "codestyle", "design", "documentation",
    "errorprone", "performance", "security", "multithreading"
]
CONCURRENCY = 6
HEADERS = {"User-Agent": "pmd-rules-scraper/1.0 (+https://github.com)"}

# -----------------------
# Logger
# -----------------------
class ColorFormatter(logging.Formatter):
    def format(self, record):
        level = record.levelno
        if level >= logging.CRITICAL:
            color = Fore.MAGENTA
        elif level >= logging.ERROR:
            color = Fore.RED
        elif level >= logging.WARNING:
            color = Fore.YELLOW
        elif level >= logging.INFO:
            color = Fore.BLUE
        else:
            color = Fore.CYAN
        msg = super().format(record)
        return f"{color}{msg}{Style.RESET_ALL}"

def setup_logger(name="pmd_scraper") -> logging.Logger:
    colorama_init(autoreset=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    fmt = "[%(levelname)s] %(message)s"
    ch.setFormatter(ColorFormatter(fmt))
    if not logger.handlers:
        logger.addHandler(ch)
    return logger

logger = setup_logger()

# -----------------------
# Helpers
# -----------------------
def normalize_ruleset_label_from_href(href: str) -> str:
    # href like "#best-practices" => "bestpractices"
    if not href:
        return ""
    h = href.lstrip("#").strip()
    return re.sub(r'[^A-Za-z0-9]', '', h).lower()

def normalize_ruleset_label_from_text(text: str) -> str:
    # fallback if href missing; e.g. "Best Practices" -> "bestpractices"
    if not text:
        return ""
    return re.sub(r'[^A-Za-z0-9]', '', text).lower()

async def fetch(session: aiohttp.ClientSession, url: str) -> Optional[str]:
    try:
        async with session.get(url, headers=HEADERS, timeout=30) as resp:
            if resp.status == 200:
                return await resp.text()
            logger.warning(f"HTTP {resp.status} for {url}")
            return None
    except asyncio.TimeoutError:
        logger.warning(f"Timeout fetching {url}")
    except Exception as e:
        logger.error(f"Error fetching {url}: {e}")
    return None

# -----------------------
# Parsing index (language) -> rulesets
# -----------------------
def parse_index_for_rulesets(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    toc = soup.select_one("#toc")
    if not toc:
        # sometimes toc inside inline-toc -> #inline-toc #toc
        toc = soup.select_one("div#inline-toc div#toc")
    if not toc:
        logger.debug("No #toc found on index page.")
        return []
    found = []
    for a in toc.select("a"):
        href = a.get("href", "")
        text = a.get_text(strip=True)
        # skip 'additional-rulesets' or anything that contains 'additional'
        if "additional" in (href + text).lower():
            continue
        norm = normalize_ruleset_label_from_href(href) or normalize_ruleset_label_from_text(text)
        if norm:
            found.append(norm)
    # dedupe but preserve order
    seen = set()
    out = []
    for r in found:
        if r not in seen:
            seen.add(r)
            out.append(r)
    return out

# -----------------------
# Helpers for rule block parsing
# -----------------------
def _collect_rule_block(h2_tag: Tag) -> List[Tag]:
    """
    Collect all siblings between this h2 and the next h2 (the rule block).
    Returns a list with h2_tag as first element and subsequent elements until next rule-h2.
    """
    block: List[Tag] = [h2_tag]
    for sib in h2_tag.next_siblings:
        if isinstance(sib, NavigableString):
            # skip pure whitespace nodes
            if not str(sib).strip():
                continue
            # wrap navigable string into a Tag-like object as string
            block.append(sib)
            continue
        # sib is Tag
        if isinstance(sib, Tag) and sib.name == "h2":
            # stop at next h2
            # allow if h2 is not a rule header (but generally next h2 is next rule)
            break
        block.append(sib)
    return block

def _text_of_pre_elements(soup_block: BeautifulSoup) -> str:
    """Collect text from <pre> tags inside the block (examples)."""
    examples = []
    # There may be pre tags inside divs with class language-java or directly pre
    for pre in soup_block.find_all("pre"):
        txt = pre.get_text("\n", strip=True)
        if txt:
            examples.append(txt)
    # sometimes code samples present in <div class="language-java ..."><div class="highlight"><pre>...
    if not examples:
        for div in soup_block.find_all("div", class_=lambda c: c and "language-java" in c or "language-xml" in c or "language-xpath" in c):
            pre = div.find("pre")
            if pre:
                txt = pre.get_text("\n", strip=True)
                if txt:
                    examples.append(txt)
    return "\n\n".join(examples).strip()

def parse_rule_block(block_elements: List) -> Dict[str, str]:
    """
    Given block elements (h2 and following tags until next h2),
    extract title (use h2 id if present), since, priority, description, examples.
    Excludes the Java class reference and XPath expressions (per requirement).
    """
    # Build a small soup out of the block for easier xpath/selector use
    html = "".join(str(x) for x in block_elements)
    soup_block = BeautifulSoup(html, "html.parser")

    # Title: prefer h2 id (gives canonical rule id) else text stripped
    h2 = soup_block.find("h2")
    title = ""
    if h2:
        title = (h2.get("id") or h2.get_text(strip=True) or "").strip()
        # sometimes id includes anchor text; normalize to lower-case id style like 'apexassertionsshouldincludemessage'
        title = title.strip()

    # Initialize fields
    since = ""
    priority = ""
    description = ""
    examples = ""

    # Walk paragraphs to get since, priority, first descriptive paragraph
    # We'll iterate elements in order inside the block
    for el in soup_block.find_all(["p", "div", "table"], recursive=False):
        # skip the rule-definition lines that mention "This rule is defined by the following Java class"
        strong = el.find("strong")
        strong_text = strong.get_text(strip=True).rstrip(":") if strong else ""
        if strong_text.lower().startswith("since"):
            # remainder of paragraph after the strong
            full = el.get_text(" ", strip=True)
            # remove 'Since:' from full
            remainder = re.sub(r'^\s*Since:\s*', '', full, flags=re.IGNORECASE).strip()
            since = remainder
            continue
        if strong_text.lower().startswith("priority"):
            full = el.get_text(" ", strip=True)
            remainder = re.sub(r'^\s*Priority:\s*', '', full, flags=re.IGNORECASE).strip()
            priority = remainder
            continue
        # skip paragraphs that describe 'This rule is defined by' (class or xpath)
        if strong_text.lower().startswith("this rule") or "defined by" in el.get_text(" ", strip=True).lower():
            continue
        # a descriptive paragraph (first non-meta paragraph)
        # also skip the summary top-line in ruleset page (we exclude summary as you asked)
        # choose the first paragraph that contains useful descriptive text
        text = el.get_text(" ", strip=True)
        if text and not description:
            # avoid picking up the 'Use this rule by referencing it' paragraphs
            if text.lower().startswith("use this rule"):
                continue
            # avoid property tables (skip table)
            description = text
            # don't break — continue to allow later examples collection
            # but we captured the description already
    # Examples: collect text from <pre> tags inside the block (join multiple)
    examples = _text_of_pre_elements(soup_block)

    # Final cleanups: if description ends up being the 'This rule...' fallback, blank it
    if description and ("use this rule" in description.lower() or "this rule is defined" in description.lower()):
        description = ""

    # Return fields
    return {
        "title": title,
        "since": since,
        "priority": priority,
        "description": description,
        "examples": examples
    }

def parse_ruleset_page(html: str) -> List[Dict]:
    """Given a ruleset page HTML, extract every rule's structured info."""
    soup = BeautifulSoup(html, "html.parser")
    # primary content container: .post-content (fallback to body)
    content = soup.select_one("div.post-content") or soup
    rules = []
    # find all h2 tags that represent a rule header
    for h2 in content.find_all("h2"):
        classes = " ".join(h2.get("class") or []).lower()
        # treat header as rule header if class contains 'clickable' (handles clickable-header or clickableheader)
        if "clickable" not in classes and "clickable-header" not in classes and "clickableheader" not in classes:
            # still some pages may omit class; as heuristic, require h2 to have an id (the rule id)
            if not h2.get("id"):
                continue
        # collect block and parse
        block = _collect_rule_block(h2)
        rule = parse_rule_block(block)
        # only include if title present
        if rule.get("title"):
            rules.append(rule)
            logger.debug(f"Extracted rule: {rule['title']}")
    return rules

# -----------------------
# Async scrape flow
# -----------------------
async def scrape_language(language: str) -> Dict:
    out: Dict = {"language": language, "rulesets": {}}
    index_url = INDEX_TEMPLATE.format(language=language)
    logger.info(f"Fetching available rulesets from: {index_url}")

    conn = aiohttp.TCPConnector(limit_per_host=CONCURRENCY)
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(connector=conn, timeout=timeout) as session:
        index_html = await fetch(session, index_url)
        if index_html:
            detected = parse_index_for_rulesets(index_html)
            if not detected:
                logger.warning("TOC found but no rulesets extracted; using fallback list.")
                detected = FALLBACK_RULESETS.copy()
        else:
            logger.warning("Index page not found; using fallback list.")
            detected = FALLBACK_RULESETS.copy()

        logger.info(f"Detected rulesets: {', '.join(detected)}")
        sem = asyncio.Semaphore(CONCURRENCY)

        async def fetch_and_parse(rs_name: str):
            url = RULESET_TEMPLATE.format(language=language, ruleset=rs_name)
            async with sem:
                logger.info(f"Fetching ruleset: {rs_name} -> {url}")
                html = await fetch(session, url)
                if not html:
                    logger.warning(f"Page not found for {rs_name}, adding empty list.")
                    out["rulesets"][rs_name] = []
                    return
                rules = parse_ruleset_page(html)
                out["rulesets"][rs_name] = rules
                logger.info(f"Completed {rs_name} ({len(rules)} rules)")

        tasks = [asyncio.create_task(fetch_and_parse(rs)) for rs in detected]
        # ensure we await all tasks
        await asyncio.gather(*tasks)

    # ensure fallback keys exist (so final JSON keys are stable)
    for rs in FALLBACK_RULESETS:
        out["rulesets"].setdefault(rs, [])
    return out

# -----------------------
# CLI Entry
# -----------------------
def main():
    # accept CLI arg or ask interactively
    if len(sys.argv) > 1:
        language = sys.argv[1].strip().lower()
    else:
        language = input(Fore.GREEN + "Enter language name: " + Style.RESET_ALL).strip().lower()

    if not language:
        print("❌ Language name is required.")
        sys.exit(1)

    logger.info(f"Starting PMD scraper for language: {language}")

    # run the async scraper
    try:
        data = asyncio.run(scrape_language(language))
    except RuntimeError as e:
        # In interactive environments this can fail with 'asyncio.run() cannot be called...'
        # but when running as a script from the shell this should not happen.
        logger.error("RuntimeError running asyncio.run(): " + str(e))
        logger.error("If you run this inside an interactive environment (Jupyter), run the script from a terminal instead.")
        raise

    out_file = f"opp_{language}_rules.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logger.info(f"✅ Wrote output to {out_file}")

if __name__ == "__main__":
    main()
