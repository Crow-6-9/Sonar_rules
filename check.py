import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import json
import logging
from typing import List, Dict, Optional, Any
from colorama import Fore, Style, init
from concurrent.futures import ThreadPoolExecutor, as_completed

init(autoreset=True)

# List of all categories to scrape in one go
CATEGORIES: List[str] = [
    "annotation",
    "blocks",
    "design",
    "javadoc",
    "misc",
    "naming",
    "coding",
    "imports",
    "header",
    "metrics",
    "modifier",
    "regexp",
    "sizes",
    "whitespace"
]

# --- ColorFormatter and CheckstyleScraper classes remain largely the same ---

class ColorFormatter(logging.Formatter):
    COLORS = {
        logging.DEBUG: Fore.CYAN,
        logging.INFO: Fore.BLUE,
        logging.WARNING: Fore.YELLOW,
        logging.ERROR: Fore.RED,
        logging.CRITICAL: Fore.MAGENTA
    }

    def format(self, record):
        color = self.COLORS.get(record.levelno, "")
        message = super().format(record)
        return f"{color}{message}{Style.RESET_ALL}"


class CheckstyleScraper:
    def __init__(self, category: str, max_workers: int = 8):
        self.category = category.lower().strip()
        self.base_url = f"https://checkstyle.org/checks/{self.category}/"
        self.index_url = urljoin(self.base_url, "index.html")
        self.max_workers = max_workers

        # Logger setup - use a shared handler to avoid duplicate logs
        logger_name = self.category.capitalize()
        if not logging.getLogger(logger_name).handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(ColorFormatter("%(asctime)s [%(levelname)s] [%(name)s] %(message)s"))
            self.logger = logging.getLogger(logger_name)
            self.logger.setLevel(logging.INFO)
            self.logger.addHandler(handler)
        else:
            self.logger = logging.getLogger(logger_name)

        self.logger.info(f"Initialized scraper for category: {self.category}")

    # ------------------------ UTILITIES ------------------------

    def fetch_soup(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch and parse HTML content."""
        try:
            self.logger.debug(f"Fetching URL: {url}")
            if (response := requests.get(url, timeout=15)).status_code == 200:
                return BeautifulSoup(response.text, "html.parser")
        except Exception as e:
            self.logger.error(f"Failed to fetch {url}: {e}")
        return None

    # ------------------------ EXTRACTION LOGIC ------------------------

    def extract_checkstyle_info(self, soup: BeautifulSoup) -> Optional[str]:
        for p in soup.find_all("p"):
            if (text := p.get_text(" ", strip=True)).startswith("Since Checkstyle"):
                return text
        return None

    def extract_description(self, section: BeautifulSoup) -> Optional[str]:
        if not section:
            return None
        parts: List[str] = []

        for tag in section.find_all(["p", "div", "li", "code", "td", "th"], recursive=True):
            if (txt := tag.get_text(" ", strip=True)):
                parts.append(txt)

        for list_tag in section.find_all(["ul", "ol"], recursive=True):
            items = [li.get_text(" ", strip=True) for li in list_tag.find_all("li")]
            if items:
                parts.append("• " + "; ".join(items))

        for table in section.find_all("table"):
            rows = [
                " | ".join(td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"]))
                for tr in table.find_all("tr")
            ]
            if rows:
                parts.append("\n".join(rows))

        return "\n\n".join(parts).strip() if parts else None

    # def extract_properties(self, section: BeautifulSoup) -> List[Dict[str, str]]:
    #     properties = []
    #     if not section or not (table := section.find("table")):
    #         return properties

    #     for row in table.find_all("tr")[1:]:
    #         cols = [col.get_text(" ", strip=True) for col in row.find_all("td")]
    #         if len(cols) >= 5:
    #             properties.append({
    #                 "name": cols[0],
    #                 "description": cols[1],
    #                 "type": cols[2],
    #                 "default_value": cols[3],
    #                 "since": cols[4]
    #             })
    #     return properties

    def extract_examples(self, section: BeautifulSoup) -> List[str]:
        examples = []
        if not section:
            return examples
        for pre in section.find_all("pre"):
            if (code := pre.get_text("\n", strip=True)):
                examples.append(code)
        return examples

    def extract_rule_details(self, rule_url: str, rule_name: str) -> Optional[Dict]:
        """Extract all rule details from a single rule page."""
        self.logger.info(f"{Fore.GREEN}Extracting rule: {rule_name}{Style.RESET_ALL}")
        if not (soup := self.fetch_soup(rule_url)):
            return None

        checkstyle_info = self.extract_checkstyle_info(soup)
        desc_section = prop_section = ex_section = None

        for section in soup.find_all("section"):
            sid = section.get("id", "").lower()
            if "_description" in sid:
                desc_section = section
            # elif "_properties" in sid:
            #     prop_section = section
            if "_examples" in sid:
                ex_section = section
            else: 
                pass

        description = self.extract_description(desc_section)
        # properties = self.extract_properties(prop_section)
        examples = self.extract_examples(ex_section)

        return {
            "title": rule_name,
            "checkstyle": checkstyle_info,
            "description": description,
            # "properties": properties or None,
            "examples": examples or None
        }

    # ------------------------ PARALLEL SCRAPER (MODIFIED) ------------------------

    def scrape(self) -> Dict[str, Any]:
        """
        Main scraping method for one category.
        Returns the data dictionary instead of writing a file.
        """
        self.logger.info(f"Fetching index page: {self.index_url}")
        if not (soup := self.fetch_soup(self.index_url)):
            self.logger.error("Failed to load index page.")
            return {}

        rules: List[Dict] = []
        rule_links = []

        for td in soup.find_all("td"):
            if not (a_tag := td.find("a")) or not a_tag.has_attr("href"):
                continue
            rule_url = urljoin(self.base_url, a_tag["href"].split("#")[0])
            rule_name = a_tag.get_text(strip=True)
            rule_links.append((rule_url, rule_name))

        total_rules = len(rule_links)
        self.logger.info(f"Found {total_rules} rules. Starting parallel extraction with {self.max_workers} threads...")

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self.extract_rule_details, url, name): name for url, name in rule_links}
            for future in as_completed(futures):
                rule_name = futures[future]
                try:
                    if result := future.result():
                        rules.append(result)
                except Exception as e:
                    self.logger.error(f"Error processing rule {rule_name}: {e}")

        final_json = {
            "category": self.category.capitalize(),
            "total_rules": len(rules),
            "rules": rules
        }

        self.logger.info(
            f"{Fore.GREEN}✅ Extraction complete for {self.category} — {len(rules)} rules collected.{Style.RESET_ALL}"
        )
        return final_json


# ------------------------ ENTRY POINT (MODIFIED) ------------------------

def run_scraper_for_category(category: str) -> Optional[Dict]:
    """Initializes and runs the scraper for a single category."""
    try:
        scraper = CheckstyleScraper(category, max_workers=8)
        return scraper.scrape()
    except Exception as e:
        print(f"{Fore.RED}CRITICAL ERROR for category {category}: {e}{Style.RESET_ALL}")
        return None

if __name__ == "__main__":
    
    print(f"{Fore.YELLOW}*** Starting parallel scraping for {len(CATEGORIES)} Checkstyle categories ***{Style.RESET_ALL}")

    MAX_GLOBAL_WORKERS = 10 
    combined_data: List[Dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=MAX_GLOBAL_WORKERS) as executor:
        # Submit scraping task for each category
        futures = {executor.submit(run_scraper_for_category, cat): cat for cat in CATEGORIES}
        
        for future in as_completed(futures):
            category_name = futures[future]
            if result := future.result():
                combined_data.append(result)
                print(f"{Fore.CYAN}--- Finished collecting data for category: {category_name} ---{Style.RESET_ALL}")

    # --- FINAL CONSOLIDATION STEP ---
    final_output = {
        "checkstyle_categories": combined_data
    }
    
    combined_filename = "all_checkstyle_rules_combined.json"
    
    # Sort the list of category data alphabetically for cleaner output
    final_output["checkstyle_categories"].sort(key=lambda x: x.get("category", ""))
    
    with open(combined_filename, "w", encoding="utf-8") as f:
        json.dump(final_output, f, indent=4, ensure_ascii=False)

    print(f"\n{Fore.GREEN}✨ ALL SCRAPING COMPLETE! Total categories processed: {len(combined_data)}. Data saved to a single file: {combined_filename}.{Style.RESET_ALL}")
