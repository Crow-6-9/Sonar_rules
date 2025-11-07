import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import json
import logging
from typing import List, Dict, Optional
from colorama import Fore, Style, init

init(autoreset=True)


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
    def __init__(self, category: str):
        self.category = category.lower().strip()
        self.base_url = f"https://checkstyle.org/checks/{self.category}/"
        self.index_url = urljoin(self.base_url, "index.html")

        # Logger setup with color
        handler = logging.StreamHandler()
        handler.setFormatter(ColorFormatter("%(asctime)s [%(levelname)s] %(message)s"))
        self.logger = logging.getLogger(self.category.capitalize())
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(handler)

        self.logger.info(f"Initialized scraper for category: {self.category}")

    # ------------------------ UTILITIES ------------------------

    def fetch_soup(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch and parse HTML."""
        try:
            self.logger.debug(f"Fetching URL: {url}")
            if (response := requests.get(url, timeout=15)).status_code == 200:
                return BeautifulSoup(response.text, "html.parser")
        except Exception as e:
            self.logger.error(f"Failed to fetch {url}: {e}")
        return None

    # ------------------------ EXTRACTION LOGIC ------------------------

    def extract_checkstyle_info(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract 'Since Checkstyle X.X' text."""
        for p in soup.find_all("p"):
            if (text := p.get_text(" ", strip=True)).startswith("Since Checkstyle"):
                return text
        return None

    def extract_description(self, section: BeautifulSoup) -> Optional[str]:
        """Extract paragraphs, code, lists, and table text."""
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

    def extract_properties(self, section: BeautifulSoup) -> List[Dict[str, str]]:
        """Extract table of properties."""
        properties = []
        if not section or not (table := section.find("table")):
            return properties

        for row in table.find_all("tr")[1:]:
            cols = [col.get_text(" ", strip=True) for col in row.find_all("td")]
            if len(cols) >= 5:
                properties.append({
                    "name": cols[0],
                    "description": cols[1],
                    "type": cols[2],
                    "default_value": cols[3],
                    "since": cols[4]
                })
        return properties

    def extract_examples(self, section: BeautifulSoup) -> List[str]:
        examples = []
        if not section:
            return examples
        for pre in section.find_all("pre"):
            if (code := pre.get_text("\n", strip=True)):
                examples.append(code)
        return examples

    def extract_rule_details(self, rule_url: str, rule_name: str) -> Optional[Dict]:
        self.logger.info(f"{Fore.GREEN}Extracting rule: {rule_name}{Style.RESET_ALL}")
        if not (soup := self.fetch_soup(rule_url)):
            return None

        checkstyle_info = self.extract_checkstyle_info(soup)
        desc_section = prop_section = ex_section = None

        for section in soup.find_all("section"):
            sid = section.get("id", "").lower()
            if "_description" in sid:
                desc_section = section
            elif "_properties" in sid:
                prop_section = section
            elif "_examples" in sid:
                ex_section = section

        description = self.extract_description(desc_section)
        properties = self.extract_properties(prop_section)
        examples = self.extract_examples(ex_section)

        return {
            "title": rule_name,
            "checkstyle": checkstyle_info,
            "description": description,
            "properties": properties or None,
            "examples": examples or None
        }

    # ------------------------ MAIN SCRAPER LOGIC ------------------------

    def scrape(self) -> Dict:
        self.logger.info(f"Fetching index page: {self.index_url}")
        if not (soup := self.fetch_soup(self.index_url)):
            self.logger.error("Failed to load index page.")
            return {}

        rules_data = []

        for td in soup.find_all("td"):
            if not (a_tag := td.find("a")) or not a_tag.has_attr("href"):
                continue

            rule_url = urljoin(self.base_url, a_tag["href"].split("#")[0])
            rule_name = a_tag.get_text(strip=True)

            if details := self.extract_rule_details(rule_url, rule_name):
                rules_data.append(details)

        final_json = {
            "checks": self.category.capitalize(),
            "total_rules": len(rules_data),
            "rules": rules_data
        }

        filename = f"{self.category}_checks.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(final_json, f, indent=4, ensure_ascii=False)

        self.logger.info(f"{Fore.GREEN}✅ Extraction complete — {len(rules_data)} rules saved to {filename}{Style.RESET_ALL}")
        return final_json


# ------------------------ ENTRY POINT ------------------------

if __name__ == "__main__":
    category = input("Enter Checkstyle category (e.g. annotation, coding, naming): ").strip().lower()
    scraper = CheckstyleScraper(category)
    scraper.scrape()
