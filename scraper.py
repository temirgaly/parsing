import requests
from bs4 import BeautifulSoup
from typing import List, Optional


def scrape_kato_from_project(psdid: int) -> Optional[List[str]]:
    """
    Scrape KATO naming from project partial view.

    Args:
        psdid: Project ID

    Returns:
        List of KATO names (e.g., ["Республика Казахстан", "область Жетісу", "Каратальский район"]) or None
    """
    url = f"https://www.epsd.kz/Modules/Banks/Projects/View/{psdid}"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'lxml')

        # a) Find table with class="simple table table-bordered"
        table = soup.find('table', class_='simple table table-bordered')

        if not table:
            print(f"  ⚠ Table not found for psdid {psdid}")
            return None

        # b) Find the row with "Местоположение объекта"
        rows = table.find_all('tr')
        for row in rows:
            tds = row.find_all('td')
            if len(tds) >= 2:
                first_td_text = tds[0].get_text(strip=True)
                if 'Местоположение объекта' in first_td_text:
                    # c) Get text from second td
                    kato_text = tds[1].get_text(strip=True)

                    # Remove trailing semicolon and split by comma
                    kato_text = kato_text.rstrip(';').strip()

                    # Split by comma: "Республика Казахстан, область Жетісу, Каратальский район"
                    kato_names = [name.strip() for name in kato_text.split(',')]

                    return kato_names if kato_names else None

        print(f"  ⚠ 'Местоположение объекта' not found for psdid {psdid}")
        return None

    except requests.exceptions.RequestException as e:
        print(f"  ❌ HTTP Error scraping psdid {psdid}: {e}")
        return None
    except Exception as e:
        print(f"  ❌ Parsing error for psdid {psdid}: {e}")
        return None
