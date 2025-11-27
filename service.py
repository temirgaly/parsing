import requests
from typing import List, Dict
from config import API_URL, ROWS_PER_PAGE
from models import Project


def fetch_projects_page(page: int) -> Dict:
    """
    Fetch a single page of projects from the API.

    Args:
        page: Page number to fetch

    Returns:
        Dict containing API response with total pages, records, and rows
    """
    payload = {
        "page": str(page),
        "rows": str(ROWS_PER_PAGE),
        "sidx": "id",
        "sord": "desc"
    }

    try:
        response = requests.post(API_URL, data=payload)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching page {page}: {e}")
        return None


def fetch_all_projects() -> List[Project]:
    """
    Fetch all projects from all pages.

    Returns:
        List of Project objects
    """
    all_projects = []

    # Fetch first page to get total pages
    first_page = fetch_projects_page(1)
    if not first_page:
        return all_projects

    total_pages = first_page.get("total", 1)
    print(f"Total pages: {total_pages}")

    # Process first page
    page1_count = 0
    for row in first_page.get("rows", []):
        if 'Кызылорд' in row['cell'][2] and row['cell'][8] == 'Согласован':
            project = Project(row)
            all_projects.append(project)
            page1_count += 1


    print(f"Page 1: Fetched {page1_count} filtered projects (out of {len(first_page.get('rows', []))} total)")

    # Fetch remaining pages
    for page in range(2, total_pages + 1):
        page_data = fetch_projects_page(page)
        if page_data:
            rows = page_data.get("rows", [])
            page_count = 0
            for row in rows:
                if 'Кызылорд' in row['cell'][2] and row['cell'][8] == 'Согласован':
                    project = Project(row)
                    all_projects.append(project)
                    page_count += 1
            print(f"Page {page}: Fetched {page_count} filtered projects (out of {len(rows)} total)")

    print(f"Total projects fetched: {len(all_projects)}")
    return all_projects

if __name__ == "__main__":
    fetch_all_projects()