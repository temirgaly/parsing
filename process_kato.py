from data import get_empty_kato_records, find_kato_code, update_kato_code
from scraper import scrape_kato_from_project
from kato_matcher import KatoMatcher
from concurrent.futures import ThreadPoolExecutor, as_completed


def process_kato_codes():
    """
    Main function to process KATO codes:
    1. Get records with empty katoCode
    2. Scrape KATO naming from partial view
    3. Find matching code from katoCode table (hierarchical)
    4. Update psdinfo with the code
    """
    print("=== Starting KATO Code Processing ===\n")

    # Initialize KATO matcher (loads data once, caches in memory)
    print("Initializing KATO matcher...")
    matcher = KatoMatcher()
    print()

    # Step 1: Get records with empty katoCode
    print("Step 1: Fetching records with empty katoCode...")
    records = get_empty_kato_records()
    print(f"Found {len(records)} records to process\n")

    if not records:
        print("No records to process. Done!")
        return

    success_count = 0
    fail_count = 0

    def process_single_record(record_info):
        """Process a single record"""
        idx, psdid, title = record_info

        print(f"[{idx}/{len(records)}] Processing psdid: {psdid}")
        print(f"  Title: {title[:60]}...")

        # Step 2: Scrape KATO naming
        kato_names = scrape_kato_from_project(psdid)

        if not kato_names:
            print(f"  ❌ Could not scrape KATO names")
            return False

        print(f"  Found KATO: {' -> '.join(kato_names)}")

        # Step 3: Find matching code (using hierarchical matcher)
        kato_code = find_kato_code(kato_names, matcher=matcher)

        if not kato_code:
            print(f"  ❌ Could not find matching KATO code in database")
            return False

        print(f"  Matched code: {kato_code}")

        # Step 4: Update database
        update_kato_code(psdid, kato_code)
        print(f"  ✓ Updated successfully")
        return True

    # Process records in parallel with max 5 workers
    with ThreadPoolExecutor(max_workers=5) as executor:
        # Prepare data for parallel processing
        record_infos = [(idx, psdid, title) for idx, (psdid, title) in enumerate(records, 1)]

        # Submit all tasks
        futures = {executor.submit(process_single_record, info): info for info in record_infos}

        # Collect results as they complete
        for future in as_completed(futures):
            try:
                success = future.result()
                if success:
                    success_count += 1
                else:
                    fail_count += 1
            except Exception as e:
                print(f"  ❌ Exception: {e}")
                fail_count += 1

    print(f"\n=== Processing Complete ===")
    print(f"Success: {success_count}")
    print(f"Failed: {fail_count}")
    print(f"Total: {len(records)}")


if __name__ == "__main__":
    process_kato_codes()
