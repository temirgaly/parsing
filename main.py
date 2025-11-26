from service import fetch_all_projects
from data import ensure_unique_constraint, sync_projects


def main():
    print("=== Step 2: Fetching projects from API ===")
    projects = fetch_all_projects()
    print(f"\nFetched {len(projects)} projects")

    if projects and len(projects) > 0:
        print("\nFirst project example:")
        print(projects[0].__dict__)

        print("\n=== Step 6: Syncing data to PostgreSQL ===")
        ensure_unique_constraint()
        sync_projects(projects)
        print("\nSync completed!")
    else:
        print("No projects to sync")


if __name__ == "__main__":
    main()
