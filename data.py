import psycopg2
from psycopg2.extras import execute_batch, RealDictCursor
from config import DB_CONFIG


def get_connection():
    """
    Create and return a database connection.
    """
    return psycopg2.connect(**DB_CONFIG)


def create_table():
    """
    Create the psdinfo table if it doesn't exist.
    """
    conn = get_connection()
    cursor = conn.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS psdinfo (
        id BIGSERIAL PRIMARY KEY,
        psdid INT4,
        title TEXT,
        client VARCHAR(500),
        designer VARCHAR(500),
        report_number VARCHAR(100),
        report_date DATE,
        review_place VARCHAR(500),
        status VARCHAR(100),
        cost NUMERIC(18, 2),
        cost_description TEXT,
        created_at TIMESTAMPTZ DEFAULT now(),
        updated_at TIMESTAMPTZ DEFAULT now()
    );
    """

    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()


def ensure_unique_constraint():
    """
    Add UNIQUE constraint on psdid if it doesn't exist.
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        add_constraint_query = """
        ALTER TABLE psdinfo ADD CONSTRAINT psdinfo_psdid_unique UNIQUE (psdid);
        """
        cursor.execute(add_constraint_query)
        conn.commit()
        print("Added UNIQUE constraint on psdid")
    except psycopg2.errors.DuplicateTable:
        conn.rollback()
        print("UNIQUE constraint already exists")
    except Exception as e:
        conn.rollback()
        print(f"Note: {e}")
    finally:
        cursor.close()
        conn.close()


def sync_projects(projects):
    """
    Synchronize projects with database:
    1. Delete projects from DB that don't exist in fresh API data
    2. Update existing projects
    3. Insert new projects

    Args:
        projects: List of Project objects from API
    """
    if not projects:
        print("No projects to sync")
        return

    conn = get_connection()
    cursor = conn.cursor()

    try:
        fresh_psd_ids = [p.project_id for p in projects]
        # Step 1: Delete outdated records (exist in DB but not in fresh data)
        delete_query = """
        DELETE FROM psdinfo
        WHERE psdid NOT IN %s
        """
        cursor.execute(delete_query, (tuple(fresh_psd_ids),))
        deleted_count = cursor.rowcount
        print(f"Deleted {deleted_count} outdated records")

        # Step 2 & 3: Upsert (Update or Insert)
        upsert_query = """
        INSERT INTO psdinfo (psdid, title, client, designer, report_number, report_date,
                             review_place, status, cost_description, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (psdid)
        DO UPDATE SET
            title = EXCLUDED.title,
            client = EXCLUDED.client,
            designer = EXCLUDED.designer,
            report_number = EXCLUDED.report_number,
            report_date = EXCLUDED.report_date,
            review_place = EXCLUDED.review_place,
            status = EXCLUDED.status,
            cost_description = EXCLUDED.cost_description,
            updated_at = now()
        """

        batch_data = []
        for project in projects:
            batch_data.append((
                project.project_id,
                project.project_name,
                project.customer,
                project.contractor,
                project.contract_number,
                project.contract_date,
                project.branch,
                project.status,
                project.note
            ))

        execute_batch(cursor, upsert_query, batch_data)
        print(f"Upserted {len(batch_data)} projects")

        conn.commit()
        print("Database synchronized successfully")

    except Exception as e:
        conn.rollback()
        print(f"Error syncing projects: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def get_empty_kato_records():
    """
    Get all records where katoCode is NULL or empty.

    Returns:
        List of tuples (psdid, title)
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        query = """
        SELECT psdid, title
        FROM psdinfo
        WHERE katoCode IS NULL OR katoCode = ''
        """
        cursor.execute(query)
        records = cursor.fetchall()
        return records
    finally:
        cursor.close()
        conn.close()


def find_kato_code(kato_names, matcher=None):
    """
    Find KATO code by matching names from the katoCode table using hierarchical matching.

    Args:
        kato_names: List of KATO names (e.g., ["Республика Казахстан", "Кызылорда", "Шиели"])
        matcher: KatoMatcher instance (if None, uses legacy matching)

    Returns:
        KATO code or None
    """
    if not kato_names:
        return None

    # Use new hierarchical matcher if provided
    if matcher:
        return matcher.find_kato_code(kato_names)

    # Legacy fallback (simple matching)
    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Start from the most specific (last) name and work backwards
        for name in reversed(kato_names):
            query = """
            SELECT code FROM katoCode
            WHERE LOWER(TRIM(name_ru)) = LOWER(TRIM(%s))
            ORDER BY level DESC
            LIMIT 1
            """
            cursor.execute(query, (name,))
            result = cursor.fetchone()
            if result:
                return result[0]

        return None
    finally:
        cursor.close()
        conn.close()


def update_kato_code(psdid, kato_code):
    """
    Update katoCode for a specific psdid.

    Args:
        psdid: Project ID
        kato_code: KATO code to update
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        query = """
        UPDATE psdinfo
        SET katoCode = %s, updated_at = now()
        WHERE psdid = %s
        """
        cursor.execute(query, (kato_code, psdid))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error updating psdid {psdid}: {e}")
    finally:
        cursor.close()
        conn.close()
