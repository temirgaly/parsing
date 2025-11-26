import os
from dotenv import load_dotenv

load_dotenv()

API_URL = "https://epsd.kz/Modules/Banks/Projects/GetBankProjects"
ROWS_PER_PAGE = 2000

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME", "maindb"),
    "user": os.getenv("DB_USER", "admin"),
    "password": os.getenv("DB_PASSWORD", "admin")
}
