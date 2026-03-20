import pymysql, os
from dotenv import load_dotenv
load_dotenv("/opt/automacoes/cliquedf/operacional/.env")

def get_ixc_conn():
    return pymysql.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", 3306)),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        database=os.getenv("DB_NAME"),
        connect_timeout=10,
        cursorclass=pymysql.cursors.DictCursor,
        charset="utf8mb4",
    )
