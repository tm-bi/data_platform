from __future__ import annotations
import pyodbc
from _bootstrap import setup_sys_path

setup_sys_path()

from common.settings import settings

def main() -> int:
    conn_str = (
        f"DRIVER={{{settings.mssql_driver}}};"
        f"SERVER={settings.mssql_host},{settings.mssql_port};"
        f"DATABASE={settings.mssql_db};"
        f"UID={settings.mssql_user};"
        f"PWD={settings.mssql_password};"
        f"Encrypt={settings.mssql_encrypt};"
        f"TrustServerCertificate={settings.mssql_trust_cert};"
    )

    print("[QUALITY][MSSQL] conectando...")
    with pyodbc.connect(conn_str, timeout=10) as conn:
        cur = conn.cursor()
        cur.execute("select 1;")
        val = cur.fetchone()[0]
    print(f"[QUALITY][MSSQL] OK (select 1 => {val})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
