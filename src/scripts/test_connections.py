from __future__ import annotations

import sys
import pyodbc
from firebird.driver import connect as fb_connect
from psycopg import connect as pg_connect
from _bootstrap import setup_sys_path

setup_sys_path()

# ajuste o import conforme seu PYTHONPATH / modo de execução
from common.settings import settings

def test_postgres() -> None:
    print("[POSTGRES] conectando...")
    with pg_connect(settings.pg_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute("select 1;")
            val = cur.fetchone()[0]
    print(f"[POSTGRES] OK (select 1 => {val})")


def test_firebird() -> None:
    print("[FIREBIRD] conectando...")
    # Firebird: DSN no formato host/port:path (ou host:path se não usar porta custom)
    dsn = f"{settings.firebird_host}/{settings.firebird_port}:{settings.firebird_db}"

    try: 
        with fb_connect(
            database=dsn,
            user=settings.firebird_user,
            password=settings.firebird_password,
            charset=settings.firebird_charset,
        ) as conn:
            cur = conn.cursor()
            cur.execute("select 1 from rdb$database;")
            val = cur.fetchone()[0]
        print(f"[FIREBIRD] OK (select 1 => {val})")
    except:
        print("não funcionou")          

def test_mssql() -> None:
    print("[MSSQL] conectando...")
    encrypt = settings.mssql_encrypt
    trust_cert = settings.mssql_trust_cert
    conn_str = (
        f"DRIVER={{{settings.mssql_driver}}};"
        f"SERVER={settings.mssql_host},{settings.mssql_port};"
        f"DATABASE={settings.mssql_db};"
        f"UID={settings.mssql_user};"
        f"PWD={settings.mssql_password};"
        f"Encrypt={encrypt};"
        f"TrustServerCertificate={trust_cert};"
    )

    with pyodbc.connect(conn_str, timeout=10) as conn:
        cur = conn.cursor()
        cur.execute("select 1;")
        val = cur.fetchone()[0]
    print(f"[MSSQL] OK (select 1 => {val})")


def main() -> int:
    try:
        test_postgres()
        test_firebird()
        test_mssql()
    except Exception as exc:
        print("\n[ERRO] falhou algum teste de conexão.")
        print(f"Tipo: {type(exc).__name__}")
        print(f"Detalhe: {exc}")
        return 1

    print("\n[TUDO OK] Conexões validadas.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
