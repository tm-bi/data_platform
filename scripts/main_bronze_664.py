from __future__ import annotations

import csv
import os
import re
import uuid
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import List

import chardet
import psycopg2
from psycopg2.extensions import connection as PgConnection
from dotenv import load_dotenv

@dataclass(frozen=True)
class PgConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str

def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'

def sanitize_table_name(stem: str) -> str:
    """
    Garante que o nome vire um identificador válido no Postgres.
    Ex.: '202511_270' ok. Se começar com número, vamos prefixar 't_'.
    """
    s = stem.strip().lower().replace("\ufeff", "")
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-z0-9_]", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        s = "t_unnamed"
    if s[0].isdigit():
        s = f"t_{s}"
    return s

def detect_encoding(file_path: Path, sample_bytes: int = 200_000) -> str:
    raw = file_path.read_bytes()[:sample_bytes]
    guess = chardet.detect(raw)
    enc = (guess.get("encoding") or "utf-8").strip()
    if enc.lower() in {"iso-8859-1", "latin-1", "latin1"}:
        return "latin1"
    return enc

def ensure_schema(conn: PgConnection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {qident(schema)};")
    conn.commit()

def drop_table_if_exists(conn: PgConnection, schema: str, table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {qident(schema)}.{qident(table)};")
    conn.commit()

def ensure_raw_table(conn: PgConnection, schema: str, table: str) -> None:
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {qident(schema)}.{qident(table)} (
        line_no      BIGINT NOT NULL,
        raw_line     TEXT NOT NULL,
        _source_file TEXT NOT NULL,
        _ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        _batch_id    TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS {qident(f"idx_{table}_line_no")}
        ON {qident(schema)}.{qident(table)} (line_no);

    CREATE INDEX IF NOT EXISTS {qident(f"idx_{table}_batch_id")}
        ON {qident(schema)}.{qident(table)} (_batch_id);
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()

def file_to_copy_csv(
    file_path: Path,
    encoding: str,
    batch_id: str,
) -> Path:
    """
    Converte o arquivo (qualquer conteúdo) em um CSV temporário UTF-8 com:
    line_no,raw_line,_source_file,_batch_id
    """
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    tmp_path = Path(tmp.name)
    tmp.close()

    with tmp_path.open("w", encoding="utf-8", newline="") as out_f:
        writer = csv.writer(
            out_f,
            delimiter=",",
            quotechar='"',
            quoting=csv.QUOTE_ALL,
            doublequote=True,
            lineterminator="\n",
        )
        writer.writerow(["line_no", "raw_line", "_source_file", "_batch_id"])

        with file_path.open("r", encoding=encoding, errors="replace", newline="") as in_f:
            for i, line in enumerate(in_f, start=1):
                raw_line = line.rstrip("\n\r")
                writer.writerow([i, raw_line, file_path.name, batch_id])

    return tmp_path

def copy_temp_csv_into_table(conn: PgConnection, schema: str, table: str, temp_csv: Path) -> None:
    copy_sql = f"""
    COPY {qident(schema)}.{qident(table)} (line_no, raw_line, _source_file, _batch_id)
    FROM STDIN WITH (
        FORMAT csv,
        HEADER true,
        DELIMITER ',',
        QUOTE '"'
    );
    """
    with conn.cursor() as cur:
        with temp_csv.open("r", encoding="utf-8", newline="") as f:
            cur.copy_expert(copy_sql, f)
    conn.commit()

def list_csv_files(path: Path) -> List[Path]:
    return sorted([p for p in path.glob("*.csv") if p.is_file()])

def main() -> None:
    BASE_DIR = Path(__file__).resolve().parents[1]   # /root/data_platform
    ENV_PATH = BASE_DIR / "config" / ".env"

    if not ENV_PATH.exists():
        raise FileNotFoundError(f".env não encontrado em: {ENV_PATH}")

    load_dotenv(dotenv_path=ENV_PATH)

    pg = PgConfig(
        host=os.environ["PG_HOST"],
        port=int(os.environ.get("PG_PORT", "5432")),
        dbname=os.environ["PG_DB"],
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
    )

    base_path = Path(os.environ["CSV_664_PATH"]).resolve()
    schema = "_bronze"  # fixo conforme seu padrão
    write_mode = os.environ.get("WRITE_MODE", "append").strip().lower()

    if not base_path.exists():
        raise FileNotFoundError(f"Pasta não encontrada: {base_path}")

    files = list_csv_files(base_path)
    if not files:
        raise FileNotFoundError(f"Nenhum .csv encontrado em: {base_path}")

    batch_id = uuid.uuid4().hex

    conn = psycopg2.connect(
        host=pg.host,
        port=pg.port,
        dbname=pg.dbname,
        user=pg.user,
        password=pg.password,
    )
    try:
        ensure_schema(conn, schema)

        for fp in files:
            table = sanitize_table_name(fp.stem)  # ex.: 202511_270 -> t_202511_270 (se começar com número)
            encoding = detect_encoding(fp)

            if write_mode == "overwrite":
                drop_table_if_exists(conn, schema, table)

            ensure_raw_table(conn, schema, table)

            tmp_csv = file_to_copy_csv(fp, encoding, batch_id)
            try:
                copy_temp_csv_into_table(conn, schema, table, tmp_csv)
                print(f"[OK] {fp.name} -> {schema}.{table} (linhas raw)")
            finally:
                tmp_csv.unlink(missing_ok=True)

        print(f"\nBatch finalizado: batch_id={batch_id} | arquivos={len(files)}")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
