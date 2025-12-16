import os
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
from datetime import date
from dotenv import load_dotenv

#-----------------------------------------------------------------

env_path = Path(__file__).resolve().parents[1] / ".env"   # /root/data_platform/.env
load_dotenv(dotenv_path=env_path)

load_dotenv()

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "seu_banco")
PG_USER = os.getenv("PG_USER", "seu_usuario")
PG_PASSWORD = os.getenv("PG_PASSWORD", "sua_senha")

SRC_SCHEMA = os.getenv("SRC_SCHEMA")
SRC_TABLE_270 = f"{SRC_SCHEMA}.{os.getenv('SRC_TABLE_270')}"

ODS_BASE_PATH = os.getenv("ODS_BASE_PATH")
TARGET_TABLE = "novaxs_270_silver_trans"

LOAD_DATE = date.today().isoformat()

OUT_DIR = f"{ODS_BASE_PATH}/{TARGET_TABLE}/load_date={LOAD_DATE}"
Path(OUT_DIR).mkdir(parents=True, exist_ok=True)

OUT_FILE = f"{OUT_DIR}/{TARGET_TABLE}.parquet"

#-----------------------------------------------------------------

engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
)

df = pd.read_sql_query(f"SELECT * FROM {SRC_TABLE_270}", engine)

#-----------------------------------------------------------------

df.rename(columns={
    "un": "valor_un",
    "total": "valor_total",
    "quantidade_de_parcelas": "qtd_parcelas",
    "prazo_estimado_do_recebimento": "prazo_estimado_recebimento"
}, inplace=True)

# Convertendo para tipo inteiro
int_cols = ["id_stg", "conta", "autorizacao", "nsu", "qtd", "qtd_parcelas"]
for col in int_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

# Tratamento de texto (trimming, lowercase no email)
text_cols = [
    "cliente", "endereco", "cidade", "celular", "telefone", "email", "agencia",
    "produto", "status", "forma_de_pagamento", "usuario", "prazo_estimado_recebimento"
]
for col in text_cols:
    if col in df.columns:
        df[col] = df[col].astype("string").str.strip()

if "email" in df.columns:
    df["email"] = df["email"].str.lower()

# Tratamento de datas (formatos BR -> DATE)
date_cols = ["data", "data_utilizacao", "data_encerramento"]
for col in date_cols:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], format="%d/%m/%Y", errors="coerce").dt.date

# Processando a coluna "criado" (data e hora)
if "criado" in df.columns:
    criado_ts = pd.to_datetime(df["criado"], format="%d/%m/%Y %H:%M:%S", errors="coerce")
    df["data_venda"] = criado_ts.dt.date
    df["hora_venda"] = criado_ts.dt.strftime("%H:%M:%S")
    df = df.drop(columns=["criado"])

# Processando "hora_utilizacao" (formato de hora)
if "hora_utilizacao" in df.columns:
    df["hora_utilizacao"] = df["hora_utilizacao"].astype("string").str.strip()
    df.loc[df["hora_utilizacao"] == "", "hora_utilizacao"] = pd.NA  # Mantém vazio se não tiver

# Convertendo valores monetários
for col in ["valor_un", "valor_total"]:
    if col in df.columns:
        s = df[col].astype("string").str.strip()
        s = s.str.replace(".", "", regex=False)  # Remove separador de milhar
        s = s.str.replace(",", ".", regex=False)  # Converte vírgula para ponto
        df[col] = pd.to_numeric(s, errors="coerce")

# Adicionando a coluna técnica "load_date"
df["load_date"] = pd.to_datetime(LOAD_DATE).date()

#-----------------------------------------------------------------

# Gravar o arquivo Parquet no destino
df.to_parquet(OUT_FILE, engine="pyarrow", index=False)

print(f"✅ Silver Transacional criada com sucesso em: {OUT_FILE}")

