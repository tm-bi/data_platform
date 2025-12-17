import pandas as pd
from pathlib import Path
import pyarrow.parquet as pq

pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)

parquet_path = Path(
    "/root/data_platform/ods/novaxs_270_silver_trans"
    "/novaxs_270_silver_trans/load_date=2025-12-17"
    "/novaxs_270_silver_trans.parquet"
) 

df = pd.read_parquet(parquet_path, engine="pyarrow")
pf = pq.ParquetFile(parquet_path)

print("---------------------------------------------------------\n")
print(f"Total de Linhas: {len(df):,}".replace(",", "."))
print(f"Total de Colunas: {len(df.columns):,}\n".replace(",", "."))
print("---------------------------------------------------------\n")

print("Colunas disponíveis neste arquivo:\n")
for i, col in enumerate(df.columns, start=1):
    print(f"{i:02d} - {col}") 

print("---------------------------------------------------------\n")
print("Formato das Colunas:")
for i in range(pf.metadata.num_columns):
    col = pf.metadata.schema.column(i)
    print(f"- {col.name} | type={col.physical_type}")

# print("\nAmostra aleatória (100 linhas):")
# print(df.head())
# print(df.sample(100, random_state=42))



