import pyarrow.parquet as pq

parquet_path = (
    "/root/data_platform/ods/novaxs_270_silver_trans/"
    "novaxs_270_silver_trans/load_date=2025-12-17/"
    "novaxs_270_silver_trans.parquet"
)

pf = pq.ParquetFile(parquet_path)

# print("Schema:")
# print(pf.schema)

# print("\nRow count:", pf.metadata.num_rows)
print("Columns:")
for i in range(pf.metadata.num_columns):
    col = pf.metadata.schema.column(i)
    print(f"- {col.name} | type={col.physical_type}")
