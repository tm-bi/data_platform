# from fastapi import FastAPI
# from fastapi.responses import FileResponse

# app = FastAPI()

# @app.get("/root/data_platform/ods/novaxs_270_silver_trans/novaxs_270_silver_trans/load_date=2025-12-17/novaxs_270_silver_trans.parquet")
# def download_parquet():
#     return FileResponse(
#         path="/root/data_platform/ods/novaxs_270_silver_trans/novaxs_270_silver_trans/load_date=2025-12-17/novaxs_270_silver_trans.parquet",
#         media_type="application/octet-stream",
#         filename="novaxs_270_silver_trans.parquet"
#     )

python3 -c "import fastapi; print(fastapi.__version__)"
