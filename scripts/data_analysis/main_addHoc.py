import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


env_path = Path("/root/data_platform/config/.env")
loaded = load_dotenv(env_path, override=True)
if not loaded:
    raise FileNotFoundError(f"Não consegui carregar o env: {env_path}")


def must_getenv(key: str) -> str:
    v = os.getenv(key)
    if not v:
        raise RuntimeError(f"{key} não está definido em {env_path}.")
    return v


url = URL.create(
    drivername="postgresql+psycopg2",
    username=must_getenv("PG_USER"),
    password=must_getenv("PG_PASSWORD"),
    host=must_getenv("PG_HOST"),
    port=int(must_getenv("PG_PORT")),
    database=must_getenv("PG_DB"),
)
engine = create_engine(url)

schema = "_silver-transacional"
table = "s_quality_ingressos"

query = text(f'''
    SELECT *
    FROM "{schema}"."{table}"
    LIMIT 10
''')

df = pd.read_sql_query(query, con=engine)

styled = (
    df.style
    .set_table_styles([
        {"selector": "table", "props": [("border-collapse", "collapse"), ("width", "100%")]} ,
        {"selector": "th, td", "props": [("border", "1px solid #999"), ("padding", "6px"), ("white-space", "nowrap")]} ,
        {"selector": "th", "props": [("background", "#eee"), ("position", "sticky"), ("top", "0"), ("z-index", "2")]} ,
    ])
)

# wrapper com scroll horizontal
html = f"""
<!doctype html>
<html lang="pt-br">
<head>
  <meta charset="utf-8" />
  <title>df_preview</title>
  <style>
    body {{ font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial; margin: 16px; }}
    .wrap {{ overflow-x: auto; border: 1px solid #ddd; border-radius: 8px; }}
    table {{ font-size: 13px; }}
  </style>
</head>
<body>
  <h3>Preview: {schema}.{table} (linhas: {len(df)})</h3>
  <div class="wrap">
    {styled.to_html(index=False)}
  </div>
</body>
</html>
"""

out = Path("df_preview.html")
out.write_text(html, encoding="utf-8")
print(f"Gerado: {out.resolve()}")
