from __future__ import annotations

from _bootstrap import setup_sys_path
setup_sys_path()

from _silver.quality.load_silver_contexto_quality import (
    silver_trans_to_silver_contexto_fato_quality,
)


def main() -> int:
    inserted = silver_trans_to_silver_contexto_fato_quality(source_file="sqlserver:quality")
    print(f"[SILVER-CONTEXTO][QUALITY] Inseridos em fato_acesso_quality: {inserted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
