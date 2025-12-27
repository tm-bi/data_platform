from __future__ import annotations

from _bootstrap import setup_sys_path

setup_sys_path()

from _silver.limber.load_silver_contexto_limber import silver_trans_to_silver_contexto_fato_limber

def main() -> int:
    inserted = silver_trans_to_silver_contexto_fato_limber(source_file="firebird:limber")
    print(f"[SILVER-CONTEXTO][LIMBER] Inseridos em fato_acesso_limber: {inserted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
