from __future__ import annotations

from _bootstrap import setup_sys_path
setup_sys_path()

from _gold.load_gold_fato_acessos import silver_contexto_to_gold_fato_acessos


def main() -> int:
    inserted = silver_contexto_to_gold_fato_acessos()
    print(f"[GOLD] Inseridos em _gold.fato_acessos: {inserted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
