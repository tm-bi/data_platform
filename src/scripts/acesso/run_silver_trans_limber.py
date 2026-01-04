from __future__ import annotations

from _bootstrap import setup_sys_path

setup_sys_path()

from _silver.limber.load_silver_trans_limber import bronze_to_silver_trans_limber


def main() -> int:
    inserted = bronze_to_silver_trans_limber()
    print(f"[SILVER-TRANS][LIMBER] Inseridos em s_limber_acesso: {inserted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
