from __future__ import annotations

from _bootstrap import setup_sys_path
setup_sys_path()

from _silver.quality.load_silver_trans_quality import bronze_to_silver_trans_quality


def main() -> int:
    inserted = bronze_to_silver_trans_quality()
    print(f"[SILVER-TRANS][QUALITY] Inseridos em s_quality_acesso: {inserted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
