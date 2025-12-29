import logging
import sys
from datetime import datetime, UTC, timedelta

from src._silver.clima.load_silver_trans_clima import load_silver_trans_clima

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Estratégia simples (sem depender do seu watermark):
# carrega últimas 48h por segurança, evitando perder algo por delay.
# Depois a gente pluga watermark oficial do seu common/watermark.py.
DEFAULT_LOOKBACK_HOURS = 48


def main() -> int:
    logging.info("[Clima][Silver Trans] Início do job")

    try:
        since = datetime.now(UTC) - timedelta(hours=DEFAULT_LOOKBACK_HOURS)
        inserted = load_silver_trans_clima(since_ingested_at=since)
        logging.info(f"[Clima][Silver Trans] Inseridas {inserted} linhas")
        logging.info("[Clima][Silver Trans] Job finalizado com sucesso")
        return 0
    except Exception:
        logging.exception("[Clima][Silver Trans] Erro fatal no job")
        return 1


if __name__ == "__main__":
    sys.exit(main())
