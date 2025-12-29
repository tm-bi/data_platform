import logging
import sys

from src._gold.clima.load_gold_clima_unificado import load_gold_clima_unificado

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def main() -> int:
    logging.info("[Clima][Gold] Início do job")
    try:
        load_gold_clima_unificado()
        logging.info("[Clima][Gold] Job finalizado com sucesso")
        return 0
    except Exception:
        logging.exception("[Clima][Gold] Erro fatal no job")
        return 1

if __name__ == "__main__":
    sys.exit(main())
