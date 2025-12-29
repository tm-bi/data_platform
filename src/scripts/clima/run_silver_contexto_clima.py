import logging
import sys

from src._silver.clima.load_silver_contexto_clima import load_silver_contexto_clima

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def main() -> int:
    logging.info("[Clima][Silver Contexto] Início do job")
    try:
        n = load_silver_contexto_clima()
        logging.info(f"[Clima][Silver Contexto] Chaves atualizadas: {n}")
        logging.info("[Clima][Silver Contexto] Job finalizado com sucesso")
        return 0
    except Exception:
        logging.exception("[Clima][Silver Contexto] Erro fatal no job")
        return 1


if __name__ == "__main__":
    sys.exit(main())
