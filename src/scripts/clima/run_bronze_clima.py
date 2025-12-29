import logging
import sys

from src._bronze.clima.accuweather.extract_accuweather import extract_accuweather
from src._bronze.clima.accuweather.load_accuweather import load_accuweather

from src._bronze.clima.climatempo.extract_climatempo import extract_climatempo
from src._bronze.clima.climatempo.load_climatempo import load_climatempo


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def main() -> int:
    logging.info("[Clima][Bronze] Início do job")

    exit_code = 0

    # AccuWeather
    try:
        rows = extract_accuweather()
        inserted = load_accuweather(rows)
        logging.info(f"[Clima][Bronze] AccuWeather finalizado ({inserted} linhas)")
    except Exception:
        logging.exception("[Clima][Bronze] AccuWeather falhou")
        exit_code = 1

    # Climatempo
    try:
        rows = extract_climatempo()
        inserted = load_climatempo(rows)
        logging.info(f"[Clima][Bronze] Climatempo finalizado ({inserted} linhas)")
    except Exception:
        logging.exception("[Clima][Bronze] Climatempo falhou")
        exit_code = 1

    if exit_code == 0:
        logging.info("[Clima][Bronze] Job finalizado com sucesso")
    else:
        logging.warning("[Clima][Bronze] Job finalizado com falhas (ver logs)")

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
