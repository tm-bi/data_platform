import logging
import time
from datetime import datetime, timedelta, UTC

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup


LOGGER = logging.getLogger(__name__)

URLS = [
    (
        "https://www.accuweather.com/pt/br/cotia/41604/daily-weather-forecast/41604",
        "Cotia"
    ),
    (
        "https://www.accuweather.com/pt/br/são-paulo/45881/daily-weather-forecast/45881",
        "São Paulo"
    ),
]


def _build_driver() -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    options.add_argument("--lang=pt-BR")
    options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36")
    options.add_argument("--disable-blink-features=AutomationControlled")



    service = Service("/usr/bin/chromedriver")

    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(60)
    driver.implicitly_wait(10)
    return driver


def extract_accuweather() -> list[dict]:
    driver = _build_driver()
    scraped_at = datetime.now(UTC)

    results: list[dict] = []

    try:
        for url, cidade in URLS:
            LOGGER.info(f"[AccuWeather] Iniciando scraping: {cidade}")
            driver.get(url)

            tempo_pagina = 70 if "são-paulo" in url else 60  # mais folga na VPS/headless

            try:
                WebDriverWait(driver, 40).until(
                    EC.presence_of_all_elements_located((By.CLASS_NAME, "daily-wrapper"))
                )
            except TimeoutException:
                LOGGER.warning(f"[AccuWeather] Timeout inicial ({cidade}), tentando refresh")
                driver.refresh()
                WebDriverWait(driver, tempo_pagina).until(
                    EC.presence_of_all_elements_located((By.CLASS_NAME, "daily-wrapper"))
                )
            time.sleep(10)
            soup = BeautifulSoup(driver.page_source, "html.parser")
            blocos = soup.find_all("div", class_="daily-wrapper")

            for idx, bloco in enumerate(blocos):
                try:
                    data_prev = (scraped_at + timedelta(days=idx)).date().isoformat()

                    def _get_temp(cls):
                        span = bloco.find("span", cls)
                        if not span or not span.text:
                            return None

                        txt = span.text.strip()
                        # o AccuWeather às vezes vem como "/20" ou " / 20° "
                        txt = txt.replace("°", "").replace("/", "").strip()

                        # extrai apenas dígitos (e sinal, se existir)
                        digits = "".join(ch for ch in txt if ch.isdigit() or ch == "-")
                        return int(digits) if digits else None


                    temp_max = None
                    temp_min = None
                    try:
                        temp_max = _get_temp("high")
                        temp_min = _get_temp("low")
                    except Exception:
                        LOGGER.warning(f"[AccuWeather] Falha ao ler temperaturas ({cidade} bloco {idx})", exc_info=True)


                    prob_chuva = "N/A"
                    relatorio = "N/A"
                    sens_term = "N/A"
                    sens_sombra = "N/A"
                    ind_uv = "N/A"
                    vento = "N/A"

                    if (p := bloco.find("div", class_="precip")):
                        prob_chuva = p.text.strip()

                    if (r := bloco.find("div", class_="phrase")):
                        relatorio = r.text.strip()

                    if (panels := bloco.find("div", class_="panels")):
                        for el in panels.find_all("p", class_="panel-item"):
                            txt = el.text
                            if "RealFeel®" in txt:
                                sens_term = el.find("span", "value").text
                            elif "RealFeel Shade™" in txt:
                                sens_sombra = el.find("span", "value").text
                            elif "UV" in txt:
                                ind_uv = el.find("span", "value").text
                            elif "Vento" in txt:
                                vento = el.find("span", "value").text

                    results.append(
                        {
                            "data_scrap": scraped_at.date().isoformat(),
                            "hora_scrap": scraped_at.time().strftime("%H:%M:%S"),
                            "origem": "AccuWeather",
                            "cidade": cidade,
                            "total_dias": len(blocos),
                            "bloco": idx,
                            "data_previsao": data_prev,
                            "tempmin": temp_min,
                            "tempmax": temp_max,
                            "sensacao_termica": sens_term,
                            "sensacao_sombra": sens_sombra,
                            "ind_max_uv": ind_uv,
                            "vento": vento,
                            "probab_chuva": prob_chuva,
                            "relatorio": relatorio,
                        }
                    )

                except Exception as e:
                    LOGGER.exception(f"[AccuWeather] Erro no bloco {idx} ({cidade}): {e}")

            LOGGER.info(f"[AccuWeather] Finalizado: {cidade}")

    finally:
        driver.quit()

    return results
