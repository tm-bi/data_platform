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
    ("https://www.climatempo.com.br/previsao-do-tempo/15-dias/cidade/431/cotia-sp", "Cotia"),
    ("https://www.climatempo.com.br/previsao-do-tempo/15-dias/cidade/558/saopaulo-sp", "São Paulo"),
]


def _build_driver() -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    options.add_argument("--lang=pt-BR")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
    )
    options.add_argument("--disable-blink-features=AutomationControlled")

    service = Service("/usr/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(60)
    driver.implicitly_wait(10)
    return driver


def _safe_int(text: str | None) -> int | None:
    if not text:
        return None
    s = text.strip().replace("°", "").replace("/", "")
    digits = "".join(ch for ch in s if ch.isdigit() or ch == "-")
    return int(digits) if digits else None


def extract_climatempo() -> list[dict]:
    driver = _build_driver()
    scraped_at = datetime.now(UTC)
    results: list[dict] = []

    try:
        for url, cidade in URLS:
            LOGGER.info(f"[Climatempo] Iniciando scraping: {cidade}")
            driver.get(url)

            # Fecha popup, se existir (como no seu código antigo)
            try:
                WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.CLASS_NAME, "icon-close"))
                ).click()
                LOGGER.info("[Climatempo] Popup fechado.")
            except Exception:
                LOGGER.info("[Climatempo] Nenhum popup encontrado.")

            # Tenta expandir para +5 dias (2 cliques) — tolerante a ausência
            for btn_id, label in [
                ("Botao_1_mais_5_dias_timeline_15_dias", "Clique 1"),
                ("Botao_2_mais_5_dias_timeline_15_dias", "Clique 2"),
            ]:
                try:
                    btn = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.ID, btn_id))
                    )
                    driver.execute_script("arguments[0].click();", btn)
                    LOGGER.info(f"[Climatempo] {label} OK.")
                    time.sleep(2)
                except Exception:
                    LOGGER.warning(f"[Climatempo] {label} - botão não encontrado.")

            # Espera os cards aparecerem
            try:
                WebDriverWait(driver, 25).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, 'div[data-visualization-content="list"] section.accordion-card')
                    )
                )
            except TimeoutException:
                LOGGER.warning(f"[Climatempo] Timeout esperando blocos ({cidade}). Pulando cidade.")
                continue

            time.sleep(6)
            soup = BeautifulSoup(driver.page_source, "html.parser")
            blocos = soup.select('div[data-visualization-content="list"] section.accordion-card')
            total_dias = len(blocos)

            for idx, bloco in enumerate(blocos):
                try:
                    data_prev = (scraped_at + timedelta(days=idx)).date().isoformat()

                    temps = bloco.select('div._flex._flex-column._margin-r-15._margin-l-20 span.-gray')
                    temp_min = _safe_int(temps[0].get_text(strip=True)) if len(temps) > 0 else None
                    temp_max = _safe_int(temps[1].get_text(strip=True)) if len(temps) > 1 else None

                    chuva_elem = bloco.select_one('div._margin-l-5 span._margin-l-5')
                    prob_chuva = chuva_elem.get_text(strip=True) if chuva_elem else "N/A"

                    vento = "N/A"
                    vento_divs = bloco.select("div.variable-card._flex._align-center")
                    for div in vento_divs:
                        if "Vento" in div.get_text(" ", strip=True):
                            vento_info = div.select_one("div._margin-l-5 div")
                            vento = vento_info.get_text(" ", strip=True) if vento_info else "N/A"
                            break

                    relatorio_elem = bloco.select_one("p.-gray.-line-height-22")
                    relatorio = relatorio_elem.get_text(" ", strip=True) if relatorio_elem else "N/A"

                    results.append(
                        {
                            "data_scrap": scraped_at.date().isoformat(),
                            "hora_scrap": scraped_at.time().strftime("%H:%M:%S"),
                            "origem": "Climatempo",
                            "cidade": cidade,
                            "total_dias": total_dias,
                            "bloco": idx,
                            "data_previsao": data_prev,
                            "tempmin": temp_min,
                            "tempmax": temp_max,
                            "sensacao_termica": "não informado",
                            "sensacao_sombra": "não informado",
                            "ind_max_uv": "não informado",
                            "vento": vento,
                            "probab_chuva": prob_chuva,
                            "relatorio": relatorio,
                        }
                    )

                except Exception as e:
                    LOGGER.exception(f"[Climatempo] Erro no bloco {idx} ({cidade}): {e}")

            LOGGER.info(f"[Climatempo] Finalizado: {cidade}")

    finally:
        driver.quit()

    return results
