from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, URL

#########################################################
# Configuração de logging
#########################################################

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
)

#########################################################
# Infra: carregar .env e criar engine
#########################################################

def load_env(env_path: Optional[Path] = None) -> None:
    if env_path is None:
        env_path = Path(__file__).resolve().parents[1] / "config" / ".env"

    if not env_path.exists():
        raise FileNotFoundError(f"Arquivo .env não encontrado em: {env_path}")
    
    load_dotenv(dotenv_path=env_path)
    logging.info("Variáveis de ambiente carregadas de %s", env_path)


def get_engine() -> Engine:
    host = os.getenv("PG_HOST")
    port = os.getenv("PG_PORT", "5432")
    db_name = os.getenv("PG_DB")
    user = os.getenv("PG_USER")
    password = os.getenv("PG_PASSWORD")

    if not all([host, port, db_name, user, password]):
        raise RuntimeError(
            "Variáveis de ambiente de banco incompletas. "
            "Verificar as credenciais do arquivo .env"
        )

    # Usa URL.create para escapar senha com #, @ etc.
    url_object = URL.create(
        drivername="postgresql+psycopg2",
        username=user,
        password=password,
        host=host,
        port=int(port),
        database=db_name,
    )

    logging.info(
        "Criando engine para %s",
        url_object.render_as_string(hide_password=True),
    )

    engine = create_engine(url_object)
    return engine

#########################################################
# Detecção da linha de cabeçalho real
#########################################################

def detectar_linha_cabecalho(path_csv: Path) -> int:
    """
    Varre o arquivo até encontrar a linha de cabeçalho da tabela,
    que começa com 'Conta;Autorização;Cliente;'.
    Retorna o índice (0-based) dessa linha.
    Se não encontrar, assume cabeçalho na primeira linha (0).
    """
    with path_csv.open("r", encoding="latin1") as f:
        for idx, line in enumerate(f):
            if line.startswith("Conta;Autorização;Cliente;"):
                return idx

    logging.warning(
        "Cabeçalho padrão não encontrado em %s. Usando primeira linha como cabeçalho.",
        path_csv.name,
    )
    return 0

#########################################################
# Leitura dos CSV (sem limpeza de linhas)
#########################################################

def ler_csv_tratado(path_csv: Path) -> pd.DataFrame:
    logging.info("Lendo arquivo (sem filtros): %s", path_csv)

    header_line_idx = detectar_linha_cabecalho(path_csv)
    skiprows = list(range(header_line_idx))

    df = pd.read_csv(
        path_csv,
        sep=";",
        encoding="latin1",
        skiprows=skiprows,
        dtype=str,
    )

    logging.info(
        "Arquivo %s: %d linhas lidas a partir do cabeçalho (linha %d).",
        path_csv.name,
        len(df),
        header_line_idx,
    )

    col_map = {
        "Conta": "conta",
        "Autorização": "autorizacao",
        "Cliente": "cliente",
        "Endereço": "endereco",
        "Cidade": "cidade",
        "Celular": "celular",
        "Telefone": "telefone",
        "Email": "email",
        "Agencia": "agencia",
        "Data": "data",
        "Criado": "criado",
        "Qtd": "qtd",
        "Produto": "produto",
        "Un.": "un",
        "Total": "total",
        "Status": "status",
        "Entregue": "entregue",
        "Forma de Pagamento": "forma_de_pagamento",
        "Usuário": "usuario",
        "Data Utilização": "data_utilizacao",
        "Hora Utilização": "hora_utilizacao",
        "NSU": "nsu",
        "Quantidade de Parcelas": "quantidade_de_parcelas",
        "Prazo estimado do recebimento": "prazo_estimado_do_recebimento",
        "Data Encerramento": "data_encerramento",
    }

    df = df.rename(columns=col_map)

    logging.info("Colunas do DataFrame após rename: %s", list(df.columns))

    if "criado" in df.columns:
        linhas_antes = len(df)

        # Série original (mantém NaN)
        criado_raw = df["criado"]

        # Série em string, para tratar espaços, [NULL], etc.
        criado_str = criado_raw.astype(str).str.strip()

        mask_invalid = (
            criado_raw.isna() |                          # NaN / valores nulos reais
            (criado_str == "") |                        # vazio depois de strip
            (criado_str.str.upper() == "[NULL]") |      # texto literal [NULL]
            (criado_str.str.upper() == "NULL")          # texto literal NULL
        )

        df = df[~mask_invalid].copy()

        linhas_depois = len(df)
        logging.info(
            "Linhas removidas por Criado nulo/vazio/[NULL]/NULL: %d",
            linhas_antes - linhas_depois,
        )
    else:
        logging.warning("Coluna 'criado' não encontrada no arquivo %s", path_csv.name)

    return df

#########################################################
# Carga dos arquivos no PostgreSQL
#########################################################

def carregar_arquivos_para_postgres(
    engine: Engine,
    csv_dir: Path,
    table_name: str,
    schema: str, 
) -> None:

    if not csv_dir.exists():
        raise FileNotFoundError(f"Diretório de csv não encontrado: {csv_dir}")
    
    csv_files = sorted(csv_dir.glob("*.csv"))
    if not csv_files:
        logging.warning("Nenhum arquivo csv encontrado em %s", csv_dir)
        return

    logging.info(
        "Econtrados %d arquivos CSV em %s. Iniciando carga...",
        len(csv_files),
        csv_dir,
    )

    total_registros = 0

    for csv_file in csv_files:
        try:
            df = ler_csv_tratado(csv_file)

            if df.empty:
                logging.info(
                    "Arquivo %s não possui linhas válidas após leitura. Pulando.",
                    csv_file.name,
                )
                continue
            
            df.to_sql(
                name=table_name,
                con=engine,
                schema=schema,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000,
            )

            total_registros += len(df)
            logging.info(
                "Arquivo %s carregado com sucesso. %d registros inseridos.",
                csv_file.name,
                len(df),
            )
        except Exception as exc:
            logging.exception(
                "Erro ao processar arquivo %s: %s", 
                csv_file.name,
                exc,
            )
            raise  # falha rápido pra gente ver o erro real

    logging.info(
        "Processo concluído. Total de registros inseridos em %s.%s: %d",
        schema,
        table_name,
        total_registros,
    )

#########################################################
# Ponto de entrada
#########################################################

def main() -> None:
    load_env()
    engine = get_engine()
    csv_base_path = os.getenv("CSV_BASE_PATH")
    if not csv_base_path:
        raise RuntimeError(
            "Variável CSV_BASE_PATH não definida no .env. "
            "Informe o caminho da pasta novaxs_270."
        )
    
    csv_dir = Path(csv_base_path)

    table_name = "novaxs_270_raw"
    # sua tabela está em stg, então deixo stg como default
    schema = os.getenv("DB_SCHEMA", "stg")

    carregar_arquivos_para_postgres(
        engine=engine,
        csv_dir=csv_dir,
        table_name=table_name,
        schema=schema,
    )


if __name__ == "__main__":
    main()
