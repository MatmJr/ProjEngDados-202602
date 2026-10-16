from prefect import flow, task, get_run_logger
from dotenv import load_dotenv
from src.extract import Extract
from src.load import Load
from src.transform import Transform
import pandas as pd

load_dotenv()

@task(retries=3, retry_delay_seconds=10)
def extract(variavel: int, estado: int) -> list[dict]:
    logger = get_run_logger()
    extractor = Extract()
    data = extractor.pnadc(variavel=variavel, estado=estado)
    logger.info(f"{len(data)} registros extraídos da série {variavel}")
    return data

@task
def load_data(data, db_name: str, collection_name: str):
    logger = get_run_logger()
    loader = Load()
    loader.load_mongo(data, db_name, collection_name)
    logger.info(f"{len(data)} registros inseridos em {db_name}.{collection_name}")

@task
def transform(db_name: str, collection_name: str) -> pd.DataFrame:
    logger = get_run_logger()
    extractor = Extract()
    transformer = Transform()
    data = extractor.extract_collection_from_mongo(db_name, collection_name)
    extractor.close()
    df = transformer.transform_pnadc(data)
    logger.info(f"{len(df)} linhas transformadas a partir de {db_name}.{collection_name}")
    return df

@task(retries=3, retry_delay_seconds=10)
def load_neon(df: pd.DataFrame, nome_tabela: str):
    logger = get_run_logger()
    ld = Load()
    ld.load_neon(df, nome_tabela)
    logger.info(f"{len(df)} linhas salvas na tabela {nome_tabela} do Neon")

@flow(name="ETL PNADC Prefect", retries=3, log_prints=True)
def etl_pnadc_flow(variavel: int = 4099, estado: int = 26):

    data = extract(variavel, estado)
    load_data(data, "IBGE", "PNADC")
    df = transform("IBGE", "PNADC")
    load_neon(df, "pnadc")

if __name__ == "__main__":
    etl_pnadc_flow()
