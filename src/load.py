from pymongo import MongoClient
from pymongo.server_api import ServerApi
import os
from dotenv import load_dotenv

load_dotenv()


class Load:
    """
    Responsável por persistir os dados extraídos da API do IBGE (PNADC),
    seja em um arquivo JSON local, seja em uma coleção do MongoDB.
    """

    def __init__(self):
        self.mongo_uri = os.getenv("MONGODB_URI")
        self.client = MongoClient(self.mongo_uri, server_api=ServerApi("1"))

    def close(self) -> None:
        """Encerra a conexão com o MongoDB."""
        self.client.close()

    def load_json(self, nome_arquivo: str, data: list[dict]) -> None:
        """
        Salva o resultado da extração em um arquivo JSON local, em jsons/.

        Atributos:
            nome_arquivo: nome do arquivo de destino, sem extensão
            data: lista de dicionários retornada pela API do IBGE
        """
        with open(f"jsons/{nome_arquivo}.json", "w", encoding="UTF-8") as f:
            f.write(str(data))

    def load_mongo(self, data: list[dict], db_name: str, collection_name: str) -> None:
        """
        Insere o resultado da extração em uma coleção do MongoDB.

        Atributos:
            data: lista de dicionários retornada pela API do IBGE
            db_name: nome do banco de dados no MongoDB
            collection_name: nome da coleção onde os documentos serão inseridos
        """
        collection = self.client[db_name][collection_name]

        if data:
            collection.insert_many(data)

        print(f"Dados inseridos com sucesso na coleção '{collection_name}'!")
        self.close()
