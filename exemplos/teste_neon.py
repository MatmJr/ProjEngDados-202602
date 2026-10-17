import pandas as pd
from sqlalchemy import create_engine
import src.config as config

engine = create_engine(config.DATABASE_URL)

df = pd.DataFrame(
    {
        "periodo": ["2024Q1", "2024Q2", "2024Q3"],
        "valor": [10.5, 11.2, 12.0],
    }
)

df.to_sql("teste_neon", engine, if_exists="replace", index=False)
print("Dados salvos no neondb!")

df_show = pd.read_sql("SELECT * FROM teste_neon", engine)
print(df_show)

engine.dispose()
