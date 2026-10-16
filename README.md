# Projeto de Engenharia de Dados - ETL

Pipeline de ETL que extrai dados da PNAD Contínua a partir da API do IBGE, carrega o resultado bruto em uma coleção MongoDB, transforma esses dados com pandas e carrega o resultado final em uma tabela SQLite local e em um banco PostgreSQL hospedado no [Neon](https://neon.tech) (com opção de salvar o bruto em arquivo JSON local). O pipeline pode ser executado como um script simples ou orquestrado com o [Prefect](https://www.prefect.io).

## Estrutura do projeto

```
src/
  extract.py    # Extract: busca dados de agregados do IBGE (agregado() é genérico; pnadc() é um atalho para o agregado 4093) e relê dados já carregados no MongoDB
  transform.py  # Transform: transforma os dados brutos da PNADC em um DataFrame pronto para o SQLite
  load.py       # Load: salva em JSON local (load_json), no MongoDB (load_mongo), em SQLite (load_sqlite) ou no Neon (load_neon)
run_etl.py                # ponto de entrada do pipeline em script simples (Extract -> Load -> Extract -> Transform -> Load), em main()
orchestration_prefect.py  # o mesmo pipeline orquestrado com Prefect (tasks + flow etl_pnadc_flow)
exemplos/                 # scripts avulsos de teste (ex.: teste_mongo.py, teste_neon.py)
notebooks/                # notebooks das aulas
jsons/                    # saídas de exemplo em JSON
```

### `Extract`

- `agregado(agregado_id, variavel, estado, periodo_inicio, periodo_fim, classificacao="2[all]")`: método genérico, reutilizável para qualquer agregado (tabela) do IBGE.
- `pnadc(variavel, estado, periodo_inicio="201201", periodo_fim="202603")`: atalho já configurado para o agregado 4093 (PNAD Contínua). O período tem 2012-01 a 2026-03 como padrão, mas pode ser sobrescrito na chamada.
- `extract_collection_from_mongo(db_name, collection_name)`: relê todos os documentos de uma coleção do MongoDB (por exemplo, a que `Load.load_mongo` acabou de popular), para alimentar a etapa de transformação.
- `Extract.UFS` e `Extract.VARIAVEIS_PNADC`: dicionários com os códigos válidos de UF e de variável do agregado 4093. `estado` e `variavel` são validados contra esses dicionários — um código inválido gera `ValueError`.
- A conexão com o MongoDB (`self.client`) é criada uma única vez, no `__init__`, e encerrada com `close()`.

### `Transform`

- `transform_pnadc(data)`: recebe a lista de dicionários no formato retornado pela API do IBGE (a mesma salva no MongoDB) e devolve um `DataFrame` com uma linha por período (data, valor), pronto para carga no SQLite.

### `Load`

- `load_json(nome_arquivo, data)`: salva os dados extraídos em `jsons/<nome_arquivo>.json`.
- `load_mongo(data, db_name, collection_name)`: insere os dados na coleção informada e fecha a conexão com o MongoDB (`close()`) logo em seguida.
- `load_sqlite(df, nome_banco="ibge.db", nome_tabela="pnadc")`: salva um `DataFrame` (já transformado) em uma tabela de um banco SQLite local.
- `load_neon(df, nome_tabela)`: salva um `DataFrame` (já transformado) em uma tabela do banco PostgreSQL no Neon, usando SQLAlchemy (driver `psycopg2`) e a string de conexão em `DATABASE_URL`. A tabela é substituída a cada execução (`if_exists="replace"`) e o engine é descartado (`dispose()`) ao final.
- A conexão com o MongoDB (`self.client`) é criada uma única vez, no `__init__` da classe.

## Configuração do Ambiente

### Windows

Criação do venv
```bash
python -m venv .venv
```

Ativação do venv
```bash
.venv\Scripts\activate
```

### Linux/Mac

Criação do venv
```bash
python3 -m venv .venv
```

Ativação do venv
```bash
source .venv/bin/activate
```

### Dependências

```bash
pip install -r requirements.txt
```

### Variáveis de ambiente

Crie um arquivo `.env` na raiz do projeto (ele não é versionado) com as strings de conexão do MongoDB e do Neon:

```
MONGODB_URI=<sua_connection_string_do_mongodb>
DATABASE_URL=postgresql://<usuario>:<senha>@<host>.neon.tech/<banco>?sslmode=require
```

### Banco no Neon

O Neon é um PostgreSQL serverless com plano gratuito. Para configurá-lo:

1. Crie uma conta e um projeto em [neon.tech](https://neon.tech).
2. No painel do projeto, clique em **Connect** e copie a *connection string* (formato `postgresql://...`).
3. Cole a string na variável `DATABASE_URL` do `.env`.

Para testar a conexão sem rodar o pipeline inteiro, use o script de exemplo:

```bash
python exemplos/teste_neon.py
```

As tabelas criadas pelo pipeline podem ser consultadas pelo **SQL Editor** do próprio painel do Neon (ex.: `SELECT * FROM pnadc;`).

## Executando o pipeline

### Como script simples

```bash
python run_etl.py
```

O pipeline roda em quatro etapas:

1. Extrai os dados da PNADC via API do IBGE e insere o resultado bruto na coleção `PNADC` do banco `IBGE` no MongoDB configurado.
2. Relê esses mesmos dados do MongoDB e os transforma em um DataFrame (uma linha por período).
3. Salva o DataFrame transformado na tabela `pnadc` do banco SQLite local `ibge.db` (arquivo gerado na raiz do projeto, não versionado).
4. Salva o mesmo DataFrame na tabela `pnadc` do banco no Neon.

### Orquestrado com Prefect

O arquivo `orchestration_prefect.py` define o mesmo pipeline como um *flow* do Prefect (`etl_pnadc_flow`), dividido em *tasks*:

| Task | O que faz | Retentativas |
|------|-----------|--------------|
| `extract` | busca a série na API do IBGE (`Extract.pnadc`) | 3, com 10 s de intervalo |
| `load_data` | insere o resultado bruto no MongoDB (`IBGE.PNADC`) | — |
| `transform` | relê a coleção do MongoDB e gera o DataFrame | — |
| `load_neon` | grava o DataFrame na tabela `pnadc` do Neon | 3, com 10 s de intervalo |

O flow também tem `retries=3` e usa o logger do Prefect (`get_run_logger`) para registrar quantos registros passaram por cada etapa. Diferente do `run_etl.py`, ele **não** grava no SQLite: o destino final é apenas o Neon.

Para executar:

```bash
python orchestration_prefect.py
```

Por padrão, o flow usa a variável `4099` (Taxa de desocupação) e a UF `26` (Pernambuco). Para outros valores, chame o flow passando os parâmetros, por exemplo `etl_pnadc_flow(variavel=4097, estado=35)`.

Para acompanhar as execuções (runs, tasks, logs e retentativas) pela interface web, suba o servidor local do Prefect em outro terminal antes de rodar o flow:

```bash
prefect server start
```

e acesse http://127.0.0.1:4200. Se o servidor não estiver rodando, o Prefect sobe um servidor temporário só para aquela execução.

## Ideias para quem quiser ir além

Este projeto foi pensado como material de estudo, priorizando simplicidade. Um ponto que dá margem para explorar conceitos mais avançados de POO é o gerenciamento das conexões com o MongoDB em `Load` e `Extract`:

- Hoje o `MongoClient` é criado uma única vez no `__init__` e fechado ao final de `load_mongo`. Isso funciona bem quando `load_mongo` é chamado uma única vez por execução (como em `run_etl.py`). No flow do Prefect, cada task cria sua própria instância de `Extract`/`Load`, então cada uma abre a sua conexão.
- Se `load_mongo` precisasse ser chamado várias vezes na mesma execução (por exemplo, para inserir em coleções diferentes), a conexão seria reaberta e fechada a cada chamada. Uma otimização possível é criar a conexão de forma "preguiçosa" (lazy), reaproveitando-a entre chamadas e deixando o encerramento por conta de quem orquestra o pipeline.

Fica como desafio para quem quiser se aprofundar em gerenciamento de recursos e ciclo de vida de objetos em Python.