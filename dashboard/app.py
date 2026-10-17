import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine

from src.config import DATABASE_URL

# Cada sexo tem sempre a mesma cor, mesmo quando outro é removido no filtro
CORES = {
    "Total": "#2a78d6",
    "Homens": "#eb6834",
    "Mulheres": "#1baf7a",
}

st.set_page_config(page_title="Desocupação em Pernambuco", page_icon="📊", layout="wide")


@st.cache_data(ttl=600)
def carregar_dados() -> pd.DataFrame:
    """
    Lê a tabela `pnadc` do Neon (camada já transformada pelo pipeline).
    O resultado fica em cache por 10 minutos, para não consultar o banco
    a cada clique no dashboard.
    """
    engine = create_engine(DATABASE_URL)
    df = pd.read_sql("SELECT periodo, valor, sexo FROM pnadc ORDER BY periodo", engine)
    engine.dispose()
    return df


def nome_trimestre(data: pd.Timestamp) -> str:
    """Converte 2026-04-01 em '2º tri/2026'."""
    return f"{data.quarter}º tri/{data.year}"


df = carregar_dados()

st.title("Taxa de desocupação — Pernambuco")
st.caption(
    "Fonte: IBGE, PNAD Contínua (agregado 4093, variável 4099). "
    "Pessoas de 14 anos ou mais. Dados lidos da tabela `pnadc` no Neon."
)

# ---------- Filtros ----------
col_sexo, col_anos = st.columns(2)

sexos = col_sexo.multiselect("Sexo", list(CORES), default=list(CORES))

ano_min = int(df["periodo"].dt.year.min())
ano_max = int(df["periodo"].dt.year.max())
anos = col_anos.slider("Anos", ano_min, ano_max, (ano_min, ano_max))

filtro = df[df["sexo"].isin(sexos) & df["periodo"].dt.year.between(anos[0], anos[1])]

if filtro.empty:
    st.warning("Nenhum dado para os filtros selecionados.")
    st.stop()

# ---------- Indicadores do último trimestre ----------
ultimo_periodo = df.dropna(subset=["valor"])["periodo"].max()
st.subheader(f"Último trimestre: {nome_trimestre(ultimo_periodo)}")

colunas = st.columns(len(sexos))
for coluna, sexo in zip(colunas, sexos):
    serie = df[df["sexo"] == sexo].set_index("periodo")["valor"]

    atual = serie.get(ultimo_periodo)
    ano_anterior = serie.get(ultimo_periodo - pd.DateOffset(years=1))

    delta = None
    if pd.notna(atual) and pd.notna(ano_anterior):
        delta = f"{atual - ano_anterior:+.1f} p.p. em 12 meses"

    # delta_color="inverse": desocupação subindo é ruim (vermelho)
    coluna.metric(sexo, f"{atual:.1f}%", delta=delta, delta_color="inverse")

# ---------- Gráfico da série histórica ----------
fig = px.line(
    filtro,
    x="periodo",
    y="valor",
    color="sexo",
    color_discrete_map=CORES,
    category_orders={"sexo": list(CORES)},
    labels={"periodo": "", "valor": "Taxa de desocupação (%)", "sexo": "Sexo"},
)
fig.update_traces(line_width=2, hovertemplate="%{y:.1f}%")
fig.update_layout(
    hovermode="x unified",
    xaxis_hoverformat="%qº tri/%Y",
    yaxis_ticksuffix="%",
    legend=dict(orientation="h", y=1.08, x=0, title=None),
    margin=dict(t=40, b=0),
)
st.plotly_chart(fig, width="stretch")
st.caption("Trimestres sem dado divulgado pelo IBGE (\"...\") aparecem como lacunas na linha.")

# ---------- Tabela com os dados ----------
with st.expander("Ver dados em tabela"):
    tabela = filtro.pivot(index="periodo", columns="sexo", values="valor")
    tabela = tabela[[s for s in CORES if s in tabela.columns]]
    tabela.index = tabela.index.map(nome_trimestre)
    st.dataframe(tabela, width="stretch")
