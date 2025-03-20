import streamlit as st
import pandas as pd
import plotly.express as px

# Cargar el archivo CSV correctamente
DATA_PATH = "former_names.csv"

# Leer el archivo CSV en un DataFrame
df = pd.read_csv(DATA_PATH)

# Convertir las fechas a formato datetime (sin aplicar .strftime aquí)
df["start_date"] = pd.to_datetime(df["start_date"]).dt.date  # Esto elimina la parte de la hora
df["end_date"] = pd.to_datetime(df["end_date"]).dt.date  # Esto también elimina la parte de la hora

# Configuración de la página
st.set_page_config(page_title="Evolución de Países", layout="wide")

# Título de la aplicación
st.title("⚽ Resultados del fútbol internacional de 1872 a 2024 "
             "Evolución de Países y Equipos")

# Filtro por país actual
selected_country = st.selectbox("Selecciona un país", ["Todos"] + sorted(df["current"].unique()))
if selected_country != "Todos":
    df = df[df["current"] == selected_country]

# Mostrar tabla de datos interactiva
st.write("### 📋 Datos de Países y sus Nombres Anteriores")
st.dataframe(df)

# Gráfico de línea: duración de nombres de los países
st.write("### 📊 Duración de los Nombres de los Países")
fig = px.timeline(df, x_start="start_date", x_end="end_date", y="former", color="current",
                  title="Duración de los Nombres Anteriores de los Países",
                  labels={"former": "Nombre Anterior", "current": "País Actual"})
st.plotly_chart(fig, use_container_width=True)

# Métricas de datos
st.write("## 📌 Datos Claves")
col1, col2, col3 = st.columns(3)
col1.metric("Total de Registros", len(df))
col2.metric("Total de Países", df["current"].nunique())

# Ahora que start_date es datetime (sin la parte de hora), puedes aplicar strftime sin problemas.
# Además, para mostrar solo el año, usas strftime después de convertir a datetime.
col3.metric("Periodo Más Antiguo", pd.to_datetime(df["start_date"].min()).strftime("%Y"))

# Gráfico de distribución de años de cambio
st.write("### 📅 Distribución de Fechas de Cambio")
df["year"] = pd.to_datetime(df["start_date"]).dt.year
fig2 = px.histogram(df, x="year", nbins=20, title="Cantidad de Cambios de Nombre por Año")

# Cambiar el nombre de "count" a "Número de Cambios"
fig2.update_layout(
    xaxis_title="Year",  # Título del eje X (si deseas modificarlo también)
    yaxis_title="number of countries",  
)

st.plotly_chart(fig2, use_container_width=True)
