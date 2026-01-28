import streamlit as st
import pandas as pd
import os

# Configuración de página
st.set_page_config(page_title="Mi Conciliador Pro", layout="wide")

# Rutas de archivos
PATH_BANCO = "data/base_cc_santander.csv"
PATH_CAT = "data/categorias.csv"

# --- FUNCIONES DE CARGA ---
def cargar_datos():
    if os.path.exists(PATH_BANCO):
        return pd.read_csv(PATH_BANCO)
    return pd.DataFrame(columns=['Fecha', 'Detalle', 'Monto', 'Banco', 'Categoria'])

def cargar_categorias():
    return pd.read_csv(PATH_CAT)['categoria'].tolist()

# --- INTERFAZ ---
st.title("💰 Conciliador Bancario Inteligente")

tab1, tab2 = st.tabs(["📥 Cargar Cartola", "📊 Conciliación y Categorías"])

with tab1:
    st.header("Carga de Datos")
    banco_sel = st.selectbox("Selecciona el Banco", ["Santander", "Chile", "BCI"])
    
    archivo = st.file_uploader("Sube tu archivo (CSV)", type=["csv"])
    
    if archivo:
        df_nuevo = pd.read_csv(archivo)
        columnas_requeridas = ['Fecha', 'Detalle', 'Monto']
        
        # 1. Validar estructura
        if set(columnas_requeridas).issubset(df_nuevo.columns):
            st.success("✅ Estructura válida")
            
            if st.button("Cargar Cartola"):
                # 2. Evitar duplicados
                df_hist = cargar_datos()
                
                # Crear una marca única para comparar (Fecha + Detalle + Monto)
                # Esto evita que carguemos lo mismo dos veces
                df_nuevo['check'] = df_nuevo['Fecha'].astype(str) + df_nuevo['Detalle'] + df_nuevo['Monto'].astype(str)
                df_hist['check'] = df_hist['Fecha'].astype(str) + df_hist['Detalle'] + df_hist['Monto'].astype(str)
                
                nuevos_registros = df_nuevo[~df_nuevo['check'].isin(df_hist['check'])].copy()
                
                if not nuevos_registros.empty:
                    nuevos_registros['Banco'] = banco_sel
                    nuevos_registros['Categoria'] = "Pendiente"
                    df_final = pd.concat([df_hist, nuevos_registros.drop(columns=['check'])], ignore_index=True)
                    df_final.to_csv(PATH_BANCO, index=False)
                    st.balloons()
                    st.success(f"Se agregaron {len(nuevos_registros)} nuevas líneas.")
                else:
                    st.warning("No hay movimientos nuevos que agregar.")
        else:
            st.error(f"Estructura incorrecta. Se esperaba: {columnas_requeridas}")

with tab2:
    st.header("Listado de Movimientos")
    df_cat = cargar_datos()
    lista_categorias = cargar_categorias()
    
    if not df_cat.empty:
        # Usamos data_editor para que puedas editar la columna Categoría
        df_editado = st.data_editor(
            df_cat,
            column_config={
                "Categoria": st.column_config.SelectboxColumn(
                    "Categoría",
                    help="Clasifica tu gasto",
                    options=lista_categorias,
                    required=True,
                )
            },
            disabled=["Fecha", "Detalle", "Monto", "Banco"], # No dejamos editar el resto
            hide_index=True,
        )
        
        if st.button("Guardar Conciliación"):
            df_editado.to_csv(PATH_BANCO, index=False)
            st.success("¡Conciliación guardada exitosamente!")
    else:
        st.info("Aún no hay datos cargados.")