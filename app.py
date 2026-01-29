import streamlit as st
import pandas as pd
import os

# Configuración de página
st.set_page_config(page_title="Mi Conciliador Pro", layout="wide")

# Rutas de archivos
PATH_BANCO = "data/base_cc_santander.csv"
PATH_CAT = "data/categorias.csv"

# --- FUNCIONES DE APOYO (LAS QUE FALTABAN) ---
def cargar_datos():
    if os.path.exists(PATH_BANCO):
        return pd.read_csv(PATH_BANCO)
    return pd.DataFrame(columns=['Fecha', 'Detalle', 'Monto', 'Banco', 'Categoria'])

def cargar_categorias():
    if os.path.exists(PATH_CAT):
        return pd.read_csv(PATH_CAT)['categoria'].tolist()
    return ["Alimentación", "Transporte", "Vivienda", "Ocio", "Suscripciones", "Pendiente"]

# --- FUNCIÓN DE PROCESAMIENTO SANTANDER ---
def validar_y_procesar_santander(archivo):
    df_meta = pd.read_excel(archivo, nrows=5, header=None)
    texto_cabecera = ""
    for col in df_meta.columns:
        texto_cabecera += " ".join(df_meta[col].astype(str))
    
    CUENTA_PROPIA = "0-000-74-80946-4"
    
    if CUENTA_PROPIA in texto_cabecera:
        st.success(f"✅ Archivo Validado: Cuenta Santander {CUENTA_PROPIA}")
        df = pd.read_excel(archivo, skiprows=3)
        df.columns = df.columns.str.strip()
        
        # Convertir a número por si acaso vienen como texto con puntos
        df['Monto cargo ($)'] = pd.to_numeric(df['Monto cargo ($)'], errors='coerce').fillna(0)
        df['Monto abono ($)'] = pd.to_numeric(df['Monto abono ($)'], errors='coerce').fillna(0)
        
        df['Monto'] = df['Monto abono ($)'] - df['Monto cargo ($)']
        
        df_final = df[['Fecha', 'Detalle', 'Monto']].copy()
        df_final['Banco'] = 'CC Santander'
        df_final['Categoria'] = 'Pendiente'
        return df_final
    else:
        st.error(f"❌ El archivo no corresponde a la cuenta {CUENTA_PROPIA}")
        return None

# --- INTERFAZ ---
st.title("💰 Conciliador Bancario Inteligente")

tab1, tab2 = st.tabs(["📥 Cargar Cartola", "📊 Conciliación y Categorías"])

with tab1:
    st.header("Carga de Datos")
    banco_sel = st.selectbox("Formato de Cartola", ["Seleccione...", "Santander (.xlsx)"])
    archivo = st.file_uploader("Subir archivo bancario", type=["xlsx", "csv"])
    
    if archivo and banco_sel != "Seleccione...":
        if banco_sel == "Santander (.xlsx)":
            df_nuevo = validar_y_procesar_santander(archivo)
            
            if df_nuevo is not None:
                st.write("### Vista previa de los datos a cargar:")
                st.dataframe(df_nuevo.head())
                
                if st.button("Confirmar e Insertar en Base de Datos"):
                    df_hist = cargar_datos()
                    # Unión y eliminación de duplicados
                    df_final = pd.concat([df_hist, df_nuevo]).drop_duplicates(
                        subset=['Fecha', 'Detalle', 'Monto'], 
                        keep='first'
                    )
                    df_final.to_csv(PATH_BANCO, index=False)
                    st.balloons()
                    st.success("¡Datos sincronizados exitosamente!")

with tab2:
    st.header("Listado de Movimientos")
    df_cat = cargar_datos()
    lista_categorias = cargar_categorias()
    
    if not df_cat.empty:
        # Editor de datos con eliminación de filas habilitada
        df_editado = st.data_editor(
            df_cat,
            column_config={
                "Categoria": st.column_config.SelectboxColumn(
                    "Categoría", 
                    options=lista_categorias,
                    required=True
                )
            },
            num_rows="dynamic",
            hide_index=True,
            width=1200
        )
        
        if st.button("Guardar Cambios Finales"):
            df_editado.to_csv(PATH_BANCO, index=False)
            st.success("Base de datos actualizada.")
    else:
        st.info("No hay datos cargados aún. Ve a la pestaña 'Cargar Cartola'.")