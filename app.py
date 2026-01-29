import streamlit as st
import pandas as pd
import os

# Configuración de página
st.set_page_config(page_title="Mi Conciliador Pro", layout="wide")

# Rutas de archivos
PATH_BANCO = "data/base_cc_santander.csv"
PATH_CAT = "data/categorias.csv"

# --- FUNCIONES DE CARGA ---
def validar_y_procesar_santander(archivo):
    # 1. Lectura rápida de las primeras filas para buscar la cuenta
    # Leemos sin saltar filas para capturar la 'basura' que tiene la info de cuenta
    df_meta = pd.read_excel(archivo, nrows=5, header=None)
    
    # Buscamos la celda que contiene el número de cuenta
    texto_cabecera = ""
    for col in df_meta.columns:
        texto_cabecera += " ".join(df_meta[col].astype(str))
    
    CUENTA_PROPIA = "0-000-74-80946-4"
    
    if CUENTA_PROPIA in texto_cabecera:
        st.success(f"✅ Archivo Validado: Cuenta Santander {CUENTA_PROPIA}")
        
        # 2. Ahora sí, procesamos los datos reales saltando las filas de cabecera
        df = pd.read_excel(archivo, skiprows=3)
        
        # Estandarizamos nombres de columnas (manejando posibles espacios extra)
        df.columns = df.columns.str.strip()
        
        # 3. Lógica de montos unificada
        df['Monto cargo ($)'] = pd.to_numeric(df['Monto cargo ($)'], errors='coerce').fillna(0)
        df['Monto abono ($)'] = pd.to_numeric(df['Monto abono ($)'], errors='coerce').fillna(0)
        
        df['Monto'] = df['Monto abono ($)'] - df['Monto cargo ($)']
        
        # 4. Formateo final para la base de datos
        df_final = df[['Fecha', 'Detalle', 'Monto']].copy()
        df_final['Banco'] = 'CC Santander'
        df_final['Categoria'] = 'Pendiente'
        
        return df_final
    else:
        st.error("❌ El archivo no corresponde a la cuenta configurada o el formato no es Santander.")
        return None

# --- INTERFAZ ---
st.title("💰 Conciliador Bancario Inteligente")

tab1, tab2 = st.tabs(["📥 Cargar Cartola", "📊 Conciliación y Categorías"])

with tab1:
    st.header("Carga de Datos")
    # Mantenemos el selector por ahora, pero la lógica será más estricta
    banco_sel = st.selectbox("Formato de Cartola", ["Seleccione...", "Santander (.xlsx)"])
    
    archivo = st.file_uploader("Subir archivo bancario", type=["xlsx", "csv"])
    
    if archivo and banco_sel != "Seleccione...":
        if banco_sel == "Santander (.xlsx)":
            # Llamamos a la función pro que creamos recién
            df_nuevo = validar_y_procesar_santander(archivo)
            
            if df_nuevo is not None:
                st.write("### Vista previa de los datos a cargar:")
                st.dataframe(df_nuevo.head())
                
                if st.button("Confirmar e Insertar en Base de Datos"):
                    df_hist = cargar_datos()
                    
                    # --- LÓGICA DE DUPLICADOS MEJORADA ---
                    # 1. Unimos lo viejo con lo nuevo
                    # 2. Eliminamos duplicados exactos basados en Fecha, Detalle y Monto
                    df_final = pd.concat([df_hist, df_nuevo]).drop_duplicates(
                        subset=['Fecha', 'Detalle', 'Monto'], 
                        keep='first'
                    )
                    
                    # Guardamos en el CSV
                    df_final.to_csv(PATH_BANCO, index=False)
                    st.balloons()
                    st.success(f"¡Proceso terminado! Se sincronizaron los movimientos de la cuenta.")
        else:
            st.info("Próximamente añadiremos más formatos de bancos.")

with tab2:
    df_cat = cargar_datos()
    if not df_cat.empty:
        # st.data_editor con num_rows="dynamic" permite borrar filas con la tecla 'Delete' 
        # o seleccionando y usando el icono de papelera que aparecerá.
        df_editado = st.data_editor(
            df_cat,
            column_config={"Categoria": st.column_config.SelectboxColumn("Categoría", options=lista_categorias)},
            num_rows="dynamic", # <--- ESTO PERMITE ELIMINAR FILAS
            hide_index=False
        )
        
        if st.button("Guardar Cambios Finales"):
            df_editado.to_csv(PATH_BANCO, index=False)
            st.success("Base de datos actualizada.")