import streamlit as st
import pandas as pd
import os
from utils.dropbox_client import DropboxManager

# Configuración de página
st.set_page_config(page_title="Mi Conciliador Pro", layout="wide")

# Rutas de archivos
PATH_BANCO = "data/base_cc_santander.csv"
PATH_CAT = "data/categorias.csv"

# --- DROPBOX CONFIG ---
if 'dropbox' in st.secrets:
    dbx = DropboxManager(st.secrets['dropbox']['access_token'])
else:
    dbx = None

# Sidebar Sync
with st.sidebar:
    st.header("☁️ Respaldo Cloud")
    if dbx:
        if st.button("🔄 Sincronizar (Descargar)"):
            with st.spinner("Descargando de Dropbox..."):
                ok1, msg1 = dbx.download_file("/base_cc_santander.csv", PATH_BANCO)
                ok2, msg2 = dbx.download_file("/categorias.csv", PATH_CAT)
                if ok1 or ok2:
                    st.success("✅ Descarga completada")
                    st.rerun()
                else:
                    st.warning(f"Info: {msg1}")
    else:
        st.error("⚠️ Token no configurado")



# --- FUNCIONES DE APOYO ---
def cargar_datos():
    if os.path.exists(PATH_BANCO):
        return pd.read_csv(PATH_BANCO)
    return pd.DataFrame(columns=['Fecha', 'Detalle', 'Monto', 'Banco', 'Categoria'])

def cargar_categorias():
    if os.path.exists(PATH_CAT):
        return pd.read_csv(PATH_CAT)['categoria'].tolist()
    return ["Alimentación", "Transporte", "Vivienda", "Ocio", "Suscripciones", "Pendiente"]

def procesar_archivo(archivo):
    """Detecta el tipo de archivo y lo procesa automáticamente"""
    try:
        # 1. Intento de lectura Santander (.xlsx)
        if archivo.name.endswith('.xlsx'):
            df_meta = pd.read_excel(archivo, nrows=5, header=None)
            texto_completo = df_meta.astype(str).values.flatten()
            texto_completo = " ".join(texto_completo)
            
            CUENTA_PROPIA = "0-000-74-80946-4"
            
            if CUENTA_PROPIA in texto_completo:
                # Usamos header=0 y buscamos la fila donde están los títulos reales
                df = pd.read_excel(archivo, skiprows=2) 
                df.columns = df.columns.str.strip() # Limpieza de espacios
                
                # Buscamos columnas por nombre parcial para evitar el KeyError
                col_cargo = [c for c in df.columns if 'Monto cargo' in c][0]
                col_abono = [c for c in df.columns if 'Monto abono' in c][0]
                col_fecha = [c for c in df.columns if 'Fecha' in c][0]
                col_detalle = [c for c in df.columns if 'Detalle' in c][0]
                
                df['Monto'] = pd.to_numeric(df[col_abono], errors='coerce').fillna(0) - \
                              pd.to_numeric(df[col_cargo], errors='coerce').fillna(0)
                
                df_final = df[[col_fecha, col_detalle, 'Monto']].copy()
                df_final.columns = ['Fecha', 'Detalle', 'Monto']
                df_final['Banco'] = 'CC Santander'
                df_final['Categoria'] = 'Pendiente'
                st.success(f"✅ Santander detectado: Cuenta {CUENTA_PROPIA}")
                return df_final
            
        # 2. Intento de lectura Genérico (.csv)
        elif archivo.name.endswith('.csv'):
            df = pd.read_csv(archivo, sep=None, engine='python')
            df.columns = df.columns.str.strip()
            columnas_req = {'Fecha', 'Detalle', 'Monto'}
            if columnas_req.issubset(df.columns):
                df['Banco'] = 'Genérico'
                df['Categoria'] = 'Pendiente'
                st.success("✅ Archivo CSV estándar detectado")
                return df[list(columnas_req) + ['Banco', 'Categoria']]
        
        st.error("❌ Formato no reconocido o cuenta no autorizada.")
        return None
    except Exception as e:
        st.error(f"❌ Error al procesar: {str(e)}")
        return None

# --- INTERFAZ ---
st.title("💰 Conciliador Bancario Inteligente")

tab1, tab2 = st.tabs(["📥 Cargar Cartola", "📊 Conciliación y Categorías"])

with tab1:
    st.header("Carga de Datos")
    archivo = st.file_uploader("Arrastra tu cartola aquí (.xlsx o .csv)", type=["xlsx", "csv"])
    
    if archivo:
        df_nuevo = procesar_archivo(archivo)
        
        if df_nuevo is not None:
            st.write("### Vista previa de carga:")
            st.dataframe(df_nuevo.head())
            
            if st.button("Confirmar e Insertar en Base de Datos"):
                df_hist = cargar_datos()
                # Unimos y eliminamos duplicados exactos
                df_unificado = pd.concat([df_hist, df_nuevo]).drop_duplicates(
                    subset=['Fecha', 'Detalle', 'Monto'], keep='first'
                )
                df_unificado.to_csv(PATH_BANCO, index=False)
                st.balloons()
                st.success(f"Sincronizado: {len(df_nuevo)} registros procesados.")
                
                # Auto Backup
                if dbx:
                    ok, msg = dbx.upload_file(PATH_BANCO, "/base_cc_santander.csv")
                    if ok: st.toast("☁️ Respaldo en Dropbox actualizado")
                    else: st.error(f"Error respaldo: {msg}")

with tab2:
    st.header("Listado de Movimientos")
    df_cat = cargar_datos()
    lista_categorias = cargar_categorias()
    
    if not df_cat.empty:
        df_editado = st.data_editor(
            df_cat,
            column_config={
                "Categoria": st.column_config.SelectboxColumn("Categoría", options=lista_categorias, required=True),
                "Monto": st.column_config.NumberColumn(format="$%d")
            },
            num_rows="dynamic",
            hide_index=True,
            use_container_width=True
        )
        
        if st.button("Guardar Cambios Finales"):
            df_editado.to_csv(PATH_BANCO, index=False)
            st.success("Cambios guardados en la base de datos.")
            
            # Auto Backup
            if dbx:
                ok, msg = dbx.upload_file(PATH_BANCO, "/base_cc_santander.csv")
                if ok: st.toast("☁️ Respaldo en Dropbox actualizado", icon="☁️")
                else: st.error(f"Error respaldo: {msg}")
    else:
        st.info("Bandeja de entrada vacía.")