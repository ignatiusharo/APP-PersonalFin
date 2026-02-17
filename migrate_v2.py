import pandas as pd
import os
import re
import requests
from datetime import datetime
import dropbox
from utils.dropbox_client import DropboxManager

# 1. Load Secrets
url = ""
key = ""
dbx_token = ""
dbx_refresh = ""
dbx_app_key = ""
dbx_app_secret = ""

try:
    with open(".streamlit/secrets.toml", "r") as f:
        content = f.read()
        url_match = re.search(r'url\s*=\s*"(.*?)"', content)
        key_match = re.search(r'key\s*=\s*"(.*?)"', content)
        
        if url_match: url = url_match.group(1)
        if key_match: key = key_match.group(1)
        
        # Dropbox (Handling both styles)
        rt_match = re.search(r'refresh_token\s*=\s*"(.*?)"', content)
        ak_match = re.search(r'app_key\s*=\s*"(.*?)"', content)
        as_match = re.search(r'app_secret\s*=\s*"(.*?)"', content)
        at_match = re.search(r'access_token\s*=\s*"(.*?)"', content)
        
        if rt_match: dbx_refresh = rt_match.group(1)
        if ak_match: dbx_app_key = ak_match.group(1)
        if as_match: dbx_app_secret = as_match.group(1)
        if at_match: dbx_token = at_match.group(1)

    # Cleanup URL
    if url.endswith("/"): url = url[:-1]
    rest_url = f"{url}/rest/v1"

except Exception as e:
    print(f"Error reading secrets: {e}")
    exit(1)

headers = {
    "apikey": key,
    "Authorization": f"Bearer {key}",
    "Content-Type": "application/json",
    "Prefer": "return=representation"
}

# 2. Init Dropbox Client
if dbx_refresh and dbx_app_key and dbx_app_secret:
    dbx_manager = DropboxManager(refresh_token=dbx_refresh, app_key=dbx_app_key, app_secret=dbx_app_secret)
else:
    dbx_manager = DropboxManager(access_token=dbx_token)

# 3. Download Data from Dropbox
print("--- Downloading data from Dropbox ---")
data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

PATH_BANCO = os.path.join(data_dir, "base_cc_santander.csv")
PATH_CAT = os.path.join(data_dir, "categorias.csv")
PATH_PRESUPUESTO = os.path.join(data_dir, "presupuesto.csv")

# Attempt downloads but don't crash if optional files are missing
files_to_download = [
    ("/base_cc_santander.csv", PATH_BANCO),
    ("/categorias.csv", PATH_CAT),
    ("/presupuesto.csv", PATH_PRESUPUESTO)
]

for dbx_path, local_path in files_to_download:
    try:
        ok, msg = dbx_manager.download_file(dbx_path, local_path)
        if ok:
            print(f"Downloaded: {dbx_path}")
        else:
            print(f"Warning: Could not download {dbx_path}: {msg}")
    except Exception as e:
        print(f"Error downloading {dbx_path}: {e}")

def calculate_period(dt_str):
    try:
        dt = pd.to_datetime(dt_str, dayfirst=True)
        month = dt.month
        year = dt.year
        if dt.day >= 25:
            month += 1
            if month > 12:
                month = 1
                year += 1
        months_es = {1: 'ene', 2: 'feb', 3: 'mar', 4: 'abr', 5: 'may', 6: 'jun',
                     7: 'jul', 8: 'ago', 9: 'sep', 10: 'oct', 11: 'nov', 12: 'dic'}
        return f"{months_es[month]}-{year}"
    except:
        return "err"

# 4. Process Categories
cat_map = {}
if os.path.exists(PATH_CAT):
    print("--- Migrating categories ---")
    df_cat_map = pd.read_csv(PATH_CAT)
    col_cat_name = [c for c in df_cat_map.columns if 'categor' in c.lower()][0]
    col_tipo_name = [c for c in df_cat_map.columns if 'tipo' in c.lower()][0]

    categories_data = []
    for _, row in df_cat_map.iterrows():
        categories_data.append({
            "name": str(row[col_cat_name]).strip(),
            "type": str(row[col_tipo_name]).strip(),
            "grouper": "Sin Agrupar"
        })

    # Deduplicate
    unique_cats = {c['name']: c for c in categories_data}.values()

    upsert_headers = headers.copy()
    upsert_headers["Prefer"] = "return=representation,resolution=merge-duplicates"

    res = requests.post(f"{rest_url}/categories", json=list(unique_cats), headers=upsert_headers)
    if res.status_code not in [200, 201]:
        print(f"Categories error: {res.text}")
        res = requests.get(f"{rest_url}/categories", headers=headers)

    categories_from_db = res.json()
    print(f"Categories in DB: {len(categories_from_db)}")
    cat_map = {c['name']: c['id'] for c in categories_from_db}
else:
    print("Skipping categories migration (file missing).")

# 5. Process Facts
if os.path.exists(PATH_BANCO):
    print("--- Migrating facts ---")
    df_movs = pd.read_csv(PATH_BANCO)
    facts_data = []
    if 'Fecha' in df_movs.columns:
        for _, row in df_movs.iterrows():
            cat_name = str(row.get('Categoria', 'Pendiente')).strip()
            cat_id = cat_map.get(cat_name)
            
            try:
                raw_date = str(row['Fecha'])
                dt = pd.to_datetime(raw_date, dayfirst=True)
                iso_date = dt.strftime('%Y-%m-%d')
                period = calculate_period(raw_date)
            except:
                continue

            facts_data.append({
                "date": iso_date,
                "period": period,
                "detail": str(row.get('Detalle', '')),
                "amount": float(row.get('Monto', 0)),
                "bank": str(row.get('Banco', 'Santander')),
                "category_id": cat_id,
                "status": "Conciliado"
            })

    # Batch upload facts
    BATCH_SIZE = 100
    for i in range(0, len(facts_data), BATCH_SIZE):
        batch = facts_data[i:i+BATCH_SIZE]
        res = requests.post(f"{rest_url}/facts", json=batch, headers=headers)
        if res.status_code not in [200, 201]:
            print(f"Facts batch error: {res.text}")
    print(f"Facts migrated: {len(facts_data)}")
else:
    print("Skipping facts migration (file missing).")

# 6. Process Budget
if os.path.exists(PATH_PRESUPUESTO):
    print("--- Migrating budget ---")
    df_budget_csv = pd.read_csv(PATH_PRESUPUESTO)
    budget_data = []
    for _, row in df_budget_csv.iterrows():
        cat_name = str(row['Categoria']).strip()
        cat_id = cat_map.get(cat_name)
        if not cat_id: continue
        
        for col in df_budget_csv.columns:
            if col == 'Categoria': continue
            
            try:
                dt_col = pd.to_datetime(col + "-01")
                months_es = {1: 'ene', 2: 'feb', 3: 'mar', 4: 'abr', 5: 'may', 6: 'jun',
                             7: 'jul', 8: 'ago', 9: 'sep', 10: 'oct', 11: 'nov', 12: 'dic'}
                period_name = f"{months_es[dt_col.month]}-{dt_col.year}"
                
                val = float(row[col])
                if val != 0:
                    budget_data.append({
                        "category_id": cat_id,
                        "period": period_name,
                        "amount": val
                    })
            except:
                continue

    if budget_data:
        res = requests.post(f"{rest_url}/budget", json=budget_data, headers=headers)
        if res.status_code not in [200, 201]:
            print(f"Budget error: {res.text}")
    print(f"Budget migrated: {len(budget_data)}")
else:
    print("Skipping budget migration (file missing).")

print("\n--- MIGRATION COMPLETE ---")
