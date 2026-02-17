import dropbox
import os
import re

# Simple parsing of secrets.toml
access_token = None
refresh_token = None
app_key = None
app_secret = None

try:
    with open(".streamlit/secrets.toml", "r") as f:
        content = f.read()
        at_match = re.search(r'access_token\s*=\s*"(.*?)"', content)
        rt_match = re.search(r'refresh_token\s*=\s*"(.*?)"', content)
        ak_match = re.search(r'app_key\s*=\s*"(.*?)"', content)
        as_match = re.search(r'app_secret\s*=\s*"(.*?)"', content)
        
        if at_match: access_token = at_match.group(1)
        if rt_match: refresh_token = rt_match.group(1)
        if ak_match: app_key = ak_match.group(1)
        if as_match: app_secret = as_match.group(1)

    if refresh_token and app_key and app_secret and refresh_token != "DEJAR_VACIO_POR_AHORA":
        dbx = dropbox.Dropbox(
            oauth2_refresh_token=refresh_token,
            app_key=app_key,
            app_secret=app_secret
        )
    else:
        dbx = dropbox.Dropbox(access_token)

    print("--- Dropbox Connection Test ---")
    try:
        acc = dbx.users_get_current_account()
        print(f"Connected as: {acc.name.display_name}")
    except Exception as e:
        print(f"Connection Failed: {e}")
        exit(1)

    print("\n--- Listing Files in Root ---")
    res = dbx.files_list_folder("")
    for entry in res.entries:
        if isinstance(entry, dropbox.files.FileMetadata):
            print(f"FILE: {entry.path_display} (Size: {entry.size} bytes, Modified: {entry.client_modified})")
        else:
            print(f"DIR: {entry.path_display}")

    # Check for file
    target = "/base_cc_santander.csv"
    print(f"\n--- Checking {target} ---")
    try:
        meta = dbx.files_get_metadata(target)
        print(f"FOUND: {meta.name}")
        print(f"Size: {meta.size} bytes")
    except Exception as e:
        print(f"ERROR: {e}")

except Exception as e:
    print(f"Diagnostic failed: {e}")
