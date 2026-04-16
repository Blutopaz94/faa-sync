import os
import requests
import zipfile
import pandas as pd
from supabase import create_client

def get_supabase_client():
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not url or not key:
        raise Exception("❌ Missing Supabase Secrets!")
    return create_client(url, key)

def update_registry():
    faa_url = "https://registry.faa.gov/database/ReleasableAircraft.zip"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
    }

    print("Downloading FAA Database...")
    r = requests.get(faa_url, headers=headers, stream=True, timeout=120)
    
    if r.status_code == 200:
        with open("faa_data.zip", "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        print("✅ Download successful.")
    else:
        print(f"❌ Server Error {r.status_code}")
        return

    print("Extracting MASTER.txt...")
    with zipfile.ZipFile("faa_data.zip", "r") as zip_ref:
        zip_ref.extract("MASTER.txt")

    print("Processing Data...")
    # Read the file. Using encoding='utf-8-sig' handles the hidden BOM characters
    df = pd.read_csv("MASTER.txt", encoding='ISO-8859-1', low_memory=False)
    
    # CLEAN COLUMNS AGGRESSIVELY
    # This strips spaces AND removes hidden special characters
    df.columns = df.columns.str.strip().str.replace('"', '').str.replace("'", "")
    
    print(f"Detected columns: {list(df.columns[:10])}...") # Debug print

    # Helper to find column names even if they change slightly (e.g. AC-WEIGHT vs AC WEIGHT)
    def find_col(target):
        for col in df.columns:
            if target.upper() in col.upper().replace(' ', '').replace('-', ''):
                return col
        return None

    # Identify the actual column names in this specific file
    col_weight = find_col('ACWEIGHT')
    col_status = find_col('REGSTATUS')
    col_type = find_col('TYPEACFT')
    col_nnum = find_col('NNUMBER')
    col_mfr = find_col('MFR')
    col_model = find_col('MODEL')
    col_year = find_col('YEARMFR')

    if not col_weight:
        raise Exception(f"Could not find Weight column. Available columns: {df.columns}")

    print(f"Using columns: {col_weight}, {col_status}, {col_type}")

    # FILTER: Small Fixed-Wing Only (Type 4 & 5, Weight Class 1)
    # We use .astype(str) to ensure we can strip and compare accurately
    filtered_df = df[
        (df[col_weight].astype(str).str.contains('CLASS 1', case=False, na=False)) & 
        (df[col_status].astype(str).str.strip().upper() == 'A') &
        (df[col_type].astype(str).str.strip().isin(['4', '5']))
    ].copy()

    final_df = pd.DataFrame()
    final_df['n_number'] = "N" + filtered_df[col_nnum].astype(str).str.strip()
    final_df['mfr'] = filtered_df[col_mfr].astype(str).str.strip()
    final_df['model'] = filtered_df[col_model].astype(str).str.strip()
    final_df['year'] = filtered_df[col_year].astype(str).str.strip()

    records = final_df.to_dict('records')
    print(f"Found {len(records)} aircraft. Syncing to Supabase...")

    supabase = get_supabase_client()
    # Batch sync
    for i in range(0, len(records), 500):
        batch = records[i:i+500]
        supabase.table("FAA Small Aircraft").upsert(batch).execute()

    print("🎉 MISSION COMPLETE.")

if __name__ == "__main__":
    update_registry()
