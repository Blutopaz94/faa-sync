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
    headers = {'User-Agent': 'Mozilla/5.0'}

    print("Downloading FAA Database...")
    r = requests.get(faa_url, headers=headers, stream=True, timeout=120)
    
    if r.status_code == 200:
        with open("faa_data.zip", "wb") as f:
            f.write(r.content)
        print("✅ Download successful.")
    else:
        raise Exception(f"❌ FAA Server error {r.status_code}")

    print("Extracting files...")
    with zipfile.ZipFile("faa_data.zip", "r") as zip_ref:
        zip_ref.extract("MASTER.txt")
        zip_ref.extract("ACFTREF.txt")

    print("Loading Data...")
    master = pd.read_csv("MASTER.txt", encoding='ISO-8859-1', low_memory=False)
    ref = pd.read_csv("ACFTREF.txt", encoding='ISO-8859-1', low_memory=False)

    # Clean column names for both files
    for df_temp in [master, ref]:
        df_temp.columns = [c.strip().replace('ï»¿', '') for c in df_temp.columns]
    
    print("Merging Tables...")
    # Join on the Manufacturer/Model code
    df = pd.merge(master, ref, left_on='MFR MDL CODE', right_on='CODE', how='inner')
    
    # Debug: Print all column names available after merge to ensure we have what we need
    print(f"Available columns after merge: {list(df.columns)}")

    print("Filtering for Small Fixed-Wing Aircraft...")
    # Logic: 
    # 'AC-WEIGHT' and 'TYPE-ACFT' usually come from the REF table
    # 'STATUS CODE' usually comes from the MASTER table
    
    # We check if they were renamed during merge, otherwise use standard names
    col_weight = 'AC-WEIGHT' if 'AC-WEIGHT' in df.columns else 'AC-WEIGHT_y'
    col_type = 'TYPE-ACFT' if 'TYPE-ACFT' in df.columns else 'TYPE-ACFT_y'
    col_status = 'STATUS CODE' if 'STATUS CODE' in df.columns else 'STATUS CODE_x'
    col_mfr = 'MFR' if 'MFR' in df.columns else 'MFR_y'
    col_model = 'MODEL' if 'MODEL' in df.columns else 'MODEL_y'

    filtered_df = df[
        (df[col_weight].astype(str).str.strip() == 'CLASS 1') & 
        (df[col_status].astype(str).str.strip() == 'A') &
        (df[col_type].astype(str).str.strip().isin(['4', '5']))
    ].copy()

    # Map to your Supabase columns
    final_df = pd.DataFrame()
    final_df['n_number'] = "N" + filtered_df['N-NUMBER'].astype(str).str.strip()
    final_df['mfr'] = filtered_df[col_mfr].astype(str).str.strip()
    final_df['model'] = filtered_df[col_model].astype(str).str.strip()
    final_df['year'] = filtered_df['YEAR MFR'].astype(str).str.strip()

    records = final_df.to_dict('records')
    print(f"Found {len(records)} aircraft matching criteria.")

    # 3. Sync to Supabase
    supabase = get_supabase_client()
    print("Syncing to Supabase in batches of 500...")
    
    for i in range(0, len(records), 500):
        batch = records[i:i+500]
        try:
            supabase.table("FAA Small Aircraft").upsert(batch).execute()
        except Exception as e:
            print(f"⚠️ Error in batch starting at {i}: {e}")

    print("🎉 MISSION COMPLETE: Database updated.")

if __name__ == "__main__":
    update_registry()
