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

    print("Loading and Merging Data...")
    # Load Master list (Planes)
    master = pd.read_csv("MASTER.txt", encoding='ISO-8859-1', low_memory=False)
    master.columns = master.columns.str.strip().str.replace('ï»¿', '')

    # Load Reference list (Weights and Types)
    ref = pd.read_csv("ACFTREF.txt", encoding='ISO-8859-1', low_memory=False)
    ref.columns = ref.columns.str.strip()

    # Join the tables on the Manufacturer/Model Code
    # In MASTER it is 'MFR MDL CODE', in ACFTREF it is 'CODE'
    df = pd.merge(master, ref, left_on='MFR MDL CODE', right_on='CODE', how='inner')

    print("Filtering for Small Fixed-Wing Aircraft...")
    # --- UPDATED FILTER ---
    # AC-WEIGHT: CLASS 1 (Small)
    # STATUS CODE: A (Active)
    # TYPE-ACFT_y: 4 or 5 (Fixed wing single/multi engine)
    filtered_df = df[
        (df['AC-WEIGHT'].str.strip() == 'CLASS 1') & 
        (df['STATUS CODE'].str.strip() == 'A') &
        (df['TYPE-ACFT_y'].astype(str).str.strip().isin(['4', '5']))
    ].copy()

    # Map to your Supabase columns
    final_df = pd.DataFrame()
    final_df['n_number'] = "N" + filtered_df['N-NUMBER'].astype(str).str.strip()
    final_df['mfr'] = filtered_df['MFR_y'].astype(str).str.strip() # Use official MFR from reference
    final_df['model'] = filtered_df['MODEL_y'].astype(str).str.strip() # Use official Model from reference
    final_df['year'] = filtered_df['YEAR MFR'].astype(str).str.strip()

    records = final_df.to_dict('records')
    print(f"Found {len(records)} aircraft matching your criteria.")

    # Sync to Supabase
    supabase = get_supabase_client()
    print("Syncing to Supabase in batches of 500...")
    
    for i in range(0, len(records), 500):
        batch = records[i:i+500]
        try:
            supabase.table("FAA Small Aircraft").upsert(batch).execute()
        except Exception as e:
            print(f"⚠️ Error in batch: {e}")

    print("🎉 MISSION COMPLETE: Database updated.")

if __name__ == "__main__":
    update_registry()
