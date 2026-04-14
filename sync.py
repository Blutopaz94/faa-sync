import os
import requests
import zipfile
import pandas as pd
from supabase import create_client
from datetime import datetime, timedelta

# 1. Setup Supabase
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_SERVICE_KEY")
supabase = create_client(url, key)

def update_registry():
    now = datetime.now()
    # Try current month, then previous month
    months_to_try = [now, now.replace(day=1) - timedelta(days=1)]
    
    success = False
    for date_to_check in months_to_try:
        faa_filename = f"AR{date_to_check.strftime('%m%Y')}.zip"
        faa_url = f"https://registry.faa.gov/database/{faa_filename}"
        
        print(f"Attempting download: {faa_url}")
        r = requests.get(faa_url)
        
        if r.status_code == 200:
            with open("faa_data.zip", "wb") as f:
                f.write(r.content)
            success = True
            print(f"✅ Successfully downloaded {faa_filename}")
            break
        else:
            print(f"❌ {faa_filename} not found.")

    if not success:
        raise Exception("Could not find a valid FAA database file for the last two months.")

    print("Extracting MASTER.txt...")
    with zipfile.ZipFile("faa_data.zip", "r") as zip_ref:
        zip_ref.extract("MASTER.txt")

    print("Filtering Data (Small GA & Helicopters)...")
    # CRITICAL: We use encoding='ISO-8859-1' because FAA data is not standard UTF-8
    # We use low_memory=False to handle the large file size
    try:
        df = pd.read_csv("MASTER.txt", encoding='ISO-8859-1', low_memory=False)
    except Exception as e:
        print(f"Standard read failed, trying alternate encoding: {e}")
        df = pd.read_csv("MASTER.txt", encoding='cp1252', low_memory=False)

    df.columns = df.columns.str.strip()

    # --- UPDATED PARAMETERS ---
    # TYPE-ACFT: 4 (Single Engine), 5 (Multi Engine), 6 (Rotorcraft/Helicopter)
    # AC-WEIGHT: CLASS 1 (Up to 12,499 lbs)
    # REG-STATUS: A (Active)
    small_ga = df[
        (df['AC-WEIGHT'].str.strip() == 'CLASS 1') & 
        (df['REG-STATUS'].str.strip() == 'A') &
        (df['TYPE-ACFT'].astype(str).str.strip().isin(['4', '5', '6']))
    ].copy()

    # Map FAA columns to Supabase columns
    final_df = pd.DataFrame()
    final_df['n_number'] = "N" + small_ga['N-NUMBER'].astype(str).str.strip()
    final_df['mfr'] = small_ga['MFR'].astype(str).str.strip()
    final_df['model'] = small_ga['MODEL'].astype(str).str.strip()
    final_df['year'] = small_ga['YEAR MFR'].astype(str).str.strip()

    records = final_df.to_dict('records')
    print(f"Found {len(records)} aircraft. Syncing batches to Supabase...")

    # Upload in small batches to avoid timeouts
    for i in range(0, len(records), 500):
        batch = records[i:i+500]
        try:
            supabase.table("FAA Small Aircraft").upsert(batch).execute()
        except Exception as e:
            print(f"⚠️ Error in batch starting at {i}: {e}")

    print("🎉 MISSION COMPLETE: Database is up to date!")

if __name__ == "__main__":
    update_registry()
