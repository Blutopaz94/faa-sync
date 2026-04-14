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
    # 2. DYNAMIC URL LOGIC: Automatically find this month's file
    # The FAA uses the format ARMMYYYY.zip (e.g., AR042026.zip)
    now = datetime.now()
    
    # Try the current month first
    faa_filename = f"AR{now.strftime('%m%Y')}.zip"
    faa_url = f"https://registry.faa.gov/database/{faa_filename}"
    
    print(f"Checking for current month: {faa_url}")
    r = requests.get(faa_url)
    
    if r.status_code != 200:
        # If current month isn't posted yet, fallback to previous month
        print("Current month not found, trying previous month...")
        last_month = now.replace(day=1) - timedelta(days=1)
        faa_filename = f"AR{last_month.strftime('%m%Y')}.zip"
        faa_url = f"https://registry.faa.gov/database/{faa_filename}"
        r = requests.get(faa_url)

    if r.status_code != 200:
        print("❌ Could not locate a valid FAA database file.")
        return

    with open("faa_data.zip", "wb") as f:
        f.write(r.content)

    print("Extracting MASTER.txt...")
    with zipfile.ZipFile("faa_data.zip", "r") as zip_ref:
        zip_ref.extract("MASTER.txt")

    print("Filtering Data for Small GA & Helicopters...")
    # Read FAA data
    df = pd.read_csv("MASTER.txt", low_memory=False)
    df.columns = df.columns.str.strip()

    # --- UPDATED PARAMETERS ---
    # TYPE-ACFT: 4 (Single Engine), 5 (Multi Engine), 6 (Rotorcraft/Helicopter)
    # AC-WEIGHT: CLASS 1 (Up to 12,499 lbs)
    # REG-STATUS: A (Active)
    small_ga = df[
        (df['AC-WEIGHT'].str.strip() == 'CLASS 1') & 
        (df['REG-STATUS'].str.strip() == 'A') &
        (df['TYPE-ACFT'].str.strip().isin(['4', '5', '6']))
    ].copy()

    # Prepare for Supabase
    final_df = pd.DataFrame()
    final_df['n_number'] = "N" + small_ga['N-NUMBER'].str.strip()
    final_df['mfr'] = small_ga['MFR'].str.strip()
    final_df['model'] = small_ga['MODEL'].str.strip()
    final_df['year'] = small_ga['YEAR MFR'].str.strip()

    records = final_df.to_dict('records')
    print(f"Syncing {len(records)} planes to Supabase...")

    # Upload in batches to avoid timeout
    for i in range(0, len(records), 1000):
        batch = records[i:i+1000]
        try:
            supabase.table("FAA Small Aircraft").upsert(batch).execute()
        except Exception as e:
            print(f"Error in batch: {e}")

    print("✅ Sync Complete!")

if __name__ == "__main__":
    update_registry()
