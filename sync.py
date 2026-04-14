import os
import requests
import zipfile
import pandas as pd
from supabase import create_client

# 1. Setup Supabase
def get_supabase_client():
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not url or not key:
        raise Exception("❌ Missing Supabase Secrets! Check GitHub Settings.")
    return create_client(url, key)

def update_registry():
    # 2. THE EVERGREEN LINK (Standard Daily FAA Update)
    faa_url = "https://registry.faa.gov/database/ReleasableAircraft.zip"
    
    print(f"Downloading latest FAA Master Database...")
    r = requests.get(faa_url, stream=True)
    
    if r.status_code == 200:
        with open("faa_data.zip", "wb") as f:
            f.write(r.content)
        print("✅ Download successful.")
    else:
        raise Exception(f"❌ FAA Server error {r.status_code}. The site might be down.")

    print("Extracting MASTER.txt...")
    with zipfile.ZipFile("faa_data.zip", "r") as zip_ref:
        zip_ref.extract("MASTER.txt")

    print("Filtering Data for Small GA (Fixed-Wing Only)...")
    # Read with ISO-8859-1 encoding to handle special characters
    df = pd.read_csv("MASTER.txt", encoding='ISO-8859-1', low_memory=False)
    df.columns = df.columns.str.strip()

    # --- THE STRICT FILTER ---
    # TYPE-ACFT: 4 (Single Engine), 5 (Multi Engine) -> No helicopters (Type 6)
    # AC-WEIGHT: CLASS 1 (Under 12,500 lbs)
    # REG-STATUS: A (Active)
    filtered_df = df[
        (df['AC-WEIGHT'].str.strip() == 'CLASS 1') & 
        (df['REG-STATUS'].str.strip() == 'A') &
        (df['TYPE-ACFT'].astype(str).str.strip().isin(['4', '5']))
    ].copy()

    # Map to Supabase columns
    final_df = pd.DataFrame()
    final_df['n_number'] = "N" + filtered_df['N-NUMBER'].astype(str).str.strip()
    final_df['mfr'] = filtered_df['MFR'].astype(str).str.strip()
    final_df['model'] = filtered_df['MODEL'].astype(str).str.strip()
    final_df['year'] = filtered_df['YEAR MFR'].astype(str).str.strip()

    records = final_df.to_dict('records')
    print(f"Found {len(records)} fixed-wing aircraft matching your criteria.")

    # 3. Sync to Supabase
    supabase = get_supabase_client()
    print("Syncing to Supabase in batches of 500...")
    
    for i in range(0, len(records), 500):
        batch = records[i:i+500]
        try:
            supabase.table("FAA Small Aircraft").upsert(batch).execute()
        except Exception as e:
            print(f"⚠️ Error in batch starting at {i}: {e}")

    print("🎉 MISSION COMPLETE: Database updated with current small GA registry.")

if __name__ == "__main__":
    update_registry()
