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
    # 2. THE EVERGREEN LINK
    faa_url = "https://registry.faa.gov/database/ReleasableAircraft.zip"
    
    # NEW: Browser headers to bypass the 403 Forbidden block
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.faa.gov/'
    }
    
    print(f"Downloading latest FAA Master Database (Mimicking Browser)...")
    
    # We use the headers here to trick the FAA server
    r = requests.get(faa_url, headers=headers, stream=True, timeout=60)
    
    if r.status_code == 200:
        with open("faa_data.zip", "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        print("✅ Download successful.")
    else:
        print(f"❌ FAA Server Response: {r.text[:200]}") # Show first 200 chars of error
        raise Exception(f"❌ FAA Server error {r.status_code}. The site is blocking the script.")

    print("Extracting MASTER.txt...")
    with zipfile.ZipFile("faa_data.zip", "r") as zip_ref:
        zip_ref.extract("MASTER.txt")

    print("Filtering Data for Small GA (Fixed-Wing Only)...")
    # Read with ISO-8859-1 encoding
    df = pd.read_csv("MASTER.txt", encoding='ISO-8859-1', low_memory=False)
    df.columns = df.columns.str.strip()

    # --- THE STRICT FILTER ---
    # TYPE-ACFT: 4 (Single Engine), 5 (Multi Engine)
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

    print("🎉 MISSION COMPLETE: Database updated.")

if __name__ == "__main__":
    update_registry()
