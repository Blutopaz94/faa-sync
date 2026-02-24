import pandas as pd
import zipfile
import requests
import io
import os
from supabase import create_client

# Credentials from GitHub Secrets
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)

def load_faa(filename):
    with open(filename, 'r', encoding='utf-8', errors='ignore') as f:
        first_line = f.readline()
    df = pd.read_csv(filename, low_memory=False, skiprows=(0 if ',' in first_line else 1))
    df.columns = [c.strip().replace('-', ' ') for c in df.columns]
    return df

# The "Browser Trick" to avoid being blocked
headers = {'User-Agent': 'Mozilla/5.0'}
r = requests.get("https://registry.faa.gov/database/ReleasableAircraft.zip", headers=headers)

with zipfile.ZipFile(io.BytesIO(r.content)) as z:
    z.extract('MASTER.txt')
    z.extract('ACFTREF.txt')

# Process data
ref = load_faa('ACFTREF.txt')
ref['CODE'] = ref['CODE'].astype(str).str.strip()
small = ref[(ref['TYPE ACFT'].astype(str).str.contains('4|5')) & (ref['AC WEIGHT'].astype(str).str.contains('1'))].copy()

master = load_faa('MASTER.txt')
master['MFR MDL CODE'] = master['MFR MDL CODE'].astype(str).str.strip()
data = master.merge(small[['CODE', 'MFR', 'MODEL']], left_on='MFR MDL CODE', right_on='CODE')

# Prepare list
rows = [{"n_number": "N"+str(r['N NUMBER']).strip(), "mfr": str(r['MFR']).strip(), "model": str(r['MODEL']).strip(), "year": str(r['YEAR MFR']).strip()} for _, r in data.iterrows()]

# Upsert (Update existing / Insert new)
for i in range(0, len(rows), 2000):
    supabase.table("aircraft").upsert(rows[i:i+2000]).execute()
