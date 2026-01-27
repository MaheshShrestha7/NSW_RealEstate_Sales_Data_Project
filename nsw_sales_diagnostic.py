import requests
import zipfile
import io

def diagnose_nsw_data(year=2025):
    url = f"https://www.valuergeneral.nsw.gov.au/__psi/yearly/{year}.zip"
    headers = {"User-Agent": "Mozilla/5.0"}

    print(f"--- DIAGNOSTIC MODE: Checking {year} ---")
    print(f"1. Downloading header from {url}...")
    
    response = requests.get(url, headers=headers, stream=True)
    if response.status_code != 200:
        print(f"CRITICAL: Download failed. Status: {response.status_code}")
        return

    try:
        with zipfile.ZipFile(io.BytesIO(response.content)) as outer_zip:
            # 1. LIST FILES IN OUTER ZIP
            outer_files = outer_zip.namelist()
            print(f"\n[Outer ZIP Content Sample]: {outer_files[:5]}")
            
            # Find the first zip file
            inner_zips = [f for f in outer_files if f.lower().endswith('.zip')]
            if not inner_zips:
                print("ERROR: No .zip files found inside the master zip.")
                return
            
            target_zip = inner_zips[0]
            print(f"\n2. Peeking inside: {target_zip}")

            # 2. OPEN FIRST INNER ZIP
            with outer_zip.open(target_zip) as zf:
                inner_bytes = io.BytesIO(zf.read())
                with zipfile.ZipFile(inner_bytes) as inner_zip:
                    # LIST FILES IN INNER ZIP
                    inner_files = inner_zip.namelist()
                    print(f"[Inner ZIP Content Sample]: {inner_files[:5]}")
                    
                    dat_files = [f for f in inner_files if f.lower().endswith('.dat')]
                    if not dat_files:
                        print("ERROR: No .dat files found inside the weekly zip.")
                        return
                    
                    target_dat = dat_files[0]
                    print(f"\n3. Reading first 5 lines of: {target_dat}")
                    
                    # 3. PRINT RAW TEXT CONTENT
                    with inner_zip.open(target_dat) as f:
                        # Read first 1024 bytes to get a sample
                        sample_text = f.read(1024).decode('cp1252', errors='replace')
                        lines = sample_text.splitlines()
                        
                        print("-" * 40)
                        for i, line in enumerate(lines[:5]):
                            print(f"Line {i+1}: {repr(line)}")
                        print("-" * 40)
                        
                        # ANALYSIS
                        first_line = lines[0] if lines else ""
                        if ";" in first_line:
                            print("\nANALYSIS: Delimiter appears to be ';'")
                        elif "," in first_line:
                            print("\nANALYSIS: Delimiter appears to be ',' (Change code to comma!)")
                        
                        parts = first_line.split(';')
                        print(f"First record type identifier: '{parts[0]}'")

    except zipfile.BadZipFile:
        print("ERROR: The downloaded file is not a valid ZIP. It might be a corrupt download or an HTML error page.")

if __name__ == "__main__":
    diagnose_nsw_data(2025)