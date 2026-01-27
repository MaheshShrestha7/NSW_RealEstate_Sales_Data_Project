import requests
import zipfile
import io
import pandas as pd
from pathlib import Path

# Bulk NSW Property Sales Data : https://valuation.property.nsw.gov.au/embed/propertySalesInformation
# File Structure: File will be in Delimited flat ASCII.
    #Record type ‘A’: is a header record and will be the first record in the file. It is to include the file type, district code,ndate and time of lodgement.
    #Record type ‘B’: will contain property address and sales information.
    #Record type ‘C’: will contain Property description details.
    #Record type ‘D’: Owner details suppressed.
    #Record type ‘Z’: will be a trailer record and is to be the last record in the file. It is to include a property count and a total record count.
    #Records ‘A’ and ‘Z’ to be included in the total record count.

# 


def download_and_process_nsw_sales(year=2025,output_path="NSW_Real_Estate_Sales/output/nsw_sales_merged.csv"):
    

    # 1. URL to download yearly ZIP file
    url = f"https://www.valuergeneral.nsw.gov.au/__psi/yearly/{year}.zip" # --< Static Endpoint File URL for NSW Sales Data 
    
    # Headers to mimic a browser
    request_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    # Columns matched to 'B' record structure --> Record type ‘B’: will contain property address and sales information.
    headers = [
        "Record_Type",       # B
        "District_Code",     # 001
        "Property_Id",       # 11883
        "Sale_Counter",      # 1
        "Download_Date",     # 20250106 01:05
        "Property_Name",     # (Empty in your sample)
        "Unit_No",           # (Empty)
        "House_No",          # 178
        "Street_Name",       # HOPETOUN ST
        "Locality",          # KURRI KURRI
        "Postcode",          # 2327
        "Area",              # 505.9
        "Area_Type",         # M
        "Contract_Date",     # 20241116
        "Settlement_Date",   # 20241230
        "Purchase_Price",    # 620000
        "Zoning",            # R3
        "Nature_of_Property",# R
        "Primary_Purpose",   # RESIDENCE
        "Strata_Lot_No",     # (Empty)
        "Component_Code",    # MAD
        "Sale_Code",         # (Empty)
        "Interest_Share",    # 0
        "Dealing_No"         # AU723713
    ]

    print(f"1. Downloading data from {url}...")
    try:
        response = requests.get(url, headers=request_headers, stream=True)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"CRITICAL ERROR: Download failed. {e}")
        return

    all_sales_rows = []
    
    print("2. extracting and parsing nested ZIP files...")
    
    with zipfile.ZipFile(io.BytesIO(response.content)) as outer_zip:
        # Find all weekly zip files inside (20250106.zip, etc.)
        inner_zips = [f for f in outer_zip.namelist() if f.lower().endswith('.zip')]
        print(f"   Found {len(inner_zips)} weekly files.")

        for inner_zip_name in inner_zips:
            with outer_zip.open(inner_zip_name) as zf:
                inner_bytes = io.BytesIO(zf.read())
                with zipfile.ZipFile(inner_bytes) as inner_zip:
                    
                    # Find all .dat files inside the weekly zip
                    dat_files = [f for f in inner_zip.namelist() if f.lower().endswith('.dat')]
                    
                    for dat_file in dat_files:
                        with inner_zip.open(dat_file) as f:
                            # Use 'cp1252' decoding for NSW government files
                            content = f.read().decode('cp1252', errors='replace')
                            
                            for line in content.splitlines():
                                parts = line.split(';')
                                
                                # --- FILTER FOR 'B' ---
                                if parts and parts[0] == 'B':
                                    
                                    # Ensure row length matches headers
                                    clean_parts = parts[:len(headers)]
                                    if len(clean_parts) < len(headers):
                                        clean_parts += [''] * (len(headers) - len(clean_parts))
                                        
                                    all_sales_rows.append(clean_parts)

    print(f"3. Merging {len(all_sales_rows)} rows into DataFrame...")
    
    if not all_sales_rows:
        print("Warning: No sales records (Type B) found.")
        return

    df = pd.DataFrame(all_sales_rows, columns=headers)

    # --- CLEANING ---
    # Convert Price to numeric
    df['Purchase_Price'] = pd.to_numeric(df['Purchase_Price'], errors='coerce')
    
    # Convert Dates (Format YYYYMMDD to datetime)
    df['Contract_Date'] = pd.to_datetime(df['Contract_Date'], format='%Y%m%d', errors='coerce')
    df['Settlement_Date'] = pd.to_datetime(df['Settlement_Date'], format='%Y%m%d', errors='coerce')
    
    # Sort by Contract Date
    df = df.sort_values(by='Contract_Date')

    # Save
    print(f"4. Saving to {output_path}...")
    df.to_csv(output_path, index=False)
    print("Done! You can now open the CSV file.")




if __name__ == "__main__":

    # parameters
    year = 2025
    target_dir = "NSW_Real_Estate_Sales/output"
    
    output_csv = f"nsw_sales_{year}_merged.csv"
    
    output_path = Path(target_dir) / output_csv


    download_and_process_nsw_sales(year,output_path)