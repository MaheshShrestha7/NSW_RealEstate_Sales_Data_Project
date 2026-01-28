import requests
import zipfile
import io
import pandas as pd
from logger import get_logger
logger = get_logger("nsw_sales_extract")


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


def extract_nsw_sales(year: int) -> pd.DataFrame:
    url = f"https://www.valuergeneral.nsw.gov.au/__psi/yearly/{year}.zip" # --< Static Endpoint File URL for NSW Sales Data 

    # Headers to mimic a browser
    request_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }


    logger.info(f"Downloading data from {url} for year {year}")
    try:
        response = requests.get(url, headers=request_headers, stream=True)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.exception(f"CRITICAL ERROR: Download failed. {e}")
        return

    all_sales_rows = []
    
    logger.info("Extracting and parsing nested ZIP files...")
    
    with zipfile.ZipFile(io.BytesIO(response.content)) as outer_zip:
        # Find all weekly zip files inside (20250106.zip, etc.)
        inner_zips = [f for f in outer_zip.namelist() if f.lower().endswith('.zip')]
        logger.info(f"Found {len(inner_zips)} weekly files.")

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

    logger.info(f"Merging {len(all_sales_rows)} rows into DataFrame.")
    
    if not all_sales_rows:
        logger.warning("Warning: No sales records (Type B) found.")
        return

    df = pd.DataFrame(all_sales_rows, columns=headers)

    return df