import pandas as pd
from logger import get_logger

logger = get_logger("nsw_sales_transform")

def transform_nsw_sales(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Starting transformation stage")

    initial_count = len(df)
    df = df.copy()

    try:
        # Type casting
        df["Purchase_Price"] = pd.to_numeric(df["Purchase_Price"], errors="coerce")
        df["Area"] = pd.to_numeric(df["Area"], errors="coerce")

        df["Contract_Date"] = pd.to_datetime(df["Contract_Date"], format="%Y%m%d", errors="coerce")
        df["Settlement_Date"] = pd.to_datetime(df["Settlement_Date"], format="%Y%m%d", errors="coerce")

        # Standardisation
        df["Street_Name"] = df["Street_Name"].str.title().str.strip() #Remove leading/trailing spaces
        df["Locality"] = df["Locality"].str.title().str.strip() #Remove leading/trailing spaces
        df["Postcode"] = df["Postcode"].astype(str).str.zfill(4) #Ensure 4-digit postcodes, padding with leading zeros if necessary (until its total length is 4)

        # Business rules
        df = df[df["Purchase_Price"] > 0] # Remove records with non-positive purchase prices
        df = df[df["Contract_Date"].notna()] # Remove records with invalid contract dates

        # Deduplication
        df = df.drop_duplicates(
            subset=["Property_Id", "Contract_Date", "Purchase_Price"]
        )

    except Exception:
        logger.exception("Transformation failed")
        raise

    final_count = len(df)
    logger.info(
        f"Transformation completed. Records before={initial_count}, after={final_count}"
    )

    return df
