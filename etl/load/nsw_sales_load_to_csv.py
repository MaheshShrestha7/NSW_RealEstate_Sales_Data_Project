from pathlib import Path
import pandas as pd
from logger import get_logger

logger = get_logger("nsw_sales_load_to_csv")

def load_to_csv(df: pd.DataFrame, output_path: Path) -> None:
    logger.info(f"Starting load to CSV: {output_path}")

    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)
    except Exception:
        logger.exception("Failed to load data to CSV")
        raise

    logger.info(f"Load completed successfully. Records loaded: {len(df)}")

