from pathlib import Path
from logger import get_logger

from extract.nsw_sales_extract import extract_nsw_sales
from transform.nsw_sales_transform import transform_nsw_sales
from load.nsw_sales_load_to_csv import load_to_csv

logger = get_logger("pipeline")


def build_paths(year: int) -> tuple[Path, Path]:
    """
    Builds raw and curated output paths dynamically based on year.
    """
    raw_path = Path(f"data/raw/nsw_sales_{year}.csv")
    curated_path = Path(f"data/curated/nsw_sales_{year}.csv")
    return raw_path, curated_path


def run(year: int) -> None:
    logger.info(f"ETL pipeline for NSW Real Estate Sales Data started | year={year}")

    raw_path, curated_path = build_paths(year)

    try:

        # --------------------
        # EXTRACT
        # --------------------
        raw_df = extract_nsw_sales(year)

        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_df.to_csv(raw_path, index=False)

        logger.info(f"Raw data saved | path={raw_path} | records={len(raw_df)}")

        # --------------------
        # TRANSFORM
        # --------------------
        curated_df = transform_nsw_sales(raw_df)
        logger.info(f"Data transformed | records={len(curated_df)} ")

        # --------------------
        # LOAD
        # --------------------
        load_to_csv(curated_df, curated_path)
        logger.info(f"Curated data saved | path={curated_path} | records={len(curated_df)} ")

    except Exception:
        logger.exception("ETL pipeline for NSW Real Estate Sales Data failed")
        raise

    logger.info(
        f"ETL pipeline for NSW Real Estate Sales Data completed successfully | curated_path={curated_path}"
    )


if __name__ == "__main__":
    run(2025)
