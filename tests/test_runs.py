import pandas as pd
from pathlib import Path
import os

# 1. Determine the base directory dynamically
try:
    # Works in .py files
    base_path = Path(__file__).resolve().parent
except NameError:
    # Works in .ipynb notebooks (gets current working directory)
    base_path = Path(os.getcwd())

# 2. Identify the Project Root
# If your script/notebook is in the 'test' folder, we need to go up one level
if base_path.name in ["test", "tests"]:
    project_root = base_path.parent
else:
    project_root = base_path

# 3. Construct the path to your CSV
file_path = project_root / "NSW_Real_Estate_Sales" / "output" / "nsw_sales_2025_merged.csv"

# 4. Load the data
if file_path.exists():
    df = pd.read_csv(file_path)
    print(f"Successfully loaded data from: {file_path}")
else:
    print(f"Error: Path does not exist -> {file_path}")