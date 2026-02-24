import os
import pandas as pd
from google import genai
from dotenv import load_dotenv
from typing import Dict, Any

# --- CONFIGURATION ---
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
DATA_DIR = "./data"
OUTPUT_EDA_FILE = "tmf_eda_central_data.csv"
OUTPUT_DOC_FILE = "data_cleaning_and_lineage_report.md"

client = genai.Client(api_key=GEMINI_API_KEY)
model_name = "gemini-3-flash-preview"

SUPPORTED_EXTENSIONS = [".csv", ".xls", ".xlsx"]

def get_signatures(directory: str) -> Dict[str, Any]:
    """Scan folder and read metadata from supported files."""
    print("🔍 Profiling local files...")
    signatures = {}
    for filename in os.listdir(directory):
        ext = os.path.splitext(filename)[1].lower()
        if ext in SUPPORTED_EXTENSIONS:
            path = os.path.join(directory, filename)
            try:
                if ext == ".csv":
                    df = pd.read_csv(path, nrows=3)
                else:
                    df = pd.read_excel(path, nrows=3)
                signatures[filename] = {
                    "columns": df.columns.tolist(),
                    "sample": df.astype(str).head(2).to_dict(orient='records')
                }
            except Exception as e:
                print(f"⚠️ Error reading {filename}: {e}")
    return signatures

def generate_etl_code(signatures: Dict[str, Any]) -> str:
    """Generate ETL script for EDA using Gemini API."""
    print("🤖 AI is designing the Data Engineering pipeline...")
    
    # ADVANCED PROMPT: Enforcing strict Data Quality rules discovered during the audit.
    prompt = f"""
    You are a Senior Data Engineer working on an MSBA Capstone project for TMF Health Quality Institute.
    You have these files in the directory '{DATA_DIR}': {signatures}
    
    TASK: Write a Python pandas script to generate a clean, unified central dataset for Exploratory Data Analysis (EDA).
    
    STRICT BUSINESS RULES & TRANSFORMATIONS:
    1. Load all relevant historical hospital cost report data and any summary/trend data. Do NOT restrict the data to 2017-2023; process all available years dynamically.
    2. Geographic Filter: Keep ONLY hospitals in the 5 client states: AR, LA, NM, OK, and TX.
    3. The "Arkansas Zero" Fix (Identifier Cleaning): Standardize the 'CCN' (or Provider CCN) column across ALL loaded dataframes. You MUST cast the CCN as a string and pad it with leading zeros to ensure it is exactly 6 digits long (e.g., `df['CCN'] = df['CCN'].astype(str).str.zfill(6)`).
    4. Provider Type Filter: After padding the CCN, isolate Critical Access Hospitals (CAHs) by ensuring the 3rd and 4th digits of the 6-digit CCN are '13'.
    5. Structural Deduplication (Crucial): BEFORE performing any joins, you must drop duplicate rows in the secondary tables (like trends or solvency files) based on the CCN column (e.g., `df.drop_duplicates(subset=['CCN'])`). This prevents Cartesian explosion during the merge.
    6. Merge Data: Perform left joins to combine the base historical data with the deduplicated secondary tables using the standardized 6-digit CCN and/or Year as keys.
    7. Clean Empty Data: Explicitly drop columns that are 100% missing across the dataset (e.g., drop 'medicare_ip_pct' and 'DRG Amounts Other Than Outlier Payments' if they are totally null). 
    8. Save the final, unified pandas dataframe to '{OUTPUT_EDA_FILE}' as a CSV. THIS STEP IS MANDATORY.
    
    Return ONLY the valid Python code block starting with `import pandas as pd`. Do not include markdown explanations. Ensure 'numpy' is imported. Ensure the file is saved as '{OUTPUT_EDA_FILE}'.
    """
    response = client.models.generate_content(model=model_name, contents=prompt)
    return (response.text.replace("```python", "").replace("```", "").strip()
            if response and hasattr(response, 'text') and response.text else "")

def generate_documentation(successful_code: str) -> None:
    """Generate Markdown documentation for the ETL process."""
    print("📝 AI is generating the Data Cleaning and Lineage Documentation...")
    prompt = f"""
    You are a Senior Data Analytics Consultant authoring a formal Data Lineage and Architecture document for a Master's Capstone project with TMF Health Quality Institute. 
    
    Review the following Python ETL script which successfully combined data to model Rural Emergency Hospital (REH) conversions.
    
    PYTHON SCRIPT:
    {successful_code}
    
    TASK: Write a comprehensive, highly detailed technical document in Markdown format explaining the end-to-end data engineering flow. 
    
    REQUIRED SECTIONS & CONTENT TO INCLUDE:
    
    1. **Executive Summary**: Provide a brief overview of the final dataset (`tmf_eda_central_data.csv`). Explain that it acts as the Single Source of Truth for Exploratory Data Analysis regarding CAH performance and REH viability.
    
    2. **Source Data Inventory**: Briefly list the types of files brought into the pipeline (Historical financials, Trends, Macro-Stats, Solvency Analysis, Projections).
    
    3. **Data Scoping & Filtering**: 
       - *Geographic*: Detail the restriction to AR, LA, NM, OK, and TX. Explain the business context (TMF's QIN-QIO service area).
       - *Provider Type*: Explain how Critical Access Hospitals (CAHs) were isolated using the CMS Certification Number (CCN), specifically looking for '13' in the 3rd and 4th digit positions.
       
    4. **Data Cleaning & Identifier Standardization**: 
       - *The "Arkansas Zero" Fix*: Detail the critical step of explicitly casting the CCN column to a string and padding it with leading zeros (`zfill(6)`). Explain WHY this was necessary (preventing pandas from truncating AR state codes starting with '04', which breaks the CAH filter).
       - *Null Handling*: Mention the programmatic dropping of columns that were 100% missing across the dataset to reduce dimensionality without losing statistical value.
       
    5. **Data Joining Strategy & Entity Relationship Mapping**: 
       - *Base Grain & Composite Key Definition*: State that `cah_all_years` acts as the central Fact Table. Define its unique grain as one row per Hospital per Year, making its Composite Primary Key `[CCN] + [data_year]`.
       - *Entity Relationship (ER) Merge Topography*: Explain that all joins were performed as Left Joins onto the Base Table to prevent loss of historical observations. 
       - *Join Keys & Relationships*: Explicitly detail how Dimension Tables were mapped (e.g., `cah_hospital_trends` and `reh_solvency_analysis` joined via Many-to-One on `CCN`; `yearly_summary_stats` joined via Many-to-One on `data_year`).
       - *Pre-Join Deduplication*: Explicitly document that secondary tables were aggressively deduplicated using their respective keys *before* the join. Explain WHY (to protect the composite grain and prevent Cartesian explosion / row inflation).
       
    6. **Quality Assurance Summary**: Conclude by noting that the pipeline enforces a strict 1:1 row mapping with the historical source data, successfully yielding a highly reliable dataset free of pipeline-induced row inflation.
    
    Write this in a formal, authoritative tone suitable for submission to corporate clients and an academic grading committee.
    """
    response = client.models.generate_content(model=model_name, contents=prompt)
    if response and hasattr(response, 'text') and response.text:
        with open(OUTPUT_DOC_FILE, "w", encoding="utf-8") as f:
            f.write(response.text)
        print(f"📄 Documentation saved to '{OUTPUT_DOC_FILE}'.")
    else:
        print("Failed to generate documentation.")

def run_agentic_workflow() -> None:
    sigs = get_signatures(DATA_DIR)
    python_code = generate_etl_code(sigs)
    attempts = 0
    success = False
    namespace = {"pd": pd, "os": os}
    while attempts < 3:
        try:
            import numpy as np
            namespace["np"] = np
            print(f"Executing ETL attempt {attempts + 1}...")
            exec(python_code, namespace)
            print(f"✅ Success! Central EDA file '{OUTPUT_EDA_FILE}' generated.")
            success = True
            break
        except Exception as e:
            attempts += 1
            print(f"⚠️ Retry {attempts} fixing error: {e}")
            correction_prompt = f"The code failed with error {e}. Fix this code:\n{python_code}\n\nReturn ONLY valid python code."
            response = client.models.generate_content(model=model_name, contents=correction_prompt)
            python_code = (response.text.replace("```python", "").replace("```", "").strip()
                           if response and hasattr(response, 'text') and response.text else "")
    if success:
        generate_documentation(python_code)

if __name__ == "__main__":
    run_agentic_workflow()