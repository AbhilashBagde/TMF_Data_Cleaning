# TMF_Data_Cleaning

```markdown
# TMF Health Quality Institute: Rural Emergency Hospital (REH) ETL Pipeline
**UT Austin MSBA Capstone Project**

## Overview
This repository contains an automated, Agentic AI data engineering pipeline designed to synthesize historical Medicare Cost Reports and hospital financial projections. 

The pipeline uses the Gemini API to dynamically generate, execute, and document Python data transformations. It produces an auditor-grade, unified dataset focused on Critical Access Hospitals (CAHs) across the five-state TMF QIN-QIO service area (AR, LA, NM, OK, TX), preparing the data for our final Exploratory Data Analysis (EDA) and predictive modeling phases.

## 🏗️ Architecture: The Agentic Workflow
The pipeline runs a multi-agent workflow to ensure data integrity:
1. **The Profiler:** Scans the local `./data` directory, reading schemas and sampling data from our source CSVs.
2. **The Data Engineer:** Dynamically writes and executes the pandas ETL script to filter, deduplicate, and merge the data while enforcing strict composite keys (`[CCN] + [data_year]`).
3. **The Data Architect:** Reads the successful execution code and auto-generates a Markdown-based Data Lineage Report explaining the exact transformations performed.

## 🚀 Local Setup & Installation

To run this pipeline locally, you must set up your environment and provide your own API key.

### 1. Clone the Repository
```bash
git clone <your-github-repo-url>
cd <your-repo-folder>

```

### 2. Install Dependencies

It is recommended to use a virtual environment. Install the required Python packages:

```bash
pip install pandas numpy google-genai python-dotenv openpyxl

```

### 3. Create the `.env` File (⚠️ REQUIRED)

**Do not commit your API keys to GitHub.** ***You must create a local environment variable file for the script to authenticate with the Gemini API.***

1. In the root directory of the project, create a new file named exactly `.env`
2. Open the `.env` file and add the following line, replacing the placeholder with your actual Google Gemini API key:

```env
GEMINI_API_KEY=your_actual_api_key_here

```

### 4. Ensure Data is in Place

Ensure all raw source files (historical financials, trends, solvency analysis, etc.) are located in the `./data` directory relative to the script.

## 🛠️ Usage

Execute the main pipeline script:

```bash
python main.py

```

*(Note: If the agent encounters a data ingestion error, it is programmed to self-correct and retry up to 3 times.)*

## 📂 Outputs

Upon successful execution, the script generates two critical artifacts in the root directory:

1. `tmf_eda_central_data.csv`: The Single Source of Truth dataset for our EDA.
* *Key Features:* Enforces a strict 1:1 row mapping with the historical baseline, drops 100% missing columns, and fixes the "Arkansas Zero" string identifier issue.


2. `data_cleaning_and_lineage_report.md`: An auditor-grade technical document detailing the Entity Relationship (ER) mappings, join topography, and data cleansing steps applied during the run.

## 🛡️ Data Integrity Rules Enforced

* **Geographic Filtering:** Strictly isolated to AR, LA, NM, OK, and TX.
* **Identifier Standardization:** Applies `zfill(6)` to Provider CCNs to prevent the dropping of leading zeros (crucial for Arkansas facilities).
* **Pre-Join Deduplication:** Aggressively deduplicates secondary tables prior to merging to prevent Cartesian row inflation, protecting our financial averages.

```

***

Let me know if you would like me to adjust any specific sections of this documentation before you push the final commit to your team's repository!

```
