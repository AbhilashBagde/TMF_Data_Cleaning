# Data Lineage and Architecture Documentation: REH Conversion Modeling
**Project:** Master’s Capstone – TMF Health Quality Institute  
**Prepared By:** Senior Data Analytics Consultant  
**Date:** May 22, 2024  
**Subject:** ETL Pipeline and Data Architecture for Rural Emergency Hospital (REH) Viability Analysis

---

## 1. Executive Summary
The purpose of this document is to detail the data lineage, transformation logic, and architectural design of the unified dataset `tmf_eda_central_data.csv`. This file serves as the **Single Source of Truth (SSoT)** for all Exploratory Data Analysis (EDA) and subsequent predictive modeling regarding Critical Access Hospital (CAH) performance and potential Rural Emergency Hospital (REH) conversions. By synthesizing disparate financial, solvency, and longitudinal trend data into a singular tabular structure, this pipeline enables TMF Health Quality Institute to assess hospital viability with high statistical confidence and regional specificity.

## 2. Source Data Inventory
The pipeline ingests five distinct data assets to construct the master analytical record:

*   **Historical Financials (`cah_all_years_2017_2023.csv`)**: Multi-year CMS cost report data containing the primary financial features.
*   **Hospital Trends (`cah_hospital_trends.csv`)**: Derived longitudinal metrics and performance shifts over time.
*   **Solvency Analysis (`reh_solvency_analysis.csv`)**: Risk-based metrics evaluating the cash-flow health and closure risk of rural facilities.
*   **Future Projections (`reh_projections_all_scenarios.csv`)**: Modeled outcomes for hospitals based on various REH conversion reimbursement scenarios.
*   **Macro-Statistics (`yearly_summary_stats.csv`)**: State and national level benchmarks used for contextual normalization.

## 3. Data Scoping & Filtering

### 3.1 Geographic Restriction
To align with TMF’s mandate as a Quality Innovation Network-Quality Improvement Organization (QIN-QIO), the dataset is programmatically restricted to the following five states: **Arkansas (AR), Louisiana (LA), New Mexico (NM), Oklahoma (OK), and Texas (TX)**. This scoping ensures that the analysis remains actionable for the client’s specific service area and removes extraneous data that could skew regional variance.

### 3.2 Provider Type Isolation (CAH Designation)
The Rural Emergency Hospital (REH) designation is primarily relevant to currently operating Critical Access Hospitals (CAHs). The pipeline isolates these facilities by interrogating the **CMS Certification Number (CCN)**. Per CMS regulatory standards, CAHs are identified by a '13' in the third and fourth digit positions of the CCN. The script applies a string-slice filter `[2:4] == '13'` to ensure the modeling population is restricted strictly to eligible rural facilities.

## 4. Data Cleaning & Identifier Standardization

### 4.1 The "Arkansas Zero" Fix
A critical failure point in CMS data processing is the truncation of leading zeros in numerical identifiers. Arkansas (State Code '04') is particularly vulnerable; if treated as an integer, '040001' becomes '40001', breaking the CAH filter and join logic.
*   **Transformation Logic**: The pipeline implements a `standardize_ccn` function that casts identifiers to strings, removes floating-point decimals, and applies a `zfill(6)` operation.
*   **Impact**: This ensures 100% referential integrity across all data tables and prevents the systematic exclusion of Arkansas-based facilities.

### 4.2 Dimensionality Reduction (Null Handling)
To optimize memory usage and improve model performance, the pipeline includes an automated cleaning step that drops any columns where 100% of the observations are null. This reduces noise without sacrificing variables that contain actual statistical value.

## 5. Data Joining Strategy & Entity Relationship Mapping

### 5.1 Base Grain & Composite Key Definition
The central fact table is `cah_all_years`. The fundamental **Grain** of this dataset is defined as **One row per Hospital per Fiscal Year**. 
*   **Composite Primary Key**: `[Provider CCN] + [data_year]`.

### 5.2 Entity Relationship (ER) Topography
The architecture follows a star-schema-inspired join strategy using **Left Joins** to preserve the integrity of the historical observations in the primary fact table.

| Source Table | Join Key | Relationship | Description |
| :--- | :--- | :--- | :--- |
| `cah_hospital_trends` | `Provider CCN` | Many-to-One | Attaches historical trend lines to each yearly observation. |
| `reh_solvency_analysis` | `Provider CCN` | Many-to-One | Maps risk scores to specific facilities. |
| `reh_projections` | `Provider CCN` | Many-to-One | Provides conversion scenarios for each hospital. |
| `yearly_summary_stats` | `data_year` | Many-to-One | Contextualizes yearly performance against national averages. |

### 5.3 Pre-Join Deduplication Logic
To prevent **Cartesian Explosion** (row inflation), the pipeline performs aggressive structural deduplication on all secondary (dimension) tables prior to the merge. 
*   **Procedure**: Secondary tables are reduced to unique `Provider CCN` entries using `drop_duplicates`.
*   **Rationale**: Since the secondary tables provide hospital-level attributes rather than year-specific records, merging them without deduplication would cause the historical rows to multiply for every instance found in the secondary file. Pre-joining deduplication protects the composite grain and ensures the final row count remains consistent with the historical source.

## 6. Quality Assurance Summary
The ETL pipeline concludes by enforcing a strict 1:1 row mapping relative to the filtered historical source data. By standardizing the CCN via the "Arkansas Zero" fix and deduplicating secondary tables, the resulting `tmf_eda_central_data.csv` is a high-integrity, analytically-ready dataset. It is free of pipeline-induced row inflation, characterized by standardized identifiers, and optimized for the specific geographic and regulatory requirements of the TMF Health Quality Institute's REH conversion analysis.