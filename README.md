# NREL_ENERGY_DATA_PROCESSING

# 🌞 Solar and Wind Data Processing & Clustering

This repository contains scripts and workflows for **clustering power plant locations**, **retrieving meteorological data**, **cleaning data**, and **joining clusters with existing datasets**.

## 🛠️ Workflow Overview

1️⃣ **Cluster power plant locations** ensuring no cluster has plants from the same county.  
2️⃣ **Request meteorological data** (wind speed, solar radiation) from the NREL API.  
3️⃣ **Clean and analyze clusters** to filter out anomalies and aggregate extreme conditions.  
4️⃣ **Join clusters** with existing datasets to match meteorological conditions to plant locations.  

## 📂 Project Structure

/data ├── clustered_files/ # Clustered datasets (plants_clusters.csv) ├── solar_long_clean_clusters/ # Processed solar data by cluster ├── wind_long_clean_cluster/ # Processed wind data by cluster ├── Joined_data_plants_nrel_solar/ # Solar cluster matching results ├── Joined_data_plants_nrel/ # Wind cluster matching results ├── no_of_hours_clusters/ # Aggregated duration of high/low wind & solar /scripts ├── cluster_analysis.py # Python script for clustering power plants ├── request_solar_wind_data.R # R script to request data from NREL API ├── clean_clusters.R # R script for processing raw cluster data ├── join_clusters.R # R script for merging clusters with existing datasets

## ⚙️ Setup Instructions

### 1️⃣ Install Required Packages

#### **For Python**  
Install required Python packages:

```sh
pip install -r requirements.txt
```
#### **For R**
install.packages(c("tidyverse", "here", "janitor", "httr", "purrr", "readxl", "lubridate"))

🔄 Detailed Workflow
Step 1: Cluster Power Plant Locations (Python)
Ensures that no cluster contains power plants from the same county.
Limits clusters to a maximum of 250 locations.

Step 2: Request Solar & Wind Data (R)
Requests wind speed (100m) and solar radiation (GHI) data from the NREL API.
Uses the clustered locations from Step 1.


Step 3: Clean Cluster Data (R)
Filters out extreme values (e.g., GHI < 200 or > 1000).
Aggregates high/low wind & solar hours for each cluster.


Step 4: Join Cleaned Clusters with Plant Data (R)
Matches processed solar and wind clusters to power plants.
Finds the closest data points for each plant.


📊 Results & Usage
The final outputs can be used for:
Analyzing meteorological conditions at power plant sites.
Identifying extreme weather conditions impacting power generation.
Comparing clusters across locations to study solar and wind variability.

🛰️ Data Sources
NREL Wind Toolkit API (https://developer.nrel.gov/)
EPA eGRID Database (https://www.epa.gov/egrid)

📌 Notes
The clustering script ensures plants from the same county do not end up in the same cluster.
Ensure API keys are correctly configured before running data requests.
The cleaning scripts remove extreme values for accurate analysis.

📞 Contact
For queries, contact: tarun.kumanduri99@gmail.com





