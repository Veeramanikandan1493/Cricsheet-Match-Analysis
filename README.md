# Cricsheet Match Data Analysis

## Overview

This project automates the extraction, processing, and analysis of cricket match data available on Cricsheet. The main
objectives include:

- **Data Scraping:** Automatically download and extract JSON match files using Selenium.
- **Data Transformation:** Parse and normalize JSON data into structured CSV files while handling nested structures and
  imputing missing fields.
- **Database Management:** Load the processed data into a MySQL database using batch inserts (with pymysql) for high
  performance.
- **Exploratory Data Analysis (EDA):** Generate 20 diverse visualizations using SQL queries, Matplotlib, Seaborn, and
  Plotly to extract insights on player performance, team trends, match outcomes, and more.
- **Power BI Dashboard:** Build an interactive dashboard that mirrors the EDA findings and supports strategic
  decision-making.

## Project Structure

```
CricsheetMatchDataAnalysis/
│
├── data/
│   ├── downloads/         # Raw downloaded JSON/ZIP files
│   ├── extracted/         # Unzipped JSON files
│   ├── processed/         # CSV files from data transformation
│   └── visualizations/    # Visual outputs from the EDA module
│
├── database_management/
│   └── db_manager.py      # MySQL database management with batch inserts using pymysql
│
├── data_scraping/
│   ├── scraper.py         # Selenium-based scraper to download and extract match files
│   └── config.toml        # Configuration for download URLs and file names
│
├── data_transformation/
│   └── transformer.py     # Module to transform JSON into CSVs, impute missing values, and extract wicket info
│
├── eda/
│   └── eda.py             # EDA module that generates 20 visualizations from SQL queries
│
├── powerbi/
│   └── [Power BI files]   # Power BI dashboard files connecting to MySQL for interactive reporting
│
├── README.md              # This file
└── requirements.txt       # Project dependencies
```

## Installation

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/Veeramanikandan1493/Cricsheet-Match-Analysis.git
   cd CricsheetMatchDataAnalysis
   ```

2. **Create a Virtual Environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scriptsctivate
   ```

3. **Install Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Module A: Data Scraping

- **Scrape and Extract Files:**
  ```bash
  python data_scraping/scraper.py
  ```
  This script navigates to Cricsheet, downloads the JSON/ZIP files, and extracts the specified files into
  `data/extracted`.

### Module B: Data Transformation

- **Transform JSON to CSV:**
  ```bash
  python data_transformation/transformer.py
  ```
  This module processes the extracted JSON files (including extracting wicket info from deliveries) and produces CSV
  files in `data/processed`.

### Module C: Database Management

- **Insert Processed Data into MySQL:**
  ```bash
  python database_management/db_manager.py
  ```
  Update the connection string in `db_manager.py` with your MySQL credentials. This module uses batch inserts to load
  large volumes of data efficiently.

### Module D: Exploratory Data Analysis (EDA)

- **Generate Visualizations:**
  ```bash
  python eda/eda.py
  ```
  The EDA module runs 20 SQL queries against the MySQL database and creates corresponding visualizations saved in
  `data/visualizations`.

### Power BI Dashboard

- **Connect Power BI to MySQL:**  
  Use the MySQL connector in Power BI to connect to your database and build interactive dashboards mirroring the 20
  visualizations.

## Business Use Cases & Insights

- **Player Performance Analysis:** Identify top batsmen, bowlers, and performance trends across formats.
- **Team Insights:** Compare team performance, win percentages, and match outcomes over time.
- **Match Outcomes:** Analyze win/loss patterns, margins of victory, and trends.
- **Strategic Decision-Making:** Provide data-driven insights to coaches, management, and analysts.
