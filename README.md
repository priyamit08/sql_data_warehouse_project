# Data Warehouse & Analytical Project
This project demonstrates a comprehensive data warehousing and analytics solution, from building a data warehouse to generating actionable insights. Designed as a portfolio project, it highlights industry best practices in data engineering and analytics.

# 🏗️ Data Warehouse and Analytics Project

This project demonstrates a **comprehensive data warehousing and analytics solution**, from building a data warehouse to generating actionable insights. Designed as a **portfolio project**, it follows **industry best practices** in data engineering and analytics.

---

## 📐 Data Architecture

The project implements the **Medallion Architecture** using the **Bronze, Silver, and Gold** layers:

- **🔸 Bronze Layer**  
  Stores raw data as-is from the source systems. Data is ingested from **CSV files** into a **SQL Server database**.

- **🔹 Silver Layer**  
  Data cleansing, standardization, and normalization to prepare data for analysis.

- **⭐ Gold Layer**  
  Business-ready data modeled into a **star schema** for reporting and analytics.

---

## ⚙️ Project Components

This project includes:

- **🧱 Data Architecture**  
  Designing a modern data warehouse using the Medallion architecture.

- **🔄 ETL Pipelines**  
  Extract, Transform, Load processes to populate the data warehouse.

- **📊 Data Modeling**  
  Creation of **fact** and **dimension** tables optimized for analytical queries.

- **📈 Analytics & Reporting**  
  SQL-based reports and dashboards delivering actionable insights.


## 🛠️ Tools & Resources

Everything used in this project is **free**:

| Tool / Resource           | Description                                                      |
|---------------------------|------------------------------------------------------------------|
| **Datasets**              | ERP & CRM data in CSV format                                     |
| **SQL Server Express**    | Lightweight SQL server hosting the data warehouse                |
| **SQL Server Management Studio (SSMS)** | GUI to manage and query SQL Server databases         |
| **GitHub Repository**     | Version control and collaboration                                |
| **Draw.io**               | Visual diagrams of architecture, ETL, data flow, and models      |
| **Notion**                | Project template with all phases and tasks                       |

---

## 🚀 Project Requirements

### 🔧 Data Engineering: Building the Data Warehouse

**Objective**  
Develop a modern data warehouse in SQL Server to consolidate sales data for analytical reporting.

**Specifications**

- **Data Sources**: Import from ERP and CRM systems via CSV files  
- **Data Quality**: Cleanse and resolve issues prior to analysis  
- **Integration**: Unified data model for analytical queries  
- **Scope**: Focus on latest data (no historization)  
- **Documentation**: Clear data model documentation for stakeholders

---

### 📊 Data Analysis: BI & Reporting

**Objective**  
Create SQL-based analytics to deliver insights into:

- 🧍 Customer Behavior  
- 📦 Product Performance  
- 💰 Sales Trends  

These insights provide stakeholders with key business metrics to support **strategic decision-making**.

> 📄 For more details, refer to: `docs/requirements.md`

---

## 📁 Repository Structure

data-warehouse-project/
│
├── datasets/                           # Raw datasets used for the project (ERP and CRM data)
│
├── docs/                               # Project documentation and architecture details
│   ├── etl.drawio                      # Draw.io file shows all different techniquies and methods of ETL
│   ├── data_architecture.drawio        # Draw.io file shows the project's architecture
│   ├── data_catalog.md                 # Catalog of datasets, including field descriptions and metadata
│   ├── data_flow.drawio                # Draw.io file for the data flow diagram
│   ├── data_models.drawio              # Draw.io file for data models (star schema)
│   ├── naming-conventions.md           # Consistent naming guidelines for tables, columns, and files
│
├── scripts/                            # SQL scripts for ETL and transformations
│   ├── bronze/                         # Scripts for extracting and loading raw data
│   ├── silver/                         # Scripts for cleaning and transforming data
│   ├── gold/                           # Scripts for creating analytical models
│
├── tests/                              # Test scripts and quality files
│
├── README.md                           # Project overview and instructions
├── LICENSE                             # License information for the repository
├── .gitignore                          # Files and directories to be ignored by Git
└── requirements.txt                    # Dependencies and requirements for the project
