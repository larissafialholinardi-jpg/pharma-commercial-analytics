📊 Pharma Commercial Analytics
End-to-end commercial analytics solution for a multi-business-unit pharmaceutical company, covering sell-out, sell-in, demand planning, quotas, and indirect sales channels.

Built with Databricks (Spark SQL / Delta Lake) as the data warehouse and Power BI as the BI layer, serving hundreds of users across 4 business units.

🏗️ Architecture Overview
Data Sources (Close-Up, SAP, Internal) ↓ Databricks Delta Lake (Bronze → Silver → Gold) ↓ Power BI (Import Mode + Incremental Refresh) ↓ DashPlan — Commercial Dashboard

📦 Business Units Covered
BU	Focus
BU Prescrição	Prescription drug sell-out & demand
BU Genéricos	Generic drugs commercial performance
BU Oftalmologia	Ophthalmology portfolio
BU Dermatologia	Dermatology & aesthetics
🗂️ Repository Structure
pharma-commercial-analytics/
│
├── sql/
│   ├── gold/          # Delta Lake Gold layer tables
│   └── views/         # Auxiliary views
├── dax/
│   └── measures/      # Power BI DAX measures by domain
└── docs/
    └── screenshots/   # Dashboard previews
🔑 Key Technical Highlights
Incremental Refresh configured via RangeStart/RangeEnd parameters with Value.NativeQuery against Databricks
Rolling 3-Month Demand (R3M) measures with YoY comparison
Many-to-many relationship resolution in indirect sales panel using TREATAS
Gold layer architecture with split fact tables: fato_sellout_historico_mensal, fato_sellout_diario, fato_sellout_projecao
CTE-based deduplication for PDV/CNPJ panel management across 5 commercial panels
Carry-forward patterns in DAX for historical pending orders
🛠️ Stack
Layer	Technology
Data Warehouse	Databricks / Delta Lake
Query Language	Spark SQL
BI Tool	Power BI (Import Mode)
Data Modeling	Star Schema
Version Control	GitHub
⚠️ Note on Data
All queries and measures reference anonymized or structural logic only. No real customer, patient, or proprietary commercial data is included in this repository.
