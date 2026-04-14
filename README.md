# Pharma Analytics Portfolio

Collection of commercial analytics projects built for the pharmaceutical industry, combining **Databricks (Spark SQL / Delta Lake)**, **Power BI**, and advanced DAX modeling.

---

## Projects

### [Pharma Sales Intelligence](./pharma-sales-intelligence/)
End-to-end commercial analytics platform serving hundreds of users across 4 business units.
Built with Databricks + Power BI, covering sell-out, sell-in, demand planning, quotas, and indirect sales channels.

**Highlights:** Incremental Refresh via `Value.NativeQuery`, TREATAS for many-to-many resolution, Gold layer architecture, R3M demand measures.

---

### [Performance Comercial 360°](./cases/performance-comercial-360/)
Selection case built for a pharmaceutical company, simulating B2B sell-in analytics between distributors and pharmacies.

**Highlights:** Gross → Net Revenue Waterfall, Volume × Price Elasticity, narrative analytics via DAX, dynamic ranking with TOPN+SUMX.

---

## Stack

| Layer | Technology |
|---|---|
| Data Warehouse | Databricks / Delta Lake |
| Query Language | Spark SQL |
| BI Tool | Power BI (Import Mode) |
| DAX | Advanced measures & modeling |
| Data Modeling | Star Schema |

---

## Note on Data

All queries and measures use anonymized or synthetic data only. No real customer, patient, or proprietary commercial data is included.
