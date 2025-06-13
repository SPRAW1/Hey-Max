# Hey Max Analytics Engineering

## 📌 Overview
This project simulates the role of an Analytics Engineer at HeyMax, where the goal is to set up a scalable, robust end-to-end data stack to help business stakeholders self-serve key growth and engagement metrics.

## 🛠️ Tech Stack
- **DBT** for data transformation
- **BigQuery** as the data warehouse
- **Metabase** for dashboarding and data exploration

## 📂 Data Sources
- `event_stream.csv`
  
Raw event data was mimicked through CSV uploads in BigQuery, designed for batch processing. This can be automated using scheduled queries or orchestrators like Airflow or dbt Cloud.

## 🛠️ Data Modeling Workflow
1. **Raw Layer**: CSVs ingested into BigQuery as raw tables.
2. **Staging Layer** (`stg_*`): Cleaned and enriched views, adding `event_date`, `primary_action_flag`, etc.
3. **Core Layer**: Final materialized tables (`dim_users`, `fct_events`) used for downstream analytics.

## 📊 Dashboards & Metrics
All metrics are visualized in a single Metabase dashboard:

https://lofty-anchor.metabaseapp.com/public/dashboard/4508ab26-d632-405c-b797-7795fa320918 

Metabase was chosen for its ease of self-service for non-technical users. It allows business stakeholders to create "questions"—custom visualizations using a no-code/low-code SQL interface—making it simple for teams to answer their own questions without technical help.

### 📈 Growth & Retention
- **Daily/Weekly/Monthly Active Users** (DAU/WAU/MAU)
- **Growth Accounting**: New, Retained, Resurrected, Churned users
- **Churn Rate**: 1 - Retention
- **Quick Ratio**: (New + Resurrected) / Churned
- **Triangle Retention Chart**: Users bucketed by miles earned in first week

### 💰 Engagement Metrics
- **Total Miles Earned/Redeemed** by transaction category
- **Average Miles per Event** by category (earned/redeemed)

### 🌎 Behavioral Segments
- **Retention Curves** sliced by:
  - Country
  - Platform
  - UTM Source (first-touch)

## ⚖️ Design Decisions & Tradeoffs
- Focused on **daily retention** as weekly/monthly cohorts lacked enough volume to be insightful
- Applied **Tufte’s principles**: minimized clutter, used line/bar charts over pie charts, and used conditional formatting for better comparability

- Used **batch processing** for simplicity and ease of scaling
- **Streaming alternative**: For near real-time insights, tools like **Rakam** can be explored. Rakam integrates well with event pipelines, enabling rapid ingestion and real-time cohort tracking, which is especially useful for time-sensitive use cases like monitoring live marketing campaign performance.
- BigQuery chosen for **scalability**, **performance**, and **AI-readiness** (e.g., Gemini integration)
- Metabase chosen for **ease of access and self-serve capabilities**, allowing non-technical teams to rapidly explore data and build insights through simple, interactive interfaces

## ⚙️ Scalability Considerations
- **BigQuery’s serverless architecture** ensures seamless scaling with growing data volume
- Materializing transformed tables ensures efficient query performance
- Batch ingestion allows easy integration with scheduling tools for automation
- Modular dbt structure supports adding new metrics, fields, and dashboards as the business scales

## ✅ Testing & Validation
- Spot-checked cohort assignment logic (e.g., MIN(event_date))
- Verified key metrics with manual calculations
- Compared user counts across transformation stages to ensure consistency
- Used conditional formatting and filtering in Metabase for quick anomaly detection

## 🚀 How to Use This Repo
1. Clone the repo:
   ```bash
   git clone https://github.com/yourusername/heymax-analytics-engineering.git
   cd heymax-analytics-engineering
   ```

2. Create a dbt Cloud project and connect it to:
  - This GitHub repo
  - Your BigQuery account (via the Service Account JSON key)

3. Upload your source CSVs (event_stream.csv) to BigQuery manually or through a scheduled process to mimic batch ingestion.

4. In dbt Cloud:
  - Configure your environment and connection settings
     Run:
        - dbt run to build models
        - dbt test to validate models

5. Connect Metabase to your BigQuery project, then use the curated models to build and explore dashboards.


---
**Author:** Prawin  
**Project:** Hey Max Analytics Engineering
