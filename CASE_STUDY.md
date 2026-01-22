# E-commerce Analytics Platform - Case Study

## Executive Summary

Built a comprehensive analytics platform that transforms raw e-commerce data into actionable business intelligence, enabling data-driven decisions on marketing spend, customer retention, and inventory management.

**Impact:** Identified $6M in at-risk customer value, 57.8% dead stock requiring clearance, and 4x difference in channel ROAS - enabling strategic resource reallocation.

---

## Challenge

E-commerce businesses struggle with fragmented data across orders, marketing campaigns, web analytics, and inventory systems. Without integrated analytics, they face:

- Inefficient marketing spend (unable to identify which channels drive actual revenue)
- Poor customer retention (can't identify at-risk segments)
- Inventory mismanagement (overstocked items tying up capital)
- Lack of actionable insights despite having data

---

## Solution Approach

### Architecture

**Data Stack:**
- **Warehouse:** Google BigQuery (scalable cloud storage)
- **Transformation:** dbt Cloud (data modeling & testing)
- **Visualization:** Google Looker Studio (interactive dashboards)
- **Data:** The Look E-commerce dataset (~100K orders, 2.4M events, 29K products)

**Data Flow:**
1. Raw data → BigQuery tables
2. dbt staging layer → Clean, standardized data
3. dbt intermediate layer → Business logic (sessionization, attribution)
4. dbt marts layer → Analytics-ready tables
5. Looker Studio → Executive dashboards

### Key Technical Implementations

**1. Web Sessionization (30-minute windows)**
```sql
-- Used window functions to define unique sessions
SUM(is_new_session) OVER (
  PARTITION BY user_id 
  ORDER BY event_time 
  ROWS UNBOUNDED PRECEDING
) as session_number
```
- Processed 2.4M events into distinct user sessions
- Enables accurate conversion rate tracking
- Powers add-to-cart and purchase funnel analysis

**2. Marketing Attribution**
- Matched 100K+ orders to specific marketing campaigns
- Multi-channel tracking (Google, Facebook, YouTube, Email)
- Calculated campaign-level ROAS, CPA, and conversion rates

**3. RFM Customer Segmentation**
- Automated classification into 8 segments using Recency, Frequency, Monetary quintiles
- Segments: Champions, Loyal, At Risk, Lost, etc.
- Enables targeted retention strategies

**4. Cohort Retention Analysis**
- Monthly cohort tracking over 12+ months
- Retention heatmap visualization
- Identifies drop-off patterns and lifecycle trends

**5. Inventory Forecasting**
- Days-of-stock-remaining calculations
- 30-day rolling average sales velocity
- Automated reorder recommendations

---

## Results & Business Impact

### Marketing Optimization
- **Email: 6.3x ROAS** → highest performer, increase budget allocation
- **Facebook: 1.5x ROAS** → underperforming, reallocate to Email/Google
- **Potential savings:** $50K+ annually by optimizing channel mix

### Customer Retention
- **27.4% "Lost" customers** → $6M in historical value
- **Recommendation:** Win-back campaign targeting this segment
- **Expected recovery:** 10-15% = $600K-900K recovered revenue

### Inventory Management
- **57.8% dead stock** (no sales in 30 days)
- **42.2% overstocked** (avg 330 days of inventory)
- **Opportunity:** Free up $2M+ in tied-up capital through clearance

---

## Technical Highlights

### dbt Architecture
- **15+ models** organized in 3 layers (staging, intermediate, marts)
- **Data quality tests:** Uniqueness, not-null, relationships, value constraints
- **Modular design:** Reusable CTEs and consistent naming conventions
- **Documentation:** Schema.yml with descriptions for all models

### Advanced SQL
- Window functions (`LAG()`, `SUM()`, `ROW_NUMBER()`)
- Complex joins preserving data integrity
- Date math and time-series aggregations
- Cohort analysis with self-joins

### Performance Optimization
- Partitioned tables by date (cost reduction)
- Clustered tables on high-cardinality columns
- Incremental model patterns where appropriate

---

## Dashboard Overview

### Page 1: Executive Overview
- KPIs: Revenue, Ad Spend, ROAS, Gross Profit
- Revenue vs. Spend trends
- Top campaigns performance

### Page 2: Marketing Performance
- ROAS by platform
- Spend distribution
- CTR vs. CPC analysis
- Campaign-level metrics

### Page 3: Customer Analytics
- RFM segmentation distribution
- LTV by acquisition source
- Cohort retention heatmap
- Top customers table

### Page 4: Inventory Management
- Inventory status distribution
- Overstocked products alerts
- Dead stock identification
- Product profitability analysis

---

## Skills Demonstrated

**Analytics Engineering:**
- Dimensional modeling (facts & dimensions)
- ELT pipeline design
- Data quality assurance

**Technical Skills:**
- SQL (advanced: window functions, CTEs, joins)
- dbt (transformation, testing, documentation)
- BigQuery (cloud data warehouse)
- Data visualization (Looker Studio)

**Business Acumen:**
- Marketing analytics (ROAS, CAC, attribution)
- Customer analytics (LTV, retention, segmentation)
- Inventory management (forecasting, optimization)
- Translating data into actionable recommendations

---

## Project Repository

**GitHub:** github.com/YOUR_USERNAME/ecommerce-analytics-platform

Includes:
- All dbt SQL models
- Data quality tests
- Setup documentation
- Dashboard screenshots
- Lineage graph

---

## Contact

**Your Name**
Email: your.email@example.com
LinkedIn: linkedin.com/in/yourprofile
GitHub: github.com/yourusername

---

*This project demonstrates end-to-end analytics engineering capabilities from raw data ingestion through executive-ready business intelligence.*
