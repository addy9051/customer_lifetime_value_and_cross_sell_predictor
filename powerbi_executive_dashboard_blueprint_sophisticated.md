# Enterprise Power BI Dashboard Architecture
**Target Audience:** C-Suite, VP of Customer Success, VP of Sales
**Goal:** Transform the raw AI predictions and historical operations data from Snowflake into an actionable strategic engine using advanced Power BI UX concepts (Drill-throughs, Bookmarks, and Guided Navigation).

---

> [!IMPORTANT]
> **Advanced Architecture Strategy: The "Analytic Journey"**
> Instead of building flat, isolated pages, this architecture uses a **guided diagnostic flow**. Executives start at the macro level (The "What"), drill into the AI diagnostics (The "Why"), output an action plan (The "Fix"), and can deep-dive into any single account dynamically.

---

## Page 1: The Landing Page (The "Menu")
*Purpose: A centralized hub using Power BI buttons to route executives directly to their relevant domains.*

### Layout Wireframe
| | | |
| :---: | :---: | :---: |
| | 🏢 **Company Logo & Title** | |
| 🗂️ **Global State:** <br> Total Active ARR <br> Total Predicted Churn | | 🚨 **Alerts Ticker:** <br> "3 Platinum Accounts hit High Risk this week" |
| 🔵 **Button:** <br> Enter Sales & Expansion (Cross-Sell) | 🔵 **Button:** <br> Enter Retention Center (Churn Risk) | 🔵 **Button:** <br> Enter Operations & Compliance |

*Strategy:* Use Power BI's `Page Navigation` action on buttons to route users cleanly instead of making them rely on the cluttered bottom tabs. Hide all other pages so they *must* use the guided interface.

---

## Page 2: The Command Center (The "What")
*Purpose: 10-second overview of global health and revenue velocity.*

### Layout Wireframe
| | | |
| :---: | :---: | :---: |
| 🎛️ **Slicer:** `Date` | 🎛️ **Slicer:** `Region` | 🎛️ **Slicer:** `Account Tier` |
| 💳 **KPI:** Total Active ARR | 🛑 **KPI:** Predicted Churn (Red) | 📈 **KPI:** Total Pipeline |
| 📉 **Line Chart** <br><br> *ARR vs Upsell Pipeline over Time* | 📉 **(Line Chart continued)** | 📉 **(Line Chart continued)** |
| 🌳 **Decomposition Tree (AI)** <br> *Root: Predicted Churn* <br> *Path: Region -> Tier -> Industry* | 🌳 **(Decomp Tree continued)** | 📋 **Top 10 High-Risk Accounts** <br> *(Right-Click enabled for Drillthrough)* |

*Strategy:* This is the starting point. If an executive sees a huge jump in Predicted Churn for the "Technology" sector on the Decomposition tree, they can right-click any account in the Top 10 matrix to trigger the hidden Deep-Dive page.

---

## Page 3: The Prescriptive Engine (The "Action")
*Purpose: Combine both ML paths (Churn & Cross-Sell) into a single operational hit-list using Bookmarks.*

### Layout Wireframe
| | | |
| :---: | :---: | :---: |
| 🎛️ **Bookmark Toggle:** <br> `View Expansion (Sales)` | 🎛️ **Bookmark Toggle:** <br> `View Retention (CS)` | 💰 **KPI:** Dynamic Total based on Toggle |
| 🎯 **Target Matrix** <br><br> *Acct Name, Recommended Product, Score* | 🎯 **(Target Matrix continued)** | 🤖 **Key Influencers (AI)** <br><br> *Explaining either Risk or Propensity based on Toggle* |
| 🎯 **(Target Matrix continued)** | 🎯 **(Target Matrix continued)** | 🤖 **(Key Influencers continued)** |

*Strategy:* Instead of having two completely separate pages for Sales and CS, use **Power BI Bookmarks** linked to toggle buttons. When "View Expansion" is clicked, it hides the Churn data and unhides the `fact_ml_cross_sell` Matrix and Pipeline metrics. This saves canvas space and creates a hyper-modern app-like feel.

---

## Page 4 (HIDDEN): Account 360 Deep-Dive (Drillthrough)
*Purpose: A hidden page that only appears when a user right-clicks an account on Page 2 or 3 and selects "Drillthrough -> Account 360".*

### Layout Wireframe
| | | |
| :---: | :---: | :---: |
| 🔙 **Back Button** | 🏢 **Dynamic Account Name Header** | 🏅 **Tier / Region Tags** |
| 💳 **Current ARR** | 🛑 **Churn Risk Score** | 🛒 **Top Recommendation** |
| 🎫 **Support Timeline** <br><br> *Bar Chart of Tickets over Time for this Account* | 🎫 **(Timeline continued)** | 💼 **Active Contracts** <br><br> *(Table of their specific active services)* |
| ✈️ **Travel Policy Health** <br> *(Gauge visual showing % out of policy)* | ✈️ **(Gauge continued)** | 💬 **Next Best Action** <br> *(Smart Narrative AI text summary)* |

*Strategy:*
1. Make this page "Hidden" in Power BI.
2. Drag `dim_accounts[account_name]` into the **Drillthrough Filters** bucket.
3. Now, whenever requested, this page acts as an isolated, microscopic view of a single account's entire Snowflake ledger across all 5 fact tables identically!
