# Power BI Executive Dashboard Blueprint
**Target Audience:** C-Suite, VP of Customer Success, VP of Sales
**Goal:** Transform the raw AI predictions and historical operations data from Snowflake into an actionable strategic engine.

---

> [!IMPORTANT]
> **Microsoft Official Best Practices Applied:**
> - **F-Pattern Design:** Executives read top-left to bottom-right. We place the most critical numbers top-left.
> - **Progressive Disclosure:** Use tooltips and drill-throughs so the main pages aren't cluttered.
> - **AI Visuals First:** Leverage native Power BI AI visuals to explain the "Why" behind the "What".

---

## Page 1: The Command Center (Executive Overview)
*Purpose: 10-second overview of global health and revenue velocity.*

### Layout Wireframe
| | | |
| :---: | :---: | :---: |
| 🎛️ **Slicer:** `Date` | 🎛️ **Slicer:** `Region` | 🎛️ **Slicer:** `Account Tier` |
| 💳 **KPI:** Total Active ARR | 🛑 **KPI:** Predicted Churn ARR (Red) | 📈 **KPI:** 30D Booking Run Rate |
| 🌳 **Decomposition Tree** <br><br> *Analyze: Predicted Churn ARR*<br>*Explain by: Industry, Tier, Region* | 📉 **Line Chart** <br><br> *X: Month* <br> *Y1: Active ARR* <br> *Y2: Probable Upsell* | 📉 **(Line Chart continued)** |
| 📋 **Top 10 Matrix (At-Risk)** <br> *Columns: Account Name, Predicted Churn Status* | 📋 **(Top 10 Matrix continued)** | 📋 **(Top 10 Matrix continued)** |

### Visual Configurations:
* **Middle Left (Decomposition Tree - Native AI Visual):**
  * *Business Strategy:* Allows executives to click and dynamically see exactly which combinations (e.g., *Technology* clients in *EMEA*) are driving the highest financial risk.
* **Middle Right (Line Chart):**
  * X-Axis: `dim_date[month_name]` | Y-Axis: `Active ARR` vs `Predicted Upsell` (using `fact_ml_cross_sell[probability_score]`).

---

## Page 2: The Retention Engine (Operations vs AI)
*Purpose: Arm the Customer Success team with tactical data to save at-risk accounts.*

### Layout Wireframe
| | | |
| :---: | :---: | :---: |
| 💳 **KPI:** Avg Support Cost Proxy | ⏱️ **KPI:** Avg Resolution Hours | ⚠️ **KPI:** # High Risk Accounts |
| 🔵 **Scatter Plot Matrix** <br><br> *X: Avg Resolution Hours* <br> *Y: Churn Risk Score* <br> *Bubble Size: Current ARR* | 🔵 **(Scatter Plot continued)** | 🤖 **Key Influencers (AI)** <br><br> *What increases churn?* <br> *Explain by: Ticket Severity, Tier* |
| 🔵 **(Scatter Plot continued)** | 🔵 **(Scatter Plot continued)** | 🤖 **(Key Influencers continued)** |

### Visual Configurations:
* **Middle Canvas (Scatter Plot - The Workhorse):**
  * Let Color dictate Risk: Red (High), Yellow (Med), Green (Low).
  * *Business Strategy:* Any large (high ARR) red bubble residing in the Top Right quadrant means *slow support times are directly provoking an extremely valuable client to leave*. CS executives must intervene immediately here.
* **Right Side (Key Influencers - Native AI Visual):**
  * *Business Strategy:* Microsoft's ML analyzes your ML—telling you that "When ticket severity is P1, the AI predicts churn risk increases by 34%."

---

## Page 3: The Cross-Sell Goldmine (Sales Strategy)
*Purpose: Hand Account Executives a prioritized hit list of what product to pitch to whom today.*

### Layout Wireframe
| | | |
| :---: | :---: | :---: |
| 🎛️ **Page Filter:** `Rank = 1` | 🎛️ **Page Filter:** `Prob > 80%` | 💰 **KPI:** Total Weighted Pipeline |
| 📊 **100% Stacked Bar Chart** <br><br> *Y: Industry* <br> *X: % breakout of Prod Recommendations* | 📊 **(Stacked Bar Chart continued)** | 📊 **(Stacked Bar Chart continued)** |
| 🎯 **Target Matrix** <br><br> *Acct Name, Recommended Product, Score, ARR* | 🎯 **(Target Matrix continued)** | 🎯 **(Target Matrix continued)** |

### Visual Configurations:
* **Middle Canvas (100% Stacked Bar Chart):**
  * *Business Strategy:* Reveals if the AI heavily favors pitching "Expense Management" to Finance clients versus "Travel Consulting" to Manufacturing clients to spot macro trends.
* **Bottom Canvas (Matrix Table - "The Hit List"):**
  * *Business Strategy:* Sales VPs export this exact matrix to Excel and hand it to reps as their quota list for the week. Since probability is > 80%, they have an incredibly high statistical chance of closing these exact products.

---

## Page 4: Financial Compliance & Travel Behavior
*Purpose: Connect traveler behavior to corporate policy leakage.*

### Layout Wireframe
| | | |
| :---: | :---: | :---: |
| 🎛️ **Page Filter** <br> `is_out_of_policy = TRUE` | 💵 **KPI:** Leakage Spend | 🏛️ **KPI:** Worst Offender Account |
| 🎢 **Ribbon Chart** <br><br> *X: Month* <br> *Y: Out of Policy Amount* <br> *Legend: Industry* | 🎢 **(Ribbon Chart continued)** | 🎢 **(Ribbon Chart continued)** |
| 🍩 **Donut Chart** <br><br> *Values: Out of Policy Amount* <br> *Legend: Traveler Tier* | 🍩 **(Donut Chart continued)** | 🍩 **(Donut Chart continued)** |

### Visual Configurations:
* **Middle Canvas (Ribbon Chart):**
  * Ribbon charts are incredible for tracking rank changes in out-of-policy spend across industries month-over-month.
* **Bottom Canvas (Donut Chart):**
  * *Business Strategy:* Are `Executive` travelers driving the policy leakage, or `Standard` employees? If executives are ignoring policy, HR/Finance needs to restructure the approval thresholds at the top rather than punishing employees below.

---

> [!TIP]
> **DAX Implementation Note for Cross-Sell:**
> Because you unpivoted `fact_ml_cross_sell` in our dbt pipeline, constructing Page 3 is incredibly simple. Just drag `recommendation_rank` = 1 into the visual filters pane. No complex `USERELATIONSHIP()` DAX code is required!
