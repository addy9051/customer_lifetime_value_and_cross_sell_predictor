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

### Layout & Visuals
* **Top Ribbon (Global Slicers):** `dim_date[Year/Month]`, `dim_accounts[region]`, `dim_accounts[tier]`
* **Top Row (KPI Cards):**
  * `Total Active ARR`: SUM(`fact_service_contracts[contract_value]`) where active=True.
  * `Predicted Churn ARR`: SUM of ARR where `fact_ml_churn[risk_level]` = 'High'. (Styled in Red to demand attention).
  * `Total Booking Run Rate`: Trailing 30 days total travel volume.
* **Middle Left (Decomposition Tree - Native AI Visual):**
  * **Analyze:** `Predicted Churn ARR`
  * **Explain by:** `INDUSTRY`, `TIER`, `REGION`.
  * *Business Strategy:* Allows executives to click and dynamically see exactly which combinations (e.g., *Technology* clients in *EMEA*) are driving the highest financial risk.
* **Middle Right (Line Chart):**
  * X-Axis: `dim_date[month_name]`
  * Y-Axis: `Total Active ARR` vs `Predicted Upsell Pipeline` (using `fact_ml_cross_sell[probability_score]`).
* **Bottom (Top N Matrix):**
  * Top 10 Accounts Sorted by `Predicted Churn Risk`.

---

## Page 2: The Retention Engine (Operations vs AI)
*Purpose: Arm the Customer Success team with tactical data to save at-risk accounts.*

### Layout & Visuals
* **Top Row (KPI Cards):** `Total Support Cost Proxy`, `Avg Resolution Hours`, `High Risk Accounts Count`.
* **Middle Canvas (Scatter Plot - The Workhorse):**
  * **X-Axis:** `fact_support_tickets[resolution_hours]`
  * **Y-Axis:** `fact_ml_churn[churn_risk_score]`
  * **Bubble Size:** `dim_accounts[base_acv]`
  * **Color:** Red (High Risk), Yellow (Med), Green (Low)
  * *Business Strategy:* Plotted on a 4-quadrant matrix. Any large bubble residing in the Top Right quadrant means *slow support times are directly provoking an extremely valuable client to leave*. CS executives must intervene immediately here.
* **Right Side (Key Influencers - Native AI Visual):**
  * **Analyze:** `churn_risk_score` to **Increase**
  * **Explain by:** `fact_support_tickets[severity]`, `dim_accounts[tier]`
  * *Business Strategy:* Microsoft's ML analyzes your ML—telling you that "When ticket severity is P1, the AI predicts churn risk increases by 34%."

---

## Page 3: The Cross-Sell Goldmine (Sales Strategy)
*Purpose: Hand Account Executives a prioritized hit list of what product to pitch to whom today.*

### Layout & Visuals
* **Top Row (KPI Cards):** `Total Weighted Pipeline` (SUM(`probability_score` * `dim_products[estimated_gross_margin_pct]`)).
* **Middle Left (100% Stacked Bar Chart):**
  * **Y-Axis:** `dim_accounts[industry]`
  * **X-Axis:** % Breakout of `fact_ml_cross_sell[recommended_product]`
  * *Business Strategy:* Reveals if the AI heavily favors pitching "Expense Management" to Finance clients versus "Travel Consulting" to Manufacturing clients.
* **Middle Right (Matrix Table - "The Hit List"):**
  * Columns: `Account Name`, `Recommended Product`, `Probability Score`, `Current ARR`.
  * *Filter:* Set visual filter dynamically to `recommendation_rank = 1` and `probability_score > 0.80`.
  * *Business Strategy:* Sales VPs export this exact matrix to Excel and hand it to reps as their quota list for the week. They have an 80% statistical chance of closing these deals.

---

## Page 4: Financial Compliance & Travel Behavior
*Purpose: Connect traveler behavior to corporate policy leakage.*

### Layout & Visuals
* **Page Level Filter:** `fact_bookings[is_out_of_policy] = TRUE`
* **Top Row:** `Total Out of Policy Spend`, `Most Non-Compliant Account`.
* **Middle Canvas (Ribbon Chart):**
  * **X-Axis:** `dim_date[Month]`
  * **Y-Axis:** `fact_bookings[amount]` (Out of policy only)
  * **Legend:** `dim_accounts[industry]`
* **Bottom Donut Chart:**
  * **Legend:** `dim_travelers[travel_tier]` (e.g., Executive, Standard).
  * **Values:** `fact_bookings[transaction_amount]`.
  * *Business Strategy:* Are Executives driving the policy leakage, or standard employees? If executives are ignoring policy, HR/Finance needs to restructure the approval flows.
 
---

> [!TIP]
> **DAX Implementation Note for Cross-Sell:**
> Because you unpivoted `fact_ml_cross_sell` in our dbt pipeline, constructing Page 3 is incredibly simple. Just drag `recommendation_rank` = 1 into the visual filters pane. No complex `USERELATIONSHIP()` DAX code is required!
