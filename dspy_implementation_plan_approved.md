# Implementation Plan - DSPy Integration for Prescriptive Outreach

This plan outlines the integration of **DSPy** into the project to generate tailored outreach messages and action plans based on CLV scores and portfolio metrics.

## User Review Required

> [!IMPORTANT]
> **LLM Provider Configuration**: We will use **Azure OpenAI** with the **gpt-4o** model.
> You will need to provide the following environment variables in your `.env` file:
> - `AZURE_OPENAI_API_KEY`
> - `AZURE_OPENAI_ENDPOINT`
> - `AZURE_OPENAI_API_VERSION` (e.g., `2024-02-15-preview`)
> - `AZURE_OPENAI_DEPLOYMENT_NAME` (the name of your gpt-4o deployment)

## Proposed Changes

### [MODIFY] [requirements.txt](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/requirements.txt)
- Add `dspy-ai==2.4.9` (or latest stable).
- Add `openai` (as the default adapter for most DSPy backends).

### [MODIFY] [.env.example](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/.env.example)
- Add placeholders for:
  - `AZURE_OPENAI_API_KEY`
  - `AZURE_OPENAI_ENDPOINT`
  - `AZURE_OPENAI_API_VERSION`
  - `AZURE_OPENAI_DEPLOYMENT_NAME`
  - `DSPY_CACHE_BOOL`: Set to `True` for development.

---

### Marketing Module (DSPy Programs)

#### [NEW] [marketing/](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/marketing/)
A new directory to hold LLM logic, separated from the core ML models.

#### [NEW] [outreach_program.py](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/marketing/outreach_program.py)
- **`OutreachSignature`**: Defines the mapping from predictive metrics (CLV, churn risk, industry, product gaps) to a professional outreach message.
- **`OutreachProgram`**: A `dspy.Module` that implements the logic (e.g., using `dspy.ChainOfThought`).

---

### API Enhancement

#### [MODIFY] [main.py](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/api/main.py)
- **DSPy Initialization**: Setup `dspy.settings` in the `startup` event using environment variables.
- **New Schema `MarketingOutreach`**: 
  - `account_id`: str
  - `outreach_message`: str
  - `recommended_next_step`: str
- **New Endpoint `POST /generate/outreach`**:
  - Takes an `account_id`.
  - Aggregates data from `CLV`, `Churn`, and `Cross-Sell` models.
  - Calls the `OutreachProgram`.
  - Returns the tailored message.

---

### Orchestration

#### [MODIFY] [n8n/churn_alert_workflow.json](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/n8n/churn_alert_workflow.json)
- Add instructions or a placeholder node showing how to call the new `/generate/outreach` endpoint to enrich Slack/Teams alerts with the LLM-generated script.

## Open Questions

- **Optimization Loop**: The initial implementation will focus on **Zero-Shot/Few-Shot** generation. We can implement a feedback loop of conversion data for the "Optimizer" in a subsequent phase.

## Verification Plan

### Automated Tests
- **Pydantic Validation**: Ensure the new endpoint correctly validates inputs and returns expected JSON.
- **Mock LLM Test**: Create a test case using a mock DSPy adapter to verify the end-to-end data flow without burning API credits.

### Manual Verification
- Trigger `/generate/outreach` for a "High Churn Risk" account and verify the message contextually addresses the specific issues (e.g., mentions high support ticket count).
- Verify the message correctly reflects the recommended cross-sell product.
