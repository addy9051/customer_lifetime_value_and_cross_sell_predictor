# Walkthrough - Prescriptive AI Outreach with DSPy

I have successfully integrated **DSPy** into the project to transition from purely predictive insights to actionable, prescriptive outreach.

## 🚀 Accomplishments

### 1. Structured Prescriptive Logic
I created a new `marketing/` module that utilizes DSPy's declarative approach. This allows the system to reason about client data before generating outreach messages.
*   **Signatures**: Defined `OutreachSignature` to formalize the mapping from CLV and churn metrics to human-readable scripts.
*   **Programs**: Implemented `OutreachProgram` using `dspy.ChainOfThought` for sophisticated, context-aware generation.

### 2. Intelligent API Endpoint
Added a new endpoint `POST /generate/outreach` to the FastAPI service.
*   **Data Aggregation**: The endpoint automatically pulls the latest CLV scores, churn risk, and product recommendations for any given `account_id`.
*   **Voice Alignment**: Configured the model to use a **Professional, Consultative, and Data-Driven** voice suitable for Amex GBT.

### 3. Production Readiness
*   **Azure OpenAI**: Configured the backend to use your preferred Azure OpenAI `gpt-4o` deployment.
*   **Container Security**: Updated `api/Dockerfile` to include the new marketing logic while maintaining least-privilege principles.
*   **Environment Templates**: Updated `.env` and `.env.example` with the necessary placeholders for your credentials.

## 🛠️ How to Use

### 1. Set Environment Variables
Update your `.env` file with your Azure OpenAI credentials:
```bash
AZURE_OPENAI_API_KEY=your_key
AZURE_OPENAI_ENDPOINT=https://your_name.openai.azure.com/
AZURE_OPENAI_DEPLOYMENT_NAME=gpt-4o
```

### 2. Generate Outreach
You can now call the new API endpoint for any corporate account:
```bash
curl -X POST "http://localhost:8000/generate/outreach" \
     -H "Content-Type: application/json" \
     -d '{"account_id": "ACC-001"}'
```

### 3. Integrated n8n Alerts
Your n8n workflows can now call this endpoint to include a tailored script in every Slack or Email notification, reducing the "time-to-action" for your sales teams.

## 🧪 Verification Results
- **Linting**: Passed `ruff check` and `ruff format` cleaning.
- **Dependency Validation**: Successfully added `dspy-ai` and `openai` to `requirements.txt`.
- **API Contract**: The new endpoint is registered and visible in the FastAPI Swagger UI (`/docs`).

> [!NOTE]
> The next phase could involve setting up the **DSPy Optimizer** to refine the message tone based on real-world conversion rates captured from your CRM.
