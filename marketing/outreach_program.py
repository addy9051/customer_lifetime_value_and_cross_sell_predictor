import dspy


class OutreachSignature(dspy.Signature):
    """
    Generate a tailored, professional outreach message for an Amex GBT corporate travel client.
    The message should be consultative, data-driven, and aimed at increasing service adoption or mitigating churn risk.
    """

    account_id = dspy.InputField(desc="The unique identifier for the corporate account")
    industry = dspy.InputField(desc="The industry the client operates in")
    clv_tier = dspy.InputField(desc="The CLV tier of the client (Platinum, Gold, Silver, Bronze)")
    clv_predicted = dspy.InputField(desc="The predicted 12-month CLV in USD")
    churn_risk_level = dspy.InputField(desc="The churn risk level (High, Medium, Low)")
    current_products = dspy.InputField(desc="List of products currently used by the client")
    top_recommendation = dspy.InputField(desc="The product we recommend they adopt next")
    recent_performance = dspy.InputField(desc="Summary of recent booking spend and support ticket volume")

    outreach_message = dspy.OutputField(desc="A persuasive, professional email or talk track (2-3 paragraphs)")
    recommended_next_step = dspy.OutputField(desc="A specific, actionable 'Next Best Action' for the Account Manager")


class OutreachProgram(dspy.Module):
    def __init__(self):
        super().__init__()
        # Using ChainOfThought to encourage the model to reason about the data before writing
        self.generate_outreach = dspy.ChainOfThought(OutreachSignature)

    def forward(
        self,
        account_id,
        industry,
        clv_tier,
        clv_predicted,
        churn_risk_level,
        current_products,
        top_recommendation,
        recent_performance,
    ):
        return self.generate_outreach(
            account_id=account_id,
            industry=industry,
            clv_tier=clv_tier,
            clv_predicted=clv_predicted,
            churn_risk_level=churn_risk_level,
            current_products=current_products,
            top_recommendation=top_recommendation,
            recent_performance=recent_performance,
        )
