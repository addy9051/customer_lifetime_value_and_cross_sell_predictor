"""
Streamlit Dashboard — CLV & Cross-Sell Predictor
==================================================
4-page corporate client intelligence dashboard:
  1. Account Explorer — search/filter accounts, view CLV, churn, segment
  2. Segment Map — UMAP visualization with segment overlays
  3. Cross-Sell Matrix — product recommendations per account
  4. Portfolio Health — aggregate metrics and charts
"""

import os

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

# =============================================================================
# Configuration
# =============================================================================

API_BASE = os.environ.get("API_BASE_URL", "http://localhost:8000")

st.set_page_config(
    page_title="CLV & Cross-Sell Predictor | Amex GBT",
    page_icon="💎",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =============================================================================
# Data Loading (direct from files for reliability; falls back to API)
# =============================================================================

@st.cache_data(ttl=600)
def load_features():
    path = os.environ.get("FEATURES_PATH", "data/features/account_features.parquet")
    if os.path.exists(path):
        return pd.read_parquet(path)
    return pd.DataFrame()


@st.cache_data(ttl=600)
def load_segments():
    path = os.environ.get("SEGMENTS_PATH", "models/artifacts/segmentation/account_segments.parquet")
    if os.path.exists(path):
        return pd.read_parquet(path)
    return pd.DataFrame()


@st.cache_data(ttl=600)
def load_churn_risk():
    path = os.environ.get("CHURN_PATH", "models/artifacts/survival/churn_risk_predictions.parquet")
    if os.path.exists(path):
        return pd.read_parquet(path)
    return pd.DataFrame()


@st.cache_data(ttl=600)
def load_recommendations():
    path = os.environ.get("RECS_PATH", "models/artifacts/cross_sell/account_recommendations.parquet")
    if os.path.exists(path):
        return pd.read_parquet(path)
    return pd.DataFrame()


@st.cache_data(ttl=600)
def load_cross_sell_proba():
    path = os.environ.get("PROBA_PATH", "models/artifacts/cross_sell/cross_sell_probabilities.parquet")
    if os.path.exists(path):
        return pd.read_parquet(path)
    return pd.DataFrame()


@st.cache_data(ttl=600)
def load_clv_metrics():
    path = os.environ.get("CLV_METRICS_PATH", "models/artifacts/clv/model_comparison.csv")
    if os.path.exists(path):
        return pd.read_csv(path)
    return pd.DataFrame()


def build_master_df():
    """Assemble all data sources into one master dataframe."""
    features = load_features()
    if features.empty:
        return pd.DataFrame()

    segments = load_segments()
    churn = load_churn_risk()
    recs = load_recommendations()

    df = features.copy()

    if not segments.empty:
        df = df.merge(segments[["account_id", "segment", "umap_x", "umap_y"]], on="account_id", how="left")
    if not churn.empty:
        df = df.merge(churn[["account_id", "churn_risk_score", "survival_prob_365d"]], on="account_id", how="left")
    if not recs.empty:
        df = df.merge(
            recs[["account_id", "top_1_product", "top_1_score", "top_2_product", "top_2_score"]],
            on="account_id", how="left"
        )

    return df


# =============================================================================
# Styling
# =============================================================================

SEGMENT_COLORS = {
    "Platinum Partners": "#2ecc71",
    "Growth Accounts": "#3498db",
    "At-Risk Accounts": "#e74c3c",
    "Low-Engagement": "#95a5a6",
    "Unassigned": "#bdc3c7",
}

def styled_metric(label, value, delta=None, delta_color="normal"):
    """Display a styled metric card."""
    st.metric(label=label, value=value, delta=delta, delta_color=delta_color)


# =============================================================================
# Sidebar
# =============================================================================

st.sidebar.title("💎 CLV Intelligence")
st.sidebar.markdown("**Amex GBT** — Corporate Travel Analytics")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Navigation",
    ["🏠 Portfolio Health", "🔍 Account Explorer", "🗺️ Segment Map", "🛒 Cross-Sell Matrix"],
    index=0,
)

st.sidebar.markdown("---")
st.sidebar.caption("v1.0.0 | Data refreshed at model training time")

# =============================================================================
# Page: Portfolio Health
# =============================================================================

if page == "🏠 Portfolio Health":
    st.title("📊 Portfolio Health Dashboard")
    st.markdown("Aggregate view of the **5,000 corporate client** portfolio")

    df = build_master_df()
    if df.empty:
        st.error("No data loaded. Please run the model training pipelines first.")
        st.stop()

    # KPI Row
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        total_clv = df["clv_12m"].sum()
        st.metric("Total Portfolio CLV", f"${total_clv:,.0f}")
    with col2:
        avg_clv = df["clv_12m"].mean()
        st.metric("Avg CLV per Account", f"${avg_clv:,.0f}")
    with col3:
        churn_rate = df["is_churned"].mean() * 100
        st.metric("Portfolio Churn Rate", f"{churn_rate:.1f}%")
    with col4:
        if "churn_risk_score" in df.columns:
            at_risk = (df["churn_risk_score"] > 0.5).sum()
            st.metric("High-Risk Accounts", f"{at_risk:,}")
        else:
            st.metric("High-Risk Accounts", "N/A")
    with col5:
        avg_products = df["num_active_products"].mean()
        st.metric("Avg Products/Account", f"{avg_products:.1f}")

    st.markdown("---")

    # Charts row
    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("CLV Distribution by Tier")
        fig = px.box(
            df, x="tier", y="clv_12m", color="tier",
            category_orders={"tier": ["Platinum", "Gold", "Silver", "Bronze"]},
            color_discrete_map={"Platinum": "#9b59b6", "Gold": "#f39c12", "Silver": "#95a5a6", "Bronze": "#e67e22"},
            labels={"clv_12m": "12-Month CLV ($)", "tier": "Client Tier"},
        )
        fig.update_layout(showlegend=False, height=400)
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        if "segment" in df.columns:
            st.subheader("Segment Distribution")
            seg_counts = df["segment"].value_counts().reset_index()
            seg_counts.columns = ["segment", "count"]
            fig = px.pie(
                seg_counts, values="count", names="segment",
                color="segment", color_discrete_map=SEGMENT_COLORS,
                hole=0.4,
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Run segmentation model to see segment distribution.")

    # Revenue at risk
    col_c, col_d = st.columns(2)

    with col_c:
        st.subheader("Revenue at Risk (High Churn)")
        if "churn_risk_score" in df.columns:
            risk_df = df[df["churn_risk_score"] > 0.5].sort_values("clv_12m", ascending=False).head(15)
            if not risk_df.empty:
                fig = px.bar(
                    risk_df, x="account_id", y="clv_12m",
                    color="churn_risk_score",
                    color_continuous_scale="Reds",
                    labels={"clv_12m": "CLV ($)", "account_id": "Account"},
                )
                fig.update_layout(height=400, xaxis_tickangle=45)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.success("No high-risk accounts!")
        else:
            st.info("Run survival model to see revenue at risk.")

    with col_d:
        st.subheader("Product Penetration")
        products = ["has_neo", "has_egencia_analytics_studio", "has_meetings_and_events", "has_travel_consulting"]
        product_labels = ["Neo", "Egencia Analytics", "Meetings & Events", "Travel Consulting"]
        adoption = [df[col].mean() * 100 for col in products if col in df.columns]
        labels_present = [label for label, col in zip(product_labels, products) if col in df.columns]

        if adoption:
            fig = px.bar(
                x=labels_present, y=adoption,
                labels={"x": "Product", "y": "Adoption Rate (%)"},
                color=labels_present,
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            fig.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)


# =============================================================================
# Page: Account Explorer
# =============================================================================

elif page == "🔍 Account Explorer":
    st.title("🔍 Account Explorer")

    df = build_master_df()
    if df.empty:
        st.error("No data loaded.")
        st.stop()

    # Filters
    col1, col2, col3 = st.columns(3)
    with col1:
        tier_filter = st.multiselect("Tier", ["Platinum", "Gold", "Silver", "Bronze"], default=[])
    with col2:
        if "segment" in df.columns:
            seg_filter = st.multiselect("Segment", df["segment"].dropna().unique().tolist(), default=[])
        else:
            seg_filter = []
    with col3:
        search = st.text_input("Search Account ID", "")

    filtered = df.copy()
    if tier_filter:
        filtered = filtered[filtered["tier"].isin(tier_filter)]
    if seg_filter and "segment" in filtered.columns:
        filtered = filtered[filtered["segment"].isin(seg_filter)]
    if search:
        filtered = filtered[filtered["account_id"].str.contains(search, case=False)]

    st.markdown(f"**{len(filtered):,} accounts** matching filters")

    # Display columns
    display_cols = ["account_id", "tier", "industry", "region", "clv_12m",
                    "booking_count_90d", "total_spend_90d", "num_active_products"]
    if "segment" in filtered.columns:
        display_cols.append("segment")
    if "churn_risk_score" in filtered.columns:
        display_cols.append("churn_risk_score")
    if "top_1_product" in filtered.columns:
        display_cols.append("top_1_product")

    available_cols = [c for c in display_cols if c in filtered.columns]
    st.dataframe(
        filtered[available_cols].sort_values("clv_12m", ascending=False).head(100),
        use_container_width=True,
        height=400,
    )

    # Account detail
    st.markdown("---")
    st.subheader("Account Detail View")

    account_id = st.selectbox(
        "Select Account",
        filtered["account_id"].head(100).tolist(),
        index=0 if len(filtered) > 0 else None,
    )

    if account_id:
        acct = filtered[filtered["account_id"] == account_id].iloc[0]

        col_a, col_b, col_c, col_d = st.columns(4)

        with col_a:
            st.metric("12-Month CLV", f"${acct.get('clv_12m', 0):,.0f}")
        with col_b:
            if "churn_risk_score" in acct:
                risk = acct["churn_risk_score"]
                color = "🔴" if risk > 0.5 else ("🟡" if risk > 0.25 else "🟢")
                st.metric("Churn Risk", f"{color} {risk:.1%}")
            else:
                st.metric("Churn Risk", "N/A")
        with col_c:
            st.metric("Active Products", f"{int(acct.get('num_active_products', 0))}")
        with col_d:
            st.metric("Segment", acct.get("segment", "N/A"))

        # Behavioral metrics
        st.markdown("**Behavioral Metrics**")
        bcol1, bcol2, bcol3, bcol4 = st.columns(4)
        with bcol1:
            st.metric("Bookings (90d)", f"{int(acct.get('booking_count_90d', 0))}")
        with bcol2:
            st.metric("Spend (90d)", f"${acct.get('total_spend_90d', 0):,.0f}")
        with bcol3:
            st.metric("Cancel Rate", f"{acct.get('cancellation_rate_90d', 0):.1%}")
        with bcol4:
            st.metric("Tickets/Month", f"{acct.get('ticket_rate_per_month', 0):.1f}")


# =============================================================================
# Page: Segment Map
# =============================================================================

elif page == "🗺️ Segment Map":
    st.title("🗺️ Client Segment Map")

    df = build_master_df()
    if df.empty or "umap_x" not in df.columns:
        st.warning("Run the segmentation model first to generate UMAP embeddings.")
        st.stop()

    # UMAP scatter
    fig = px.scatter(
        df, x="umap_x", y="umap_y",
        color="segment" if "segment" in df.columns else "tier",
        color_discrete_map=SEGMENT_COLORS if "segment" in df.columns else None,
        hover_data=["account_id", "tier", "clv_12m", "is_churned"],
        size=np.log1p(df["clv_12m"]).clip(lower=1),
        opacity=0.7,
        labels={"umap_x": "UMAP 1", "umap_y": "UMAP 2"},
        title="Client Landscape — UMAP Projection",
    )
    fig.update_layout(height=600)
    st.plotly_chart(fig, use_container_width=True)

    # Segment breakdown table
    if "segment" in df.columns:
        st.subheader("Segment Breakdown")
        seg_stats = df.groupby("segment").agg(
            Accounts=("account_id", "count"),
            Avg_CLV=("clv_12m", "mean"),
            Total_CLV=("clv_12m", "sum"),
            Avg_Spend_90d=("total_spend_90d", "mean"),
            Churn_Rate=("is_churned", "mean"),
            Avg_Products=("num_active_products", "mean"),
        ).round(2)
        st.dataframe(seg_stats, use_container_width=True)


# =============================================================================
# Page: Cross-Sell Matrix
# =============================================================================

elif page == "🛒 Cross-Sell Matrix":
    st.title("🛒 Cross-Sell Recommendations")

    df = build_master_df()
    proba = load_cross_sell_proba()

    if df.empty:
        st.error("No data loaded.")
        st.stop()

    if proba.empty:
        st.warning("Run the cross-sell model first to generate recommendations.")
        st.stop()

    # Merge for display
    merged = df.merge(proba, on="account_id", how="left")

    # Product heatmap (top accounts by CLV)
    st.subheader("Product Propensity Heatmap — Top 50 Accounts by CLV")

    top50 = merged.sort_values("clv_12m", ascending=False).head(50)
    product_score_cols = [
        "Neo_score", "Egencia Analytics Studio_score", "Meetings & Events_score", "Travel Consulting_score"
    ]

    available_score_cols = [c for c in product_score_cols if c in top50.columns]

    if available_score_cols:
        heatmap_data = top50.set_index("account_id")[available_score_cols]

        fig = px.imshow(
            heatmap_data.values,
            x=[c.replace("_score", "") for c in available_score_cols],
            y=heatmap_data.index.tolist(),
            color_continuous_scale="YlOrRd",
            aspect="auto",
            labels={"color": "Propensity Score"},
        )
        fig.update_layout(height=800, title="Cross-Sell Propensity Scores")
        st.plotly_chart(fig, use_container_width=True)

    # Top recommendations per segment
    if "segment" in merged.columns and "top_1_product" in merged.columns:
        st.subheader("Top Recommendation by Segment")

        for segment in merged["segment"].dropna().unique():
            seg_data = merged[merged["segment"] == segment]
            if seg_data["top_1_product"].notna().any():
                top_product = seg_data["top_1_product"].mode().iloc[0]
                avg_score = seg_data["top_1_score"].mean()
                st.markdown(f"**{segment}** → {top_product} (avg score: {avg_score:.3f})")
