"""
Azure ML Submission Script
==========================
Submits the Churn Survival model training script to run remotely on Azure ML.
Uses credentials and workspace configuration securely retrieved from the .env file.

Requirements:
    pip install azure-ai-ml azure-identity python-dotenv

Usage:
    python azure_train.py
"""

import os
from pathlib import Path

from azure.ai.ml import MLClient, command, Input, Output
from azure.ai.ml.constants import AssetTypes
from azure.ai.ml.entities import Environment
from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

def main():
    print("Authenticate & Connect to Azure ML Workspace...")
    
    # Authenticate using DefaultAzureCredential.
    # It automatically checks for AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID if using a Service Principal,
    # or leverages your local Azure CLI configuration (if you've run `az login` locally).
    credential = DefaultAzureCredential()

    # Connect to the ML Client
    try:
        ml_client = MLClient(
            credential=credential,
            subscription_id=os.environ["AZURE_SUBSCRIPTION_ID"],
            resource_group_name=os.environ["AZURE_RESOURCE_GROUP"],
            workspace_name=os.environ["AZURE_ML_WORKSPACE"],
        )
        print(f"✅ Successfully connected to Azure ML Workspace: {ml_client.workspace_name}")
    except KeyError as e:
        print(f"❌ Missing required environment variable for Azure: {e}")
        print("Please check your .env file.")
        return

    # Temporarily create the conda file for the Azure ML environment submission
    print("Defining compute environment...")
    with open("environment.yml", "w") as f:
        f.write("""
name: survival-env
dependencies:
  - python=3.11
  - pip
  - pip:
    - pandas
    - numpy
    - scikit-learn
    - lifelines
    - joblib
    - matplotlib
    - mlflow
    - azureml-mlflow
    - pyarrow
""")

    # Define the environment needed to run the survival model
    # We create a custom environment based on the local requirements
    custom_env = Environment(
        name="amex-clv-survival-env",
        description="Environment for Churn Survival Analysis",
        image="mcr.microsoft.com/azureml/openmpi4.1.0-ubuntu20.04:latest",
        conda_file="environment.yml", 
    )

    # Setup the inputs for our remote run
    # Uploading the local parquet file to Azure ML data-store
    print("Configuring input dataset...")
    my_job_inputs = {
        "features_path": Input(
            type=AssetTypes.URI_FILE, 
            path="./data/features/account_features.parquet",
            description="Synthetic account features feature matrix"
        )
    }

    # Setup the command job
    print("Building Azure ML Command Job...")
    job = command(
        code=".",  # Upload all files in current directory to the compute
        command="python -m models.survival_model --features ${{inputs.features_path}} --output-dir ./outputs",
        inputs=my_job_inputs,
        environment=custom_env,
        compute="amex-gbt-clv", # Replace with your standard target compute cluster name in Azure ML
        experiment_name="churn-survival-training",
        display_name="cox-ph-survival-run",
    )

    # Submit the job
    print(f"🚀 Submitting job to Azure ML cluster...")
    try:
        returned_job = ml_client.jobs.create_or_update(job)
        print(f"✅ Job submitted successfully!")
        print(f"Job Name: {returned_job.name}")
        print(f"View Status here: {returned_job.studio_url}")
    except Exception as e:
        print(f"❌ Job submission failed: {e}")
    finally:
        # Clean up temporary environment definition
        if os.path.exists("environment.yml"):
            os.remove("environment.yml")

if __name__ == "__main__":
    main()
