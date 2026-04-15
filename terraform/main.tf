terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }

  # SECURITY: Remote backend — prevents tfstate (containing secrets) from local storage or Git commits
  backend "azurerm" {
    resource_group_name  = "amex-gbt-ml-rg"
    storage_account_name = "amexgbttfstate"
    container_name       = "tfstate"
    key                  = "clv-predictor.tfstate"
  }
}

provider "azurerm" {
  features {}
}

# =============================================================================
# Resource Group
# =============================================================================
resource "azurerm_resource_group" "ml_rg" {
  name     = var.resource_group_name
  location = var.location
  tags = {
    Environment = "Production"
    Project     = "CLV_Cross_Sell_AI"
    Team        = "Amex_GBT_DataScience"
  }
}

# =============================================================================
# Azure Container Registry (ACR)
# =============================================================================
resource "azurerm_container_registry" "ml_acr" {
  name                = var.acr_name
  resource_group_name = azurerm_resource_group.ml_rg.name
  location            = azurerm_resource_group.ml_rg.location
  sku                 = "Standard"
  admin_enabled       = false
}

# =============================================================================
# Azure Kubernetes Service (AKS)
# =============================================================================
resource "azurerm_kubernetes_cluster" "ml_aks" {
  name                = var.cluster_name
  location            = azurerm_resource_group.ml_rg.location
  resource_group_name = azurerm_resource_group.ml_rg.name
  dns_prefix          = "${var.cluster_name}-dns"

  default_node_pool {
    name       = "default"
    node_count = var.node_count
    vm_size    = var.vm_size
  }

  identity {
    type = "SystemAssigned"
  }

  oidc_issuer_enabled       = true
  workload_identity_enabled = true

  network_profile {
    network_plugin    = "azure"
    network_policy    = "calico"
    load_balancer_sku = "standard"
  }
}

# =============================================================================
# Role Assignment (Allow AKS to pull images from ACR)
# =============================================================================
resource "azurerm_role_assignment" "aks_to_acr" {
  principal_id                     = azurerm_kubernetes_cluster.ml_aks.kubelet_identity[0].object_id
  role_definition_name             = "AcrPull"
  scope                            = azurerm_container_registry.ml_acr.id
  skip_service_principal_aad_check = true
}

# =============================================================================
# Outputs
# =============================================================================
output "aks_cluster_name" {
  value = azurerm_kubernetes_cluster.ml_aks.name
}

output "acr_login_server" {
  value = azurerm_container_registry.ml_acr.login_server
}

output "kube_config_raw" {
  value     = azurerm_kubernetes_cluster.ml_aks.kube_config_raw
  sensitive = true
}
