variable "resource_group_name" {
  description = "Name of the Azure Resource Group"
  type        = string
  default     = "amex-gbt-ml-rg"
}

variable "location" {
  description = "Azure Region"
  type        = string
  default     = "Central US"
}

variable "cluster_name" {
  description = "Name of the Azure Kubernetes Service (AKS) cluster"
  type        = string
  default     = "amex-gbt-clv-aks"
}

variable "acr_name" {
  description = "Name of the Azure Container Registry (must be globally unique)"
  type        = string
  default     = "amexgbtmlcontainers"
}

variable "node_count" {
  description = "Number of nodes in the default node pool"
  type        = number
  default     = 2
}

variable "vm_size" {
  description = "VM sizes of the nodes in the AKS cluster"
  type        = string
  default     = "Standard_D2s_v3" # Explicitly listed as available in Central US
}
