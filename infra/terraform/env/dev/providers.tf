terraform {
  required_version = ">= 1.7"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.100"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 2.53"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.51"
    }
  }
}

provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
}

provider "azuread" {}

# Auth via workspace host + PAT/OIDC is passed through environment variables
# (DATABRICKS_HOST / DATABRICKS_TOKEN or ARM_* for azure-cli auth) rather than hardcoded here --
# see .github/workflows/deploy-dev.yml for how CI supplies these.
provider "databricks" {
  host = var.databricks_workspace_url
}
