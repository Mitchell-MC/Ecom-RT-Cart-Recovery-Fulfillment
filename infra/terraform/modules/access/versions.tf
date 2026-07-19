terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
    azuread = {
      source = "hashicorp/azuread"
    }
    azurerm = {
      source = "hashicorp/azurerm"
    }
  }
}
