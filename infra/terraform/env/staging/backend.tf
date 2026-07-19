terraform {
  backend "azurerm" {
    resource_group_name  = "rg-ecom-tfstate"
    storage_account_name = "stecomtfstate"
    container_name       = "tfstate"
    key                  = "staging.tfstate"
  }
}
