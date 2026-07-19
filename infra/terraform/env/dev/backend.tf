# Remote state lives in a small storage account created once, out-of-band, by
# infra/terraform/bootstrap (see infra/terraform/README.md). This avoids the chicken-and-egg
# problem of using Terraform to create the storage account Terraform's own state lives in.
terraform {
  backend "azurerm" {
    resource_group_name  = "rg-ecom-tfstate"
    storage_account_name = "stecomtfstate" # placeholder -- override via -backend-config in CI
    container_name       = "tfstate"
    key                  = "dev.tfstate"
  }
}
