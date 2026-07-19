resource "azurerm_resource_group" "this" {
  name     = "rg-${var.project}-${var.environment}"
  location = var.location
  tags     = var.tags
}

resource "azurerm_storage_account" "lake" {
  name                     = "st${var.project}${var.environment}${var.storage_suffix}"
  resource_group_name      = azurerm_resource_group.this.name
  location                 = azurerm_resource_group.this.location
  account_tier             = "Standard"
  account_replication_type = var.environment == "staging" ? "ZRS" : "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true # required for ADLS Gen2 (Unity Catalog external location target)
  min_tls_version          = "TLS1_2"

  blob_properties {
    delete_retention_policy {
      days = var.environment == "staging" ? 30 : 7
    }
  }

  tags = var.tags
}

resource "azurerm_storage_container" "medallion" {
  for_each              = toset(["bronze", "silver", "gold", "checkpoints"])
  name                  = each.value
  storage_account_name  = azurerm_storage_account.lake.name
  container_access_type = "private"
}

# Access Connector = the managed identity Unity Catalog uses to reach this storage account
# without embedding a service principal secret anywhere in Terraform state.
resource "azurerm_databricks_access_connector" "unity_catalog" {
  name                = "ac-${var.project}-${var.environment}"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location

  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
}

resource "azurerm_role_assignment" "uc_storage_blob_data_contributor" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.unity_catalog.identity[0].principal_id
}
