output "resource_group_name" {
  value = azurerm_resource_group.this.name
}

output "storage_account_name" {
  value = azurerm_storage_account.lake.name
}

output "storage_account_id" {
  value = azurerm_storage_account.lake.id
}

output "container_urls" {
  description = "abfss:// URL per medallion container, keyed by container name"
  value = {
    for name, c in azurerm_storage_container.medallion :
    name => "abfss://${c.name}@${azurerm_storage_account.lake.name}.dfs.core.windows.net/"
  }
}

output "access_connector_id" {
  value = azurerm_databricks_access_connector.unity_catalog.id
}
