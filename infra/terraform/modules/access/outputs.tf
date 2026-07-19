output "pipeline_application_id" {
  value = azuread_application.pipeline.client_id
}

output "pipeline_service_principal_object_id" {
  value = azuread_service_principal.pipeline.object_id
}

output "secret_scope_name" {
  value = databricks_secret_scope.app.name
}

output "key_vault_uri" {
  value = azurerm_key_vault.this.vault_uri
}
