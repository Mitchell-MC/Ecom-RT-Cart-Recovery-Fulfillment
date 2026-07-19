output "catalog_name" {
  value = module.unity_catalog.catalog_name
}

output "schema_full_names" {
  value = module.unity_catalog.schema_full_names
}

output "sql_warehouse_id" {
  value = module.compute.sql_warehouse_id
}

output "pipeline_application_id" {
  value = module.access.pipeline_application_id
}

output "storage_account_name" {
  value = module.storage.storage_account_name
}
