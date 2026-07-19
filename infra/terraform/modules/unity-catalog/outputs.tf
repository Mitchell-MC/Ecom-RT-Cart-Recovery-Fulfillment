output "catalog_name" {
  value = databricks_catalog.this.name
}

output "schema_full_names" {
  value = { for k, s in databricks_schema.layers : k => "${databricks_catalog.this.name}.${s.name}" }
}
