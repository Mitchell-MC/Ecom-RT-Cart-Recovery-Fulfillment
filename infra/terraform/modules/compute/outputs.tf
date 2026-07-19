output "job_policy_id" {
  value = databricks_cluster_policy.job_default.id
}

output "streaming_policy_id" {
  value = databricks_cluster_policy.streaming.id
}

output "sql_warehouse_id" {
  value = databricks_sql_endpoint.bi.id
}

output "sql_warehouse_jdbc_url" {
  value     = databricks_sql_endpoint.bi.jdbc_url
  sensitive = true
}
