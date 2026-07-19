variable "project" {
  type = string
}

variable "environment" {
  type = string
}

variable "access_connector_id" {
  description = "Azure Databricks Access Connector resource ID (from the storage module)"
  type        = string
}

variable "container_urls" {
  description = "abfss:// URL per medallion layer, keyed by bronze/silver/gold/checkpoints (from the storage module)"
  type        = map(string)
}

variable "reader_groups" {
  description = "Databricks account-level group names granted read access (BI/analytics consumers)"
  type        = list(string)
  default     = []
}

variable "pipeline_service_principal_app_id" {
  description = "Application ID of the service principal the CI/CD pipeline and Workflows run as"
  type        = string
}
