variable "subscription_id" {
  type = string
}

variable "tenant_id" {
  type = string
}

variable "databricks_workspace_url" {
  description = "https://adb-xxxx.azuredatabricks.net"
  type        = string
}

variable "location" {
  type    = string
  default = "eastus2"
}

variable "storage_suffix" {
  description = "Globally-unique suffix for the ADLS Gen2 storage account name"
  type        = string
}

variable "github_repo" {
  description = "owner/repo for the OIDC federated credential subject"
  type        = string
}

variable "reader_groups" {
  type    = list(string)
  default = ["data-analysts"]
}
