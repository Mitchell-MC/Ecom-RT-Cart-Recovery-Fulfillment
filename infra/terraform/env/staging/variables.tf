variable "subscription_id" {
  type = string
}

variable "tenant_id" {
  type = string
}

variable "databricks_workspace_url" {
  type = string
}

variable "location" {
  type    = string
  default = "eastus2"
}

variable "storage_suffix" {
  type = string
}

variable "github_repo" {
  type = string
}

variable "reader_groups" {
  type    = list(string)
  default = ["data-analysts", "exec-sponsors"]
}
