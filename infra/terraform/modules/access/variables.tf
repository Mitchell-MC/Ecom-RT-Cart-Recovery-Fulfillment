variable "project" {
  type = string
}

variable "environment" {
  type = string
}

variable "location" {
  type = string
}

variable "resource_group_name" {
  type = string
}

variable "tenant_id" {
  type = string
}

variable "storage_account_id" {
  type = string
}

variable "job_policy_id" {
  type = string
}

variable "github_repo" {
  description = "owner/repo, used to scope the OIDC federated credential subject"
  type        = string
}
