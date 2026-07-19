variable "project" {
  description = "Short project slug used in resource names, e.g. \"ecom\""
  type        = string
}

variable "environment" {
  description = "dev | staging"
  type        = string
  validation {
    condition     = contains(["dev", "staging"], var.environment)
    error_message = "environment must be \"dev\" or \"staging\"."
  }
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "eastus2"
}

variable "storage_suffix" {
  description = "Short unique suffix (storage account names must be globally unique, lowercase, <=24 chars total)"
  type        = string
}

variable "tags" {
  type    = map(string)
  default = {}
}
