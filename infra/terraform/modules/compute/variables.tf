variable "project" {
  type = string
}

variable "environment" {
  type = string
}

variable "spark_version" {
  description = "Databricks Runtime version id, e.g. 15.4.x-scala2.12 (LTS)"
  type        = string
  default     = "15.4.x-scala2.12"
}

variable "allowed_node_types" {
  type    = list(string)
  default = ["Standard_DS3_v2", "Standard_DS4_v2"]
}

variable "max_workers" {
  type    = number
  default = 4
}

variable "max_streaming_workers" {
  type    = number
  default = 2
}
