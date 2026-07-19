locals {
  project     = "ecom"
  environment = "dev"
  tags = {
    project     = local.project
    environment = local.environment
    managed_by  = "terraform"
  }
}

module "storage" {
  source = "../../modules/storage"

  project        = local.project
  environment    = local.environment
  location       = var.location
  storage_suffix = var.storage_suffix
  tags           = local.tags
}

module "compute" {
  source = "../../modules/compute"

  project     = local.project
  environment = local.environment
}

module "access" {
  source = "../../modules/access"

  project             = local.project
  environment         = local.environment
  location            = var.location
  resource_group_name = module.storage.resource_group_name
  tenant_id           = var.tenant_id
  storage_account_id  = module.storage.storage_account_id
  job_policy_id       = module.compute.job_policy_id
  github_repo         = var.github_repo
}

module "unity_catalog" {
  source = "../../modules/unity-catalog"

  project                           = local.project
  environment                       = local.environment
  access_connector_id               = module.storage.access_connector_id
  container_urls                    = module.storage.container_urls
  reader_groups                     = var.reader_groups
  pipeline_service_principal_app_id = module.access.pipeline_application_id
}
