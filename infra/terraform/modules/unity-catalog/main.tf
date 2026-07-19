# Assumes a Unity Catalog metastore already exists for the region and is assigned to the
# workspace (metastores are typically created once per region for an org, not per project/env
# -- see docs/architecture.md "Why we don't Terraform the metastore"). This module manages the
# catalog/schema/grants layer within that metastore, scoped to one environment.

resource "databricks_storage_credential" "external" {
  name = "cred-${var.project}-${var.environment}"
  azure_managed_identity {
    access_connector_id = var.access_connector_id
  }
  comment = "Managed identity credential for ${var.project}-${var.environment} ADLS Gen2 lake"
}

resource "databricks_external_location" "medallion" {
  for_each        = var.container_urls
  name            = "loc-${var.project}-${var.environment}-${each.key}"
  url             = each.value
  credential_name = databricks_storage_credential.external.name
  comment         = "${each.key} layer for ${var.project}-${var.environment}"
}

resource "databricks_catalog" "this" {
  name         = "${var.project}_${var.environment}"
  comment      = "Ecommerce signal platform catalog (${var.environment})"
  storage_root = var.container_urls["gold"] # catalog-managed tables default here unless a schema overrides it

  depends_on = [databricks_external_location.medallion]
}

resource "databricks_schema" "layers" {
  for_each     = toset(["bronze", "silver", "gold"])
  catalog_name = databricks_catalog.this.name
  name         = each.value
  storage_root = var.container_urls[each.value]
  comment      = "${each.value} layer"
}

resource "databricks_grants" "catalog_usage" {
  catalog = databricks_catalog.this.name

  dynamic "grant" {
    for_each = var.reader_groups
    content {
      principal  = grant.value
      privileges = ["USE_CATALOG", "USE_SCHEMA"]
    }
  }

  grant {
    principal  = var.pipeline_service_principal_app_id
    privileges = ["USE_CATALOG", "USE_SCHEMA", "CREATE_TABLE", "MODIFY", "SELECT"]
  }
}

resource "databricks_grants" "gold_reader_select" {
  schema = "${databricks_catalog.this.name}.gold"

  dynamic "grant" {
    for_each = var.reader_groups
    content {
      principal  = grant.value
      privileges = ["SELECT"]
    }
  }
}
