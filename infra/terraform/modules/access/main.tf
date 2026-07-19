# CI/CD + Databricks Workflows identity: one AAD app registration per environment, federated so
# GitHub Actions can authenticate via OIDC (no long-lived client secret in GitHub, no secret
# rotation to manage). See .github/workflows/deploy-dev.yml / promote-staging.yml for the
# consuming side (azure/login with a federated credential).
resource "azuread_application" "pipeline" {
  display_name = "${var.project}-${var.environment}-pipeline"
}

resource "azuread_service_principal" "pipeline" {
  client_id = azuread_application.pipeline.client_id
}

resource "azuread_application_federated_identity_credential" "github_oidc" {
  application_id = azuread_application.pipeline.id
  display_name   = "github-actions-${var.environment}"
  audiences      = ["api://AzureADTokenExchange"]
  issuer         = "https://token.actions.githubusercontent.com"
  # Restricts the federated credential to workflow runs against this exact branch/environment,
  # so a compromised PR from a fork can't mint a token for this identity.
  subject = (var.environment == "staging"
    ? "repo:${var.github_repo}:environment:staging"
    : "repo:${var.github_repo}:ref:refs/heads/main"
  )
}

resource "azurerm_role_assignment" "pipeline_storage_contributor" {
  scope                = var.storage_account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azuread_service_principal.pipeline.object_id
}

resource "databricks_service_principal" "pipeline" {
  application_id = azuread_application.pipeline.client_id
  display_name   = "${var.project}-${var.environment}-pipeline"
  active         = true
}

resource "databricks_permissions" "pipeline_can_manage_jobs" {
  cluster_policy_id = var.job_policy_id

  access_control {
    service_principal_name = databricks_service_principal.pipeline.application_id
    permission_level       = "CAN_USE"
  }
}

# Secret scope backed by Key Vault, not a Databricks-managed scope -- keeps the source of truth
# for secrets in Azure (auditable, rotatable, shared with non-Databricks consumers) rather than
# duplicating secret material into the Databricks control plane.
resource "azurerm_key_vault" "this" {
  name                       = "kv-${var.project}-${var.environment}"
  resource_group_name        = var.resource_group_name
  location                   = var.location
  tenant_id                  = var.tenant_id
  sku_name                   = "standard"
  soft_delete_retention_days = 7
  purge_protection_enabled   = var.environment == "staging"
}

resource "databricks_secret_scope" "app" {
  name = "${var.project}-${var.environment}"

  keyvault_metadata {
    resource_id = azurerm_key_vault.this.id
    dns_name    = azurerm_key_vault.this.vault_uri
  }
}
