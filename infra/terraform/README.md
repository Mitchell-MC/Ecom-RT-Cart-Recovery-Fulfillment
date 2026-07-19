# Terraform IaC — Ecommerce Signal Platform

Provisions the Azure + Databricks platform layer for `dev` and `staging` as fully separate
Terraform state. Job/pipeline *definitions* (the actual Workflow tasks) are deployed separately
via Databricks Asset Bundles — see [orchestration/databricks](../../orchestration/databricks/).
That split is deliberate: Terraform owns slow-changing platform primitives (storage, catalog,
compute policy, identity); Asset Bundles own fast-changing job logic, so a code-only PR doesn't
need a `terraform apply` to ship.

## Module layout

| Module | Owns |
|---|---|
| `modules/storage` | Resource group, ADLS Gen2 storage account + medallion containers, Databricks Access Connector |
| `modules/unity-catalog` | Storage credential, external locations, catalog, bronze/silver/gold schemas, grants |
| `modules/compute` | Job-cluster policy, streaming-cluster policy, SQL warehouse (Power BI endpoint) |
| `modules/access` | AAD app + federated OIDC credential for GitHub Actions, Databricks service principal, Key Vault-backed secret scope |

`env/dev` and `env/staging` each wire all four modules together with environment-scoped naming
and separate remote state.

## Assumptions (documented, not hidden)

- A Unity Catalog **metastore already exists** for the target region and is assigned to both
  workspaces. Metastores are an account-level, one-per-region resource typically created once
  by whoever bootstraps the Databricks account — re-creating one per project is not realistic
  and isn't what this module does. See `docs/architecture.md`.
- The Databricks **workspaces themselves** (`dev` and `staging`) already exist. Workspace
  creation (`azurerm_databricks_workspace`) is a one-time, rarely-changed resource; in a real org
  it's usually owned by a separate "platform foundation" Terraform root, not the per-project repo.
  If you're bootstrapping from zero, add an `azurerm_databricks_workspace` resource to
  `env/dev/main.tf` before the modules above and pass its `workspace_url` output into
  `databricks_workspace_url`.

## Bootstrapping remote state (one-time, per Azure subscription)

Terraform state can't be stored in a storage account Terraform itself creates without a
chicken-and-egg problem, so this one step is run manually (or via `az cli`) exactly once:

```bash
az group create -n rg-ecom-tfstate -l eastus2
az storage account create -n stecomtfstate -g rg-ecom-tfstate -l eastus2 --sku Standard_LRS \
    --min-tls-version TLS1_2
az storage container create -n tfstate --account-name stecomtfstate
```

## Usage

```bash
cd env/dev
cp terraform.tfvars.example terraform.tfvars   # fill in real subscription/tenant/workspace values
terraform init
terraform plan  -var-file=terraform.tfvars
terraform apply -var-file=terraform.tfvars
```

`env/staging` is identical, with its own `terraform.tfvars` and state key
(`staging.tfstate`). In CI, `terraform-plan.yml` runs `plan` on every PR touching `infra/**` and
posts the plan as a PR comment; `promote-staging.yml` runs `apply` against `env/staging` only
after a human approves the GitHub Environment protection gate.

## Auth

Local runs use `az login` (azurerm/azuread providers pick up the CLI session) plus a Databricks
PAT exported as `DATABRICKS_TOKEN`. CI authenticates via OIDC federated credentials (see
`modules/access`) — no long-lived secrets are stored in GitHub.
