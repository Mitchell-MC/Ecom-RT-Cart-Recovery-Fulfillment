# EcomSignalPlatform (Power BI Project)

A `.pbip`/TMDL scaffold wired to the [local CSV preview](../../local_preview/README.md) so the
semantic model (tables, relationships, measures from [`measures.dax`](../measures.dax)) lives as
plain text under `EcomSignalPlatform.SemanticModel/definition/` instead of a binary `.pbix` --
diffable in git, editable in VS Code.

Hand-built from the spec in [`../data-model.md`](../data-model.md), [`../measures.dax`](../measures.dax),
and [`../report-layout.md`](../report-layout.md), not exported from Desktop -- open it there once to
let Desktop write its own cache (`.pbi/`, `*.abf`) and confirm everything resolves before relying on it.

## Open it

1. Power BI Desktop → File → Open → `EcomSignalPlatform.pbip` (requires the **Power BI Project
   (.pbip) save option** preview feature enabled: File → Options and settings → Options → Preview
   features).
2. First open will prompt to trust the folder and then load each table from the CSVs in
   `bi/local_preview/output/` via the `GoldPreviewFolder` Power Query parameter
   (`EcomSignalPlatform.SemanticModel/definition/expressions.tmdl`) -- currently set to this
   machine's absolute path. If you clone this repo elsewhere, update that parameter
   (Transform Data → Manage Parameters) or regenerate the CSVs and re-point it.
3. Three empty pages exist (Exec Summary, Cart Recovery Ops, Fulfillment Risk Ops) matching
   `report-layout.md` -- no visuals yet. Build those in Desktop; the model/measures are already
   wired so you're only laying out cards/tables/charts, not re-deriving DAX.
4. Modeling ribbon → Mark `dim_date` as the official Date Table (one click) -- needed for the
   time-intelligence measures (`DATESINPERIOD`, `DATEADD`) to work. Not pre-set in the TMDL because
   Desktop's "Mark as Date Table" wizard also generates internal variation metadata that's brittle
   to hand-author correctly.

## Edit it from VS Code afterward

- Install the **TMDL** extension (`analysis-services.TMDL`) for syntax highlighting/formatting on
  `.tmdl` files, and/or **Power BI Studio** (`GerhardBrueckl.powerbi-vscode`) if you want to browse
  or refresh a *published* workspace from VS Code.
- Open this folder in VS Code, edit `definition/tables/*.tmdl` or `definition/relationships.tmdl`
  directly -- e.g. add a measure, rename a column.
- Power BI Desktop doesn't watch these files for external changes: close and reopen the `.pbip`
  for edits made outside Desktop to take effect.

## Swapping to the real Databricks warehouse later

Once `infra/terraform` is applied, replace each table's `partition ... = m` CSV-import query
(`Csv.Document(File.Contents(GoldPreviewFolder & ...))`) with a Databricks connector query
against the tables named in `../data-model.md`, matching that doc's DirectQuery/Import split.
Table and column names are unchanged, so `measures.dax`/the report pages don't need touching.
