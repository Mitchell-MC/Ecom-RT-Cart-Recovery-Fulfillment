# Job-cluster policy: every Workflow task cluster in orchestration/databricks/*.yml references
# this policy_id so cost/instance-type/autotermination guardrails live in one place instead of
# being duplicated per job.
resource "databricks_cluster_policy" "job_default" {
  name = "${var.project}-${var.environment}-job-default"

  definition = jsonencode({
    "spark_version" : {
      "type" : "fixed",
      "value" : var.spark_version
    },
    "node_type_id" : {
      "type" : "allowlist",
      "values" : var.allowed_node_types
    },
    "autotermination_minutes" : {
      "type" : "fixed",
      "value" : 20
    },
    "custom_tags.project" : {
      "type" : "fixed",
      "value" : var.project
    },
    "custom_tags.environment" : {
      "type" : "fixed",
      "value" : var.environment
    },
    "num_workers" : {
      "type" : "range",
      "minValue" : 1,
      "maxValue" : var.max_workers
    },
    "spark_conf.spark.databricks.delta.preview.enabled" : {
      "type" : "fixed",
      "value" : "true"
    }
  })
}

resource "databricks_cluster_policy" "streaming" {
  name = "${var.project}-${var.environment}-streaming"

  definition = jsonencode({
    "spark_version" : {
      "type" : "fixed",
      "value" : var.spark_version
    },
    "node_type_id" : {
      "type" : "allowlist",
      "values" : var.allowed_node_types
    },
    # streaming clusters run continuously (Trigger.AvailableNow batches on a schedule, or a
    # long-running continuous job) so autotermination is intentionally disabled here.
    "autotermination_minutes" : {
      "type" : "fixed",
      "value" : 0
    },
    "num_workers" : {
      "type" : "range",
      "minValue" : 1,
      "maxValue" : var.max_streaming_workers
    },
    "custom_tags.workload" : {
      "type" : "fixed",
      "value" : "streaming"
    }
  })
}

resource "databricks_sql_endpoint" "bi" {
  name                      = "${var.project}-${var.environment}-bi-warehouse"
  cluster_size              = var.environment == "staging" ? "Small" : "2X-Small"
  auto_stop_mins            = 30
  enable_serverless_compute = true

  tags {
    custom_tags {
      key   = "project"
      value = var.project
    }
    custom_tags {
      key   = "purpose"
      value = "power-bi"
    }
  }
}
