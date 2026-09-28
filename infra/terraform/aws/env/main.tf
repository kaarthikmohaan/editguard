# Per-environment resources. One module, one state key and one .tfvars file per environment
# (design: "three environments share one Terraform module, separated by variables").
# Apply with: make infra ENV=staging|prod

terraform {
  required_version = ">= 1.16"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
  # Bucket and key are passed at init by the Makefile (key = env/<env>/terraform.tfstate).
  backend "s3" {
    region       = "ap-south-1"
    encrypt      = true
    use_lockfile = true
  }
}

variable "env" {
  description = "Environment name: staging or prod"
  type        = string
  validation {
    condition     = contains(["staging", "prod"], var.env)
    error_message = "env must be staging or prod."
  }
}

variable "prefix" {
  description = "Short prefix for S3 paths and Glue databases (design: stg_*, prod_*)"
  type        = string
}

variable "region" {
  type    = string
  default = "ap-south-1"
}

# GitHub's OIDC subject names the owner and repo with their numeric IDs
# (repo:<owner>@<owner_id>/<repo>@<repo_id>:...). The IDs never change, so a deleted and
# re-created repo with the same name cannot take over this role. They are public, not secrets.
variable "github_repo" {
  type    = string
  default = "kaarthikmohaan@158012573/editguard@1386935390"
}

variable "athena_scan_cutoff_bytes" {
  description = "Per-query scan limit (design guardrail: 1 GB)"
  type        = number
  default     = 1073741824
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      project    = "editguard"
      managed_by = "terraform"
      component  = "env"
      env        = var.env
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  account_id  = data.aws_caller_identity.current.account_id
  bucket      = "editguard-data-${local.account_id}-${var.region}"
  bucket_arn  = "arn:aws:s3:::${local.bucket}"
  layers      = ["bronze", "silver", "gold"]
  results_key = "athena-results/${var.prefix}/"
}

# --- Glue databases: <prefix>_bronze, <prefix>_silver, <prefix>_gold ---

resource "aws_glue_catalog_database" "layer" {
  for_each     = toset(local.layers)
  name         = "${var.prefix}_${each.key}"
  location_uri = "s3://${local.bucket}/${var.prefix}/${each.key}/"
}

# --- Athena workgroup with the 1 GB per-query scan cutoff ---

resource "aws_athena_workgroup" "this" {
  name          = "editguard-${var.prefix}"
  force_destroy = true # only drops saved query history; data lives in S3

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.athena_scan_cutoff_bytes

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://${local.bucket}/${local.results_key}"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

# --- CI role: GitHub Actions in this repo's <env> environment only, via OIDC ---

data "aws_iam_policy_document" "ci_trust" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = ["arn:aws:iam::${local.account_id}:oidc-provider/token.actions.githubusercontent.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${var.github_repo}:environment:${var.env}"]
    }
  }
}

resource "aws_iam_role" "ci" {
  name                 = "editguard-ci-${var.env}"
  assume_role_policy   = data.aws_iam_policy_document.ci_trust.json
  max_session_duration = 3600
}

# Data-plane access only: this environment's S3 prefix, Glue databases and Athena workgroup.
# No iam:* and nothing outside the project bucket (docs/security.md section 4).
data "aws_iam_policy_document" "ci_access" {
  statement {
    sid       = "ListOwnPrefix"
    actions   = ["s3:ListBucket"]
    resources = [local.bucket_arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${var.prefix}/*", "${local.results_key}*"]
    }
  }

  # Athena looks up the results bucket's region before each query. GetBucketLocation has no
  # s3:prefix, so the condition above never matches it. It reveals only the region.
  statement {
    sid       = "AthenaResultsBucketRegion"
    actions   = ["s3:GetBucketLocation"]
    resources = [local.bucket_arn]
  }

  statement {
    sid       = "ReadWriteOwnPrefix"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = ["${local.bucket_arn}/${var.prefix}/*", "${local.bucket_arn}/${local.results_key}*"]
  }

  statement {
    sid = "GlueOwnDatabases"
    actions = [
      "glue:GetDatabase", "glue:GetDatabases",
      "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
      "glue:GetPartition", "glue:GetPartitions", "glue:BatchCreatePartition",
      "glue:BatchDeletePartition", "glue:BatchGetPartition",
      # dbt-athena prunes old Glue table versions after each model
      "glue:GetTableVersion", "glue:GetTableVersions", "glue:BatchDeleteTableVersion",
    ]
    resources = concat(
      ["arn:aws:glue:${var.region}:${local.account_id}:catalog"],
      [for l in local.layers : "arn:aws:glue:${var.region}:${local.account_id}:database/${var.prefix}_${l}"],
      [for l in local.layers : "arn:aws:glue:${var.region}:${local.account_id}:table/${var.prefix}_${l}/*"],
    )
  }

  statement {
    sid = "AthenaOwnWorkgroup"
    actions = [
      "athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults",
      "athena:StopQueryExecution", "athena:GetWorkGroup", "athena:ListQueryExecutions",
    ]
    resources = [aws_athena_workgroup.this.arn]
  }

  # The nightly dbt build hashes usernames with the salt (design: one salt, never rotated).
  # Staging only: prod dbt runs from the laptop, so the prod CI role never needs it.
  dynamic "statement" {
    for_each = var.env == "staging" ? [1] : []
    content {
      sid       = "ReadUsernameSalt"
      actions   = ["secretsmanager:GetSecretValue"]
      resources = [data.aws_secretsmanager_secret.salt.arn]
    }
  }

  # Release drift check (ADR 0012): CI runs `terraform plan` on staging and fails if AWS differs
  # from git. Read-only: exactly the calls a plan of this module makes. Apply stays on the laptop.
  dynamic "statement" {
    for_each = var.env == "staging" ? local.drift_check_reads : {}
    content {
      sid       = statement.key
      actions   = statement.value.actions
      resources = statement.value.resources
    }
  }

  dynamic "statement" {
    for_each = var.env == "staging" ? [1] : []
    content {
      sid       = "DriftCheckListState"
      actions   = ["s3:ListBucket"]
      resources = ["arn:aws:s3:::${local.tfstate_bucket}"]
      condition {
        test     = "StringLike"
        variable = "s3:prefix"
        values   = ["env:/*", "env/${var.env}/*"]
      }
    }
  }
}

locals {
  tfstate_bucket = "editguard-tfstate-${local.account_id}-${var.region}"
  drift_check_reads = {
    DriftCheckReadState = {
      actions   = ["s3:GetObject"]
      resources = ["arn:aws:s3:::${local.tfstate_bucket}/env/${var.env}/terraform.tfstate"]
    }
    DriftCheckReadOwnRole = {
      actions   = ["iam:GetRole", "iam:GetRolePolicy", "iam:ListRolePolicies", "iam:ListAttachedRolePolicies"]
      resources = ["arn:aws:iam::${local.account_id}:role/editguard-ci-${var.env}"]
    }
    DriftCheckDescribeSalt = {
      actions   = ["secretsmanager:DescribeSecret", "secretsmanager:GetResourcePolicy"]
      resources = [data.aws_secretsmanager_secret.salt.arn]
    }
    DriftCheckReadTags = {
      actions = ["glue:GetTags", "athena:ListTagsForResource"]
      resources = concat(
        [for l in local.layers : "arn:aws:glue:${var.region}:${local.account_id}:database/${var.prefix}_${l}"],
        [aws_athena_workgroup.this.arn],
      )
    }
  }
}

data "aws_secretsmanager_secret" "salt" {
  name = "editguard/username-salt"
}

resource "aws_iam_role_policy" "ci_access" {
  name   = "editguard-ci-${var.env}-data"
  role   = aws_iam_role.ci.id
  policy = data.aws_iam_policy_document.ci_access.json
}

output "glue_databases" {
  value = [for d in aws_glue_catalog_database.layer : d.name]
}

output "athena_workgroup" {
  value = aws_athena_workgroup.this.name
}

output "ci_role_arn" {
  value = aws_iam_role.ci.arn
}
