# Account-level resources, created once and shared by the staging and prod environments:
# the data lake bucket, the $5 budget alert, the GitHub OIDC identity provider and the
# username-hashing salt. Per-environment resources live in ../env.

terraform {
  required_version = ">= 1.16"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.7"
    }
  }
  # Bucket name is passed at init (it contains the account ID): see the Makefile.
  backend "s3" {
    key          = "account/terraform.tfstate"
    region       = "ap-south-1"
    encrypt      = true
    use_lockfile = true
  }
}

variable "region" {
  type    = string
  default = "ap-south-1"
}

variable "budget_email" {
  description = "Where AWS Budgets sends alerts. Set in local.auto.tfvars (gitignored)."
  type        = string
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      project    = "editguard"
      managed_by = "terraform"
      component  = "account"
    }
  }
}

data "aws_caller_identity" "current" {}

# --- Data lake bucket: s3://<bucket>/stg/ and s3://<bucket>/prod/ (contract servers) ---

resource "aws_s3_bucket" "data" {
  bucket = "editguard-data-${data.aws_caller_identity.current.account_id}-${var.region}"
}

resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket                  = aws_s3_bucket.data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 30 # design: backups table
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  rule {
    id     = "expire-athena-results"
    status = "Enabled"
    filter {
      prefix = "athena-results/"
    }
    expiration {
      days = 7
    }
  }
}

# --- Budget: alert when real spend (credits excluded) reaches or is forecast to pass $5 ---

resource "aws_budgets_budget" "monthly" {
  name         = "editguard-monthly"
  budget_type  = "COST"
  limit_amount = "5"
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  cost_types {
    include_credit = false # otherwise credits hide real spend and the alert never fires
    include_refund = false
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.budget_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.budget_email]
  }
}

# --- GitHub Actions OIDC: CI gets short-lived AWS credentials, no stored keys ---

resource "aws_iam_openid_connect_provider" "github" {
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
}

# --- Username hashing salt (design: Secrets Manager, never rotated during the project) ---
# Write-only: the value goes to AWS but is never stored in Terraform state.

ephemeral "random_password" "salt" {
  length  = 64
  special = false
}

resource "aws_secretsmanager_secret" "salt" {
  name        = "editguard/username-salt"
  description = "Salt for hashing usernames in silver and gold. Rotating it breaks joins."
}

resource "aws_secretsmanager_secret_version" "salt" {
  secret_id                = aws_secretsmanager_secret.salt.id
  secret_string_wo         = ephemeral.random_password.salt.result
  secret_string_wo_version = 1 # bump only to deliberately replace the salt
}

output "data_bucket" {
  value = aws_s3_bucket.data.bucket
}

output "github_oidc_provider_arn" {
  value = aws_iam_openid_connect_provider.github.arn
}

output "salt_secret_arn" {
  value = aws_secretsmanager_secret.salt.arn
}
