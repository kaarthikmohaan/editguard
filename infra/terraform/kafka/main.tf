# Kafka topics for the local cluster. Source of truth: docs/design.md section 9.
# Topics for later milestones (edits.flagged in M1, edits.replay.v1 in M2) are added then.

terraform {
  required_version = ">= 1.16"
  required_providers {
    kafka = {
      source  = "Mongey/kafka"
      version = "~> 0.13"
    }
  }
}

variable "bootstrap_servers" {
  description = "Kafka brokers as seen from where Terraform runs"
  type        = list(string)
  default     = ["127.0.0.1:9092", "127.0.0.1:9094", "127.0.0.1:9096"]
}

provider "kafka" {
  bootstrap_servers = var.bootstrap_servers
  tls_enabled       = false
}

locals {
  day_ms = 24 * 60 * 60 * 1000

  topics = {
    "edits.raw.v1" = {
      partitions = 6
      config     = { "retention.ms" = tostring(7 * local.day_ms) }
    }
    "baseline.raw.v1" = {
      partitions = 3
      config     = { "retention.ms" = tostring(7 * local.day_ms) }
    }
    "edits.dlq" = {
      partitions = 1
      config     = { "retention.ms" = tostring(30 * local.day_ms) }
    }
    "_producer_state" = {
      partitions = 1
      config     = { "cleanup.policy" = "compact" }
    }
  }
}

resource "kafka_topic" "this" {
  for_each = local.topics

  name               = each.key
  partitions         = each.value.partitions
  replication_factor = 3
  config             = each.value.config
}
