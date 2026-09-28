# 0012. CD checks Terraform for drift; the laptop applies it

- Status: accepted
- Date: 2026-09-28

## Context
The build plan's release flow says CD deploys the AWS side to staging with `terraform apply`. The `env` Terraform module manages the CI roles themselves (trust policy and permissions). A CI role that can apply it needs IAM write on its own role, so a compromised workflow could grant itself anything. That contradicts `docs/security.md` ("No role has `iam:*`"; short-lived OIDC roles scoped per environment).

Recording the calls a plan of this module makes gave 15 read-only APIs: the state object, the role and its inline policy, the salt's metadata, Glue databases and tags, the Athena workgroup and tags.

## Options
| Option | Pros | Cons |
| --- | --- | --- |
| A. CD runs `terraform plan -detailed-exitcode` on staging and fails on any difference; `terraform apply` stays on the laptop with the SSO admin session | CI stays read-only on IAM; drift between git and AWS still blocks a release | Infrastructure changes need one manual `make infra` before the release |
| B. Give the staging CI role IAM write, limited to its own role | Fully automatic | The role could widen its own permissions: privilege escalation |
| C. Leave Terraform out of CD | Simplest | Drift goes unnoticed until someone looks |

## Decision
Option A. The release workflow plans staging with a read-only role (state read, `iam:Get*`/`List*` on its own role only, the salt's metadata, tags) and fails if AWS differs from git; `make infra ENV=…` on the laptop remains the only way to change infrastructure, and `make deploy` runs it for prod.

## Consequences
- A release whose Terraform changed fails at the drift check until `make infra ENV=staging` is run; the error says so.
- The staging CI role can read the Terraform state (it holds no secrets: this module only reads the salt's metadata, and the salt itself is write-only in `account`) and its own IAM policy. The prod CI role gets none of this.
- Plans use `-lock=false`: they change nothing, and the role has no write access to the state bucket.
