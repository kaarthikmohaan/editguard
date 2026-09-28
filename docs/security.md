# Security and responsible use

## 1. Assets

| Asset | Where | Sensitivity |
| --- | --- | --- |
| AWS account and data | S3, Glue, Athena | High (cost, integrity) |
| Anthropic API key | macOS Keychain | High (cost) |
| Username hashing salt | AWS Secrets Manager | Medium (re-identification) |
| Usernames in bronze | S3 | Medium (public on Wikipedia, but aggregation is sensitive) |
| Diff text and edit summaries | Bronze `raw_json`, Postgres | Low, except suppressed revisions |
| Flags and reviewer feedback | Postgres | Low |

## 2. Threat model (STRIDE)

| Threat | Example | Control |
| --- | --- | --- |
| Spoofing | Someone calls the API | Localhost only; `X-API-Key`; 60 requests/min per key |
| Tampering | Vandal writes prompt injection into an article | LLM verdicts add-only; delimited input; 20 injection evals |
| Tampering | Bad deploy corrupts a table | Iceberg snapshots, rollback, write-audit-publish for backfills |
| Repudiation | Unclear who changed infra | Terraform in git; GitHub Actions logs; OIDC session names |
| Information disclosure | Keys leak to git | Keychain for the API key; SSO/OIDC for AWS; gitleaks in pre-commit and CI |
| Information disclosure | Re-identifying editors | Hashed usernames in silver/gold; no per-user aggregates exposed; no IPs |
| Information disclosure | Showing suppressed content | Visibility events hide instantly (410); weekly purge from bronze |
| Denial of service | Runaway Athena query or LLM spend | 1 GB per-query scan cutoff; $5 budget alert; daily LLM budget; load shedding |
| Denial of service | Getting blocked by Wikimedia | Identified User-Agent; ~3 API calls/min; Retry-After; ≤2 stream connections |
| Elevation of privilege | Leaked CI credentials | Short-lived OIDC roles scoped per environment; prod requires manual approval |

## 3. Credentials

| Credential | Storage | Rotation |
| --- | --- | --- |
| AWS (local) | `aws sso login`, profile in `.env` | Session expiry |
| AWS (CI) | GitHub OIDC → IAM role per environment | Per job |
| Anthropic key | macOS Keychain, loaded by Makefile | On any suspicion |
| API key for EditGuard | `.env` (local only) | Per release |
| Salt | Secrets Manager | Never during the project (rotation breaks joins) |

`.env` is gitignored; `.env.example` holds dummy values only.

## 4. IAM

Least privilege from Terraform:
- `editguard-dev`: read/write `s3://<bucket>/dev/*`, Glue `dev_*`.
- `editguard-ci-staging`: staging prefixes and databases; Athena workgroup `editguard-stg`; read-only on the salt secret (dbt hashes usernames). Assumable only from the `staging` GitHub environment; for the release drift check (ADR 0012), read-only on its own IAM role, the staging Terraform state and resource tags.
- `editguard-ci-prod`: prod prefixes; assumable only from the `prod` GitHub environment.
- No role has `iam:*`. Outside the project bucket, only `editguard-ci-staging` can read the salt secret (`secretsmanager:GetSecretValue` on that one secret); the nightly workflow masks it and the account ID in the public Actions logs.

## 5. Scanning

| Scan | Tool | Gate |
| --- | --- | --- |
| Dependencies | pip-audit, Dependabot | High severity fails CI |
| Secrets | gitleaks | Any finding fails pre-commit and CI |
| Code | ruff `S` rules | Any finding fails CI |
| Images and Terraform | trivy | High severity fails CI |

## 6. Responsible use of Wikimedia data

- Flags are suggestions for human review. EditGuard never edits or reverts Wikipedia.
- No IP addresses: logged-out editors are temporary accounts, and IPs are never available to the public.
- No per-user rankings or "vandal lists" anywhere, including the API.
- Usernames appear only on a single-edit view, as they do on Wikipedia.
- Suppressed revisions and deleted pages are hidden immediately and purged weekly.
- Only diff text and edit metadata go to the LLM API; never usernames.
- Diff excerpts are CC BY-SA 4.0 and link to their revision.
- API use follows Wikimedia's User-Agent policy and rate limits (identified client, 200 requests/min tier; EventStreams ≤2 connections per IP).

## Username salt in use

`make dbt` reads the salt from Secrets Manager into the `USERNAME_SALT` environment variable for that one command; it is never written to git or `.env`. dbt inlines it into the SQL it sends to Athena, so it also appears in:

- Athena query history for the workgroup (kept 45 days, visible only inside this AWS account);
- `transform/target/` and `transform/logs/` on the laptop (gitignored).

Do not share those folders or Athena query exports.
