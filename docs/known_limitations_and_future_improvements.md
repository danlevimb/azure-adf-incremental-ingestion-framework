# Known Limitations and Future Improvements

## Project

`azure-adf-incremental-ingestion-framework`

## Purpose

This document describes the known limitations of the current MVP implementation and the future improvements that could make the framework more production-ready.

The project was intentionally scoped as a practical, portfolio-oriented MVP.

The goal was not to build a fully enterprise-grade ingestion platform, but to demonstrate strong Azure Data Engineering patterns through a working, documented, and validated framework.

---

## MVP Scope Reminder

The current implementation demonstrates:

* Azure Data Factory orchestration
* SQL Server local ingestion
* CSV and JSON file ingestion
* ADLS Gen2 landing and bronze zones
* Self-hosted Integration Runtime
* Dynamic datasets
* Metadata-driven source configuration
* Datetime-based watermarks
* Control table logging
* Incremental insert validation
* Incremental update validation
* Refund scenario validation
* Empty run validation
* Controlled failed run validation
* Retry after failure validation
* Evidence-based documentation

The MVP successfully proves the framework concept.

---

## Known Limitations

## 1. Local SQL Server Dependency

The project uses local SQL Server as both:

* Source system
* Control metadata store

This was intentional for cost control and portfolio practicality.

### Limitation

The framework depends on the local machine and Self-hosted Integration Runtime being available.

If the local machine is offline, ADF cannot access the SQL Server source or control metadata.

### Future Improvement

Move the control metadata layer to Azure SQL Database or another cloud-hosted metadata store.

Possible future architecture:

```text
SQL Server source
    ↓
ADF through SHIR

Azure SQL Database
    ↓
Control metadata store
```

---

## 2. Control Metadata Stored Locally

Control tables currently live inside the same local SQL Server database as the source model.

### Limitation

In a real enterprise environment, operational metadata would often be separated from the source system.

### Future Improvement

Move control metadata to a dedicated database or Azure SQL Database.

Benefits:

* Better separation of concerns
* More cloud-native metadata management
* Easier monitoring
* Reduced dependency on source system availability
* Better alignment with production patterns

---

## 3. Datetime Watermark Only

The MVP uses:

```text
UpdatedAt
```

as the only watermark column.

### Limitation

Datetime-only watermarks can have edge cases when multiple rows share the same timestamp or when timestamp precision is not reliable.

The current logic uses:

```sql
WHERE UpdatedAt > @LastWatermarkValue
  AND UpdatedAt <= @CurrentHighWatermarkValue
```

This avoids duplicate processing for already-ingested timestamps, but does not solve all possible tie scenarios.

### Future Improvement

Implement a composite watermark pattern such as:

```text
UpdatedAt + PrimaryKey
```

Example:

```sql
WHERE
    UpdatedAt > @LastWatermarkValue
    OR
    (
        UpdatedAt = @LastWatermarkValue
        AND SourcePrimaryKey > @LastProcessedPrimaryKey
    )
```

This would reduce risk when many rows share the same `UpdatedAt`.

---

## 4. No Delete Handling

The MVP supports inserts and updates.

It does not currently detect physical deletes from source tables.

### Limitation

If a row is deleted from the source system, the current framework does not capture that change.

### Future Improvement

Add one of the following strategies:

| Strategy            | Description                                                     |
| ------------------- | --------------------------------------------------------------- |
| Soft deletes        | Add an `IsDeleted` or `DeletedAt` column to source tables.      |
| CDC                 | Use SQL Server Change Data Capture.                             |
| Change Tracking     | Use SQL Server Change Tracking.                                 |
| Snapshot comparison | Compare current source state against previously ingested state. |

For this MVP, delete handling was intentionally deferred.

---

## 5. No CDC or Change Tracking

The project uses a simple watermark strategy rather than SQL Server CDC or Change Tracking.

### Limitation

The framework relies on `UpdatedAt` being updated correctly by the source system.

If the source system does not maintain `UpdatedAt` properly, some changes could be missed.

### Future Improvement

Add a future version using:

* SQL Server Change Tracking
* SQL Server Change Data Capture
* Transaction log-based ingestion
* Event-based change capture

This would make the framework more robust for production scenarios.

---

## 6. No Silver or Gold Transformation Layer

The MVP lands data into `bronze`.

### Limitation

The project does not currently implement:

* Silver cleansing
* Silver standardization
* Gold aggregation
* Dimensional modeling
* Serving layer outputs

### Future Improvement

Add a downstream transformation project or extension.

Possible next steps:

| Layer   | Future Role                                           |
| ------- | ----------------------------------------------------- |
| Silver  | Standardized, validated, deduplicated records.        |
| Gold    | Business-ready aggregations or dimensional models.    |
| Serving | Synapse Serverless, Power BI, or Fabric access layer. |

This limitation is acceptable because the project’s core goal is ingestion orchestration, not transformation.

---

## 7. ADF Git Integration Not Yet Configured

During the MVP, ADF artifacts were created and tested directly in ADF Studio.

### Limitation

The repository does not yet include the generated ADF JSON artifacts.

### Future Improvement

Configure ADF Git integration against the public GitHub repository.

Once enabled, ADF artifacts should be version-controlled, including:

* Pipelines
* Datasets
* Linked services
* Integration runtime references
* Factory configuration files

This would improve lifecycle management and project professionalism.

---

## 8. No CI/CD Deployment Pipeline

The MVP does not include automated deployment.

### Limitation

ADF artifacts, SQL scripts, and Azure resources are not deployed through a CI/CD pipeline.

### Future Improvement

Add CI/CD using one of these approaches:

* GitHub Actions
* Azure DevOps Pipelines
* ARM templates
* Bicep
* Terraform
* ADF publish branch strategy

This could be part of a future production-readiness project.

---

## 9. No Infrastructure as Code

Azure resources were created through Azure Portal and supporting scripts.

### Limitation

The resource group, ADF instance, storage account, permissions, and containers are not fully defined as Infrastructure as Code.

### Future Improvement

Add Bicep or Terraform definitions for:

* Resource group
* Storage account
* ADLS Gen2 containers
* Azure Data Factory
* Managed identity permissions
* Role assignments
* Diagnostic settings

This would make the environment easier to recreate.

---

## 10. Limited Security Implementation

The project includes basic security-conscious patterns:

* Managed identity for ADLS linked service
* Password placeholder in SQL login script
* `.gitignore` for secrets
* No committed connection strings
* Public-safe scripts

### Limitation

It does not yet include a complete enterprise security model.

Missing items include:

* Azure Key Vault
* Secret rotation
* Private endpoints
* Network restrictions
* RBAC documentation
* Least-privilege role analysis
* Managed identity permission documentation

### Future Improvement

Add a security hardening phase.

Potential additions:

* Store secrets in Azure Key Vault
* Use Key Vault-backed linked services
* Document required RBAC assignments
* Use private endpoints where appropriate
* Add security notes to deployment documentation

---

## 11. Limited Monitoring and Alerting

Monitoring was performed through:

* ADF Studio
* Copy Activity output
* SQL control tables
* Validation queries
* Evidence screenshots

### Limitation

The MVP does not include automated alerts or dashboards.

### Future Improvement

Add monitoring enhancements such as:

* Azure Monitor alerts for failed pipeline runs
* Log Analytics integration
* ADF diagnostic settings
* Operational dashboard over `ctl.IngestionRun`
* Failure notification through email or Teams
* SLA/RPO/RTO reporting

---

## 12. Basic Retry Behavior

The project validates retry after failure manually.

### Limitation

The retry behavior is operationally proven, but not automated through retry queues or advanced recovery orchestration.

### Future Improvement

Add:

* ADF activity-level retry policy
* Retry attempt counters
* Maximum retry thresholds
* Retry queue table
* Automated retry pipeline
* Failure classification
* Backoff strategy

This would make retry handling more production-ready.

---

## 13. File Ingestion Does Not Parse Business Rows

CSV and JSON file ingestion copies files from `landing` to `bronze`.

### Limitation

The MVP does not parse file contents into structured relational or analytical tables.

It validates file movement, not full file transformation.

### Future Improvement

Add file parsing and validation:

* CSV schema validation
* JSON schema validation
* Rejected file handling
* Record-level quarantine
* File metadata logging
* Silver standardized outputs

---

## 14. No Data Quality Framework

The MVP includes scenario validation but not a formal data quality rules engine.

### Limitation

The framework does not yet include configurable data quality checks such as:

* Required fields
* Valid status values
* Referential validation
* Duplicate detection
* Range validation
* Schema drift detection

### Future Improvement

Add a data quality layer.

Possible implementation options:

* SQL validation rules
* ADF validation activities
* PySpark validation in Databricks or Fabric
* Great Expectations
* Custom quarantine/rejected zones

---

## 15. No Trigger Scheduling Yet

The framework was validated through manual/debug pipeline runs.

### Limitation

The MVP does not yet include scheduled triggers as part of the public documentation package.

### Future Improvement

Add ADF triggers:

* Daily batch trigger
* Manual backfill trigger
* Source-system-specific trigger
* File ingestion trigger
* Parameterized scheduled runs

This would strengthen production-like behavior.

---

## 16. No Cost Monitoring Dashboard

The project was implemented with cost awareness, but does not include cost monitoring assets.

### Limitation

Cost tracking is handled manually through Azure Portal awareness.

### Future Improvement

Add:

* Azure budget
* Cost alerts
* Cost notes in README
* Resource cleanup checklist
* Estimated cost section
* Development shutdown guidance

---

## 17. Evidence Is Screenshot-Based

The public repository includes evidence screenshots.

### Limitation

Screenshots are useful for portfolio review but are not a substitute for automated tests.

### Future Improvement

Add automated validation scripts that can generate repeatable evidence outputs, such as:

* SQL validation result exports
* Pipeline run metadata exports
* ADLS path validation scripts
* Markdown evidence generation
* CI-based validation checks

---

## 18. No Fabric-Native Implementation

The project aligns with current Microsoft data engineering concepts, but it is Azure Data Factory and ADLS Gen2 based.

### Limitation

It does not implement Microsoft Fabric-native assets such as:

* Fabric Data Pipelines
* Lakehouse
* Warehouse
* OneLake
* Dataflows Gen2

### Future Improvement

Create a future Fabric-oriented project aligned directly with DP-700 skills.

---

## Future Improvement Roadmap

Potential improvements can be grouped by maturity level.

## Level 1 — Small Enhancements

| Enhancement                       | Value                                 |
| --------------------------------- | ------------------------------------- |
| Add ADF triggers                  | Demonstrates scheduled orchestration. |
| Add ADF Git integration           | Adds artifact versioning.             |
| Add README architecture diagram   | Improves recruiter readability.       |
| Add SQL validation export scripts | Improves repeatability.               |
| Add cost notes                    | Improves transparency.                |

---

## Level 2 — Production Readiness

| Enhancement                 | Value                           |
| --------------------------- | ------------------------------- |
| Azure Key Vault integration | Better secret management.       |
| Azure Monitor alerts        | Operational awareness.          |
| Log Analytics integration   | Centralized monitoring.         |
| Infrastructure as Code      | Reproducible environment setup. |
| CI/CD pipeline              | Deployment maturity.            |
| Automated retry policy      | Better recovery behavior.       |

---

## Level 3 — Data Engineering Expansion

| Enhancement                 | Value                                                              |
| --------------------------- | ------------------------------------------------------------------ |
| Silver transformation layer | Adds standardization and validation.                               |
| Gold serving layer          | Adds business-ready outputs.                                       |
| Synapse Serverless          | Adds SQL lake serving.                                             |
| Databricks / Delta Lake     | Adds lakehouse transformation capability.                          |
| Fabric implementation       | Aligns directly with current Microsoft data engineering direction. |
| CDC / Change Tracking       | Improves incremental capture robustness.                           |

---

## Recommended Next Improvements

The most valuable next improvements are:

1. Configure ADF Git integration.
2. Add ADF trigger documentation and evidence.
3. Add Azure Key Vault for secrets.
4. Add basic Azure Monitor alerting.
5. Add a Synapse Serverless serving layer project after this repo is fully documented.

---

## What Will Remain Out of Scope

The current public project should not expand endlessly.

The following should remain out of scope for this repository unless intentionally promoted to a new project phase:

* Full lakehouse implementation
* Full CI/CD platform
* Enterprise governance
* Purview lineage
* Databricks transformation pipelines
* Fabric migration
* Production-grade security architecture

These should be handled as future portfolio projects or separate enhancements.

---

## Conclusion

The current MVP successfully demonstrates a metadata-driven incremental ingestion framework using Azure Data Factory, SQL Server, ADLS Gen2, watermarks, control tables, failure handling, and retry validation.

The known limitations are intentional and clearly documented.

The future improvements provide a professional roadmap for evolving the project from a validated MVP into a more production-ready data engineering solution.
