# Diagrams

This folder contains architecture and pipeline flow diagrams for the `azure-adf-incremental-ingestion-framework` project.

## Files

| File | Purpose |
|---|---|
| `architecture_overview.mmd` | High-level architecture showing SQL Server local, SHIR, Azure Data Factory, ADLS Gen2, and control metadata. |
| `adf_pipeline_flow.mmd` | Pipeline-level flow showing the master orchestrator, SQL incremental pipeline, and file ingestion pipeline. |

## Diagram Strategy

The diagrams are stored as Mermaid source files so they can be version-controlled, reviewed, and updated easily.

PNG diagrams may be added later for README and documentation presentation, but Mermaid source files remain the editable source of truth.