# Visual Package

This directory contains the visual layer for the Azure ADF Incremental Ingestion Framework.

## Assets

```text
diagrams/
├── banner.png
├── 01_high_level_architecture.png
├── 02_adf_orchestration_flow.png
├── 03_control_metadata_watermark_flow.png
├── 04_end_to_end_ingestion_flow.png
└── 05_repository_documentation_map.png
```

| Asset | Role |
|---|---|
| `banner.png` | README hero / first visual impression |
| `01_high_level_architecture.png` | SQL Server + SHIR + ADF + ADLS Gen2 architecture |
| `02_adf_orchestration_flow.png` | Master / child ADF pipeline routing |
| `03_control_metadata_watermark_flow.png` | Control metadata and failure-safe watermark behavior |
| `04_end_to_end_ingestion_flow.png` | Full ingestion path from source to lake |
| `05_repository_documentation_map.png` | Documentation and repository navigation map |

## Visual discipline

- Conceptual diagrams explain architecture and framework behavior.
- Validation screenshots remain under `docs/evidence/screenshots/`.
- Visuals should reinforce implemented behavior only.
- Azure Key Vault, CI/CD deployment, Infrastructure as Code, CDC / Change Tracking, Silver/Gold transformations, and Fabric-native capabilities remain out of scope for this MVP.

## Portfolio consistency

The package aligns with the broader Azure portfolio while preserving the identity of this project around:

- metadata-driven ingestion;
- SQL Server → Azure migration patterns;
- ADF orchestration;
- control tables and stored procedures;
- watermark reliability;
- operational failure / retry validation.

The PNG assets are the published presentation layer for GitHub; the documented framework behavior remains grounded in the implementation and evidence.
