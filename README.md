# NAF Databricks Project

A data engineering and analytics project built on Databricks that transforms raw NAF (Blood Bowl) match and tournament data into structured analytical datasets, rating systems and interactive dashboards. The project serves players, coaches (including national team selectors) and the broader Blood Bowl community.

## Overview

The NAF maintains a global registry of sanctioned Blood Bowl tournaments and match results. This project ingests that data and builds a layered analytical pipeline — from raw ingestion through to dashboard-ready outputs — covering coach ratings, nation-level aggregations, tournament statistics and race/faction analysis. The pipeline emphasises reproducibility, data quality, centralised configuration and transparent analytical logic.

## Rating Models

A central feature of the project is its suite of coach rating systems, each progressively more sophisticated.

**Elo** (320) is the foundation: a classical Elo system with configurable K-factor and scale, producing a single skill estimate per coach. It provides a solid baseline but treats every game equally regardless of timing or race choice.

**State-Space Model (SSM)** (321) replaces the fixed-step Elo update with an Extended Kalman Filter. Version 1 uses a random-walk prior, while version 2 adds time-aware process noise and adaptive volatility — coaches who haven't played recently see their uncertainty grow, and the model self-tunes its responsiveness. SSM v2 produces both a point estimate and a calibrated uncertainty band for every coach.

**Race-Aware Rating** (323) extends the SSM framework into two dimensions. Each coach's predicted strength is decomposed into a global skill component (shared across all races) and a race-specific deviation (how much better or worse the coach performs with a particular faction). The two components are estimated jointly via a 2D Extended Kalman Filter, with information from each game split proportionally between global and race channels based on their relative uncertainties. This allows the model to leverage a coach's full match history for the global estimate while still capturing genuine race specialisation, avoiding the data-fragmentation problem that plagues naive per-race ratings.

Evaluation notebooks (322, 324) benchmark these models against baselines using Brier scores, accuracy, calibration plots and experience-sliced analysis.

## Architecture

The pipeline follows a medallion architecture across three layers. The Bronze layer (100) handles raw ingestion from the NAF data export. The Silver layer (200) standardises, cleans and conforms the data — resolving country codes, handling edge cases and producing a consistent analytical foundation. The Gold layer is split into four sub-layers: dimensions (310) for reference data and centralised configuration, facts (320–324) for rating engines and match-level outputs, summaries (330–334) for pre-aggregated analytical tables and presentation (340–344) for dashboard-contract views.

All tuneable parameters — Elo settings, SSM hyperparameters, race-rating configuration, activity windows, experience thresholds — are managed through a singleton configuration table (`naf_catalog.gold_dim.analytical_config`) defined in 310. Rating engines read from this config at runtime, so parameter changes propagate without code edits.

Data assets live within a unified catalog: `naf_catalog.{bronze, silver, gold_dim, gold_fact, gold_summary, gold_presentation}`.

## Project Structure

```
00 NAF Project Design/     Design specs, parameter docs, pipeline index, status
01 NAF Project Files/      Notebooks (100–350), executed in numeric order
02 NAF Dashboards/         Databricks AI/BI dashboard definitions
CLAUDE.md                  Developer reference (conventions, file map, technical notes)
```

Notebooks follow a numeric convention: 100-series for Bronze, 200-series for Silver, 310 for dimensions, 320–324 for fact/rating engines, 330–334 for summaries, 340–344 for presentation views and 350 for pipeline tests. Research notebooks (322, 324) are not part of the production pipeline but share the same config infrastructure.

## Technology Stack

The project is built using Databricks (Community Edition), PySpark, SQL, Delta Lake, Python (NumPy, Matplotlib for evaluation) and GitHub for version control. Dashboards use the Databricks AI/BI (Lovelytics) framework.

## Design Principles

The project is built around clear separation of data layers, centralised and documented configuration, consistent naming and schema conventions, reproducible and traceable transformations, analytical transparency through documented assumptions and a balance between robustness and iteration speed. All rating models are designed to be explainable — outputs include uncertainty estimates and diagnostic plots, not just point predictions.

## Status

Core pipeline layers are complete: Bronze through Gold, including Elo, SSM v1/v2 and race-aware rating engines. Coach-focused analytics and a polished coach dashboard are in production. Nation analytics are in early development with foundational structures in place. Tournament and race summary/presentation layers have stub notebooks ready for expansion.

Ongoing work includes expanding nation and tournament dashboards, refining race-rating calibration (current evaluation shows slight overconfidence at the high end) and improving publication readiness of documentation and results.

## Getting Started

This project is developed in Databricks and version-controlled through GitHub. The pipeline is organised as a sequence of notebooks following the Bronze → Silver → Gold pattern.

### Prerequisites

You need a Databricks workspace. The free [Databricks Community Edition](https://community.cloud.databricks.com/) is sufficient — no paid tier is required.

### Setup

1. **Load source data into a Volume.** In your Databricks workspace, create a Unity Catalog volume at `/Volumes/naf_catalog/bronze/naf_raw/`. Download the NAF data export (see External Data below) and upload the CSV files into that volume. Also upload the FIFA and ISO country code files referenced in the Bronze notebook.

2. **Clone the repository.** In the Databricks sidebar, go to **Repos → Add Repo** and paste the GitHub URL (`https://github.com/Tripleskull/NAF-Databricks-Project`). This pulls all notebooks and design docs into your workspace.

3. **Run the pipeline.** Open the notebooks under `01 NAF Project Files/` and run them in numeric order: 100 → 200 → 310 → 320 → 321 → 330 → 331 → 332 → 333 → 334 → 340 → 341 → 342 → 343 → 344 → 350. Each notebook is self-contained and creates its tables/views in `naf_catalog`. You can run them one at a time or wire them into a Databricks Workflow job for automation.

4. **Open the dashboards.** After the pipeline completes, import the dashboard JSON files from `02 NAF Dashboards/` via **Dashboards → Import** in the Databricks UI. The dashboards read from the `gold_presentation` views created in step 3.

Research notebooks (322, 324) are optional — they evaluate and tune the rating models but are not needed for the core pipeline.


## External Data and Reference Sources

This project relies on external reference data for country and region standardisation.

- **NAF data export (primary data source)**
  https://member.thenaf.net/glicko/nafstat-tmp-name.zip
  Daily updated dataset used as the foundation for all analysis.

- **FIFA / IOC country codes**
  https://www.rsssf.org/miscellaneous/fifa-codes.html
  Maintained by the Rec.Sport.Soccer Statistics Foundation (RSSSF).
  © Iain Jeffree and RSSSF. Used with acknowledgement.

- **ISO country codes dataset**
  https://gist.github.com/radcliff/f09c0f88344a7fcef373
  Used for ISO-based country code mappings and standardisation.

These sources are not maintained by this repository and may change over time. Ownership and rights to the underlying data remain with the respective authors and organisations.

## License

This project is released under the MIT License.

This repository is an independent data and analytics project and is not affiliated with, endorsed by or sponsored by Games Workshop.

Blood Bowl and all associated names, rules, teams and intellectual property are © Games Workshop.

NAF data is used for analytical and non-commercial purposes. Ownership and rights to the underlying data remain with the NAF and its contributors.