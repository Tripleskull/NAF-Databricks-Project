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

### Model Comparison

The three rating engines form a progression — each adds a capability the previous one lacks:

```mermaid
flowchart LR
    subgraph elo ["<b>Elo (320)</b>"]
        direction TB
        E1["Fixed K-factor update"]
        E2["Single point estimate"]
        E3["No uncertainty, no time\nawareness, no race info"]
    end

    subgraph ssm ["<b>SSM v2 (321)</b>"]
        direction TB
        S1["Extended Kalman Filter"]
        S2["Skill estimate <b>+ uncertainty band</b>"]
        S3["Time-aware: uncertainty\ngrows during inactivity"]
        S4["Adaptive volatility via EWMA"]
    end

    subgraph rr ["<b>Race-Aware (323)</b>"]
        direction TB
        R1["Joint 2D EKF"]
        R2["Global skill <b>g</b> + race\ndeviation <b>d</b> per faction"]
        R3["Shares information across\nraces via global component"]
    end

    elo -- "+uncertainty\n+time" --> ssm -- "+race\ndecomposition" --> rr
```

The plot below illustrates SSM v2 output for a single coach. The **top panel** shows the skill estimate (blue) with its ±2σ uncertainty band (shaded), overlaid with Elo (red) for comparison. Where Elo produces a single line, the SSM adds a calibrated confidence interval that widens during periods of inactivity and narrows as new results arrive. The **bottom panel** shows the process noise components that drive the uncertainty dynamics.

![SSM v2 Rating Diagnostics](Output%20Examples/SSM%20rating%20example.png)

## Dashboards

### Coach Dashboard

Interactive per-coach profile: career stats, W/D/L trends, Elo trajectory, race-specific ratings, rival analysis, and opponent-strength breakdowns.

![Coach Dashboard](Output%20Examples/coach_dashboard_preview.png)

Full dashboard export: [Coach Dashboard (PDF)](Output%20Examples/Coach%20Dashboard%202026-04-01%2008_16.pdf)

### Nation Dashboard

Nation-level analytics: member growth, rating distributions, race popularity, inter-nation results, Elo exchange, team selector, and power rankings.

![Nation Dashboard](Output%20Examples/nation_dashboard_preview.png)

Full dashboard export: [Nation Dashboard (PDF)](Output%20Examples/Nation%20Dashboard%202026-04-01%2008_21.pdf)

## Architecture

The pipeline follows a medallion architecture. Each layer builds on the previous one, with clear contracts between them:

```mermaid
flowchart LR
    subgraph src [" "]
        direction TB
        CSV["CSV / HTML\nsource files"]
    end

    subgraph bronze ["Bronze (100)"]
        direction TB
        B["Raw ingestion\n11 Delta tables"]
    end

    subgraph silver ["Silver (200)"]
        direction TB
        S["Cleaned &amp; conformed\n14 tables"]
    end

    subgraph gold ["Gold Layer"]
        direction TB
        DIM["<b>Dimensions (310)</b>\nReference entities\n+ analytical config"]
        FACT["<b>Facts (320–324)</b>\nGames, Elo, SSM,\nRace-Aware Rating"]
        SUM["<b>Summaries (330–334)</b>\nCoach, Nation,\nTournament, Race KPIs"]
        PRES["<b>Presentation (340–344)</b>\nDashboard-contract\nviews"]
        DIM --> FACT --> SUM --> PRES
    end

    subgraph out [" "]
        direction TB
        DASH["Databricks\nDashboards"]
    end

    CSV --> bronze --> silver --> DIM
    PRES --> DASH
```

All tuneable parameters — Elo settings, SSM hyperparameters, race-rating configuration, activity windows, experience thresholds — are managed through a singleton configuration table (`analytical_config`) in 310. Rating engines read from this config at runtime, so parameter changes propagate without code edits.

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