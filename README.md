## Getting Started

This project is developed in Databricks and version-controlled through GitHub. The pipeline is organised as a sequence of notebooks following the Bronze → Silver → Gold pattern.

To work with the project, clone the repository, open the notebooks in Databricks or a compatible local setup and run the pipeline in notebook order, starting from Bronze ingestion and ending with tests and presentation views.

More detailed setup and execution guidance may be added in separate documentation.

# NAF Databricks Project

A data engineering and analytics project for analysing NAF (Blood Bowl) data using a structured, production-style pipeline in Databricks. The project focuses on transforming raw match and tournament data into reliable analytical datasets and dashboards for players, coaches and the broader community.

## Overview

This project implements a full data pipeline and analytical layer designed to structure and clean raw NAF data, build consistent analytical models, generate insights through dashboards and reports and support decision-making in competitive Blood Bowl contexts. The pipeline emphasises reproducibility, data quality and clear analytical logic.

## Use Cases

The project aims to support multiple analytical perspectives. For players, it enables ratings, performance tracking and match narratives. For coaches, including national teams, it supports performance analysis, evaluation and preparation.

Tournament and race analytics are planned, with foundational structures in place. These will cover event-level metrics, participation insights, matchup analysis, power comparisons and meta-level trends as the project develops.

## Architecture

The pipeline follows a medallion architecture (Bronze → Silver → Gold). The Bronze layer handles raw ingestion, the Silver layer standardises and cleans data and the Gold layer contains analytical models and outputs, including dimension tables, fact tables, summary tables and presentation views used in dashboards.

Data assets are organised within a unified catalog structure: `naf_catalog.bronze`, `naf_catalog.silver`, `naf_catalog.gold_dim`, `naf_catalog.gold_fact`, `naf_catalog.gold_summary` and `naf_catalog.gold_presentation`. A central configuration table (naf_catalog.gold_dim.analytical_config) is used to manage analytical parameters.

## Key Features

The project includes an end-to-end data pipeline built in Databricks using SQL and PySpark, structured analytical modelling, rating systems such as ELO and state-space approaches and a dashboard-ready presentation layer. The design prioritises data quality, reproducibility and clear, explainable metrics.

## Project Structure

The pipeline is implemented as a series of notebooks organised under “NAF Project Files”, covering Bronze ingestion (100-series), Silver transformation (200-series), Gold dimension and fact tables (300-series), advanced models (320–324), summary layers (330–334), presentation views (340–344) and testing (350). Dashboards are located under “02 NAF Dashboards” and design documentation under “00 NAF Project Design”, including architecture specifications, analytical parameters, pipeline structure and development plans.

## Technology Stack

The project is built using Databricks (Community Edition), PySpark, SQL, Delta Lake and GitHub for version control.

## Design Principles

The project is built around clear separation of data layers, consistent naming and schema design, reproducible and traceable transformations, analytical transparency through documented assumptions and a balance between robustness and iteration speed.

## Status

The project is under active development. Core pipeline layers, data modelling and coach-focused analytics are implemented, including rating systems and dashboard-ready outputs.

Tournament and race analytics are currently in an early stage, with foundational structures in place but further modelling and analysis still to be developed.

Ongoing work includes expanding analytical coverage, improving dashboards and refining evaluation and tuning of rating systems.

## Notes

This project is developed as a structured data and analytics system rather than a lightweight script-based workflow. Some components, particularly advanced models and evaluation notebooks, are experimental and may evolve over time.

## License

This project is released under the MIT License.

This repository is an independent data and analytics project and is not affiliated with, endorsed by or sponsored by Games Workshop.

Blood Bowl and all associated names, rules, teams and intellectual property are © Games Workshop.

NAF data is used for analytical and non-commercial purposes. Ownership and rights to the underlying data remain with the NAF and its contributors.