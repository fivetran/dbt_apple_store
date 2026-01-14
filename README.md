<!--section="apple-store_transformation_model"-->
# Apple Store dbt Package

<p align="left">
    <a alt="License"
        href="https://github.com/fivetran/dbt_apple_store/blob/main/LICENSE">
        <img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" /></a>
    <a alt="dbt-core">
        <img src="https://img.shields.io/badge/dbt_Core™_version->=1.3.0,_<3.0.0-orange.svg" /></a>
    <a alt="Maintained?">
        <img src="https://img.shields.io/badge/Maintained%3F-yes-green.svg" /></a>
    <a alt="PRs">
        <img src="https://img.shields.io/badge/Contributions-welcome-blueviolet" /></a>
    <a alt="Fivetran Quickstart Compatible"
        href="https://fivetran.com/docs/transformations/dbt/quickstart">
        <img src="https://img.shields.io/badge/Fivetran_Quickstart_Compatible%3F-yes-green.svg" /></a>
</p>

This dbt package transforms data from Fivetran's Apple Store connector into analytics-ready tables.

## Resources

- Number of materialized models¹: 38
- Connector documentation
  - [Apple Store connector documentation](https://fivetran.com/docs/connectors/applications/apple-store)
  - [Apple Store ERD](https://fivetran.com/docs/connectors/applications/apple-store#schemainformation)
- dbt package documentation
  - [GitHub repository](https://github.com/fivetran/dbt_apple_store)
  - [dbt Docs](https://fivetran.github.io/dbt_apple_store/#!/overview)
  - [DAG](https://fivetran.github.io/dbt_apple_store/#!/overview?g_v=1)
  - [Changelog](https://github.com/fivetran/dbt_apple_store/blob/main/CHANGELOG.md)

## What does this dbt package do?
This package enables you to better understand your Apple App Store metrics at different granularities and provides intuitive reporting at the App Version, Platform Version, Device, Source Type, Territory, Subscription and Overview levels. It creates enriched models with metrics focused on aggregating all relevant application metrics into each of the reporting levels.

### Output schema
Final output tables are generated in the following target schema:

```
<your_database>.<connector/schema_name>_apple_store
```

### Final output tables

By default, this package materializes the following final tables:

| Table | Description |
| :---- | :---- |
| [apple_store__app_version_report](https://fivetran.github.io/dbt_apple_store/#!/model/model.apple_store.apple_store__app_version_report) | Tracks daily App Store metrics by app version and source type including crashes, active devices, installations, deletions, and sessions to monitor version performance and identify version-specific issues. <br></br>**Example Analytics Questions:**<ul><li>Which app versions have the highest active devices and session counts?</li><li>How do crash rates compare across different app versions and source types?</li><li>Which app versions have the most installations versus deletions?</li></ul>|
| [apple_store__device_report](https://fivetran.github.io/dbt_apple_store/#!/model/model.apple_store.apple_store__device_report) | Analyzes daily App Store metrics by device type and source including downloads, crashes, impressions, sessions, and subscription counts across different subscription types to optimize device-specific experiences. <br></br>**Example Analytics Questions:**<ul><li>Which devices have the most total downloads and active devices by source type?</li><li>How do crash rates and session counts vary across different device types?</li><li>Which devices have the highest subscription counts across free trial versus standard price subscriptions?</li></ul>|
| [apple_store__overview_report](https://fivetran.github.io/dbt_apple_store/#!/model/model.apple_store.apple_store__overview_report) | Provides a comprehensive daily summary of App Store performance including downloads, active devices, sessions, crashes, page views, and subscription counts across all subscription types to monitor overall app health. <br></br>**Example Analytics Questions:**<ul><li>What are the daily trends in total downloads, active devices, and sessions?</li><li>How do first-time downloads compare to redownloads over time?</li><li>What is the distribution of active subscriptions across free trial, pay-as-you-go, pay-up-front, and standard price types?</li></ul>|
| [apple_store__platform_version_report](https://fivetran.github.io/dbt_apple_store/#!/model/model.apple_store.apple_store__platform_version_report) | Monitors daily App Store metrics by platform version and source type including downloads, crashes, impressions, active devices, and sessions to ensure platform compatibility and prioritize platform version support. <br></br>**Example Analytics Questions:**<ul><li>Which platform versions have the most active devices and highest session counts?</li><li>How do crash rates vary across different platform versions and source types?</li><li>What percentage of downloads come from the newest versus older platform versions?</li></ul>|
| [apple_store__source_type_report](https://fivetran.github.io/dbt_apple_store/#!/model/model.apple_store.apple_store__source_type_report) | Analyzes daily App Store performance by acquisition source type including downloads, impressions, page views, active devices, sessions, installations, and deletions to measure channel effectiveness and optimize marketing spend. <br></br>**Example Analytics Questions:**<ul><li>Which source types generate the most first-time downloads and total downloads?</li><li>How do impressions and page views convert to downloads by source type?</li><li>What is the ratio of installations to deletions for organic versus paid source types?</li></ul>|
| [apple_store__subscription_report](https://fivetran.github.io/dbt_apple_store/#!/model/model.apple_store.apple_store__subscription_report) | Tracks daily subscription counts by product, territory, and state across different subscription types (free trial, pay-as-you-go, pay-up-front, standard) to analyze subscription performance and geographic distribution. <br></br>**Example Analytics Questions:**<ul><li>Which subscription products and territories have the highest active subscription counts?</li><li>How do subscription counts vary by state and subscription type (free trial vs standard)?</li><li>What is the geographic distribution of pay-as-you-go versus pay-up-front subscriptions?</li></ul>|
| [apple_store__territory_report](https://fivetran.github.io/dbt_apple_store/#!/model/model.apple_store.apple_store__source_type_report) | Monitors daily App Store metrics by territory and source type including downloads, impressions, page views, active devices, sessions, installations, and deletions to understand regional performance and optimize regional marketing. <br></br>**Example Analytics Questions:**<ul><li>Which territories have the highest total downloads and active devices by source type?</li><li>How do impressions and page views vary across different regions and sub-regions?</li><li>What territories show the strongest performance for organic versus paid acquisition?</li></ul>|

¹ Each Quickstart transformation job run materializes these models if all components of this data model are enabled. This count includes all staging, intermediate, and final models materialized as `view`, `table`, or `incremental`.

---

## Prerequisites
To use this dbt package, you must have the following:

- At least one Fivetran Apple Store connection syncing data into your destination.
- A **BigQuery**, **Snowflake**, **Redshift**, **PostgreSQL**, or **Databricks** destination.

## How do I use the dbt package?
You can either add this dbt package in the Fivetran dashboard or import it into your dbt project:

- To add the package in the Fivetran dashboard, follow our [Quickstart guide](https://fivetran.com/docs/transformations/dbt).
- To add the package to your dbt project, follow the setup instructions in the dbt package's [README file](https://github.com/fivetran/dbt_apple_store/blob/main/README.md#how-do-i-use-the-dbt-package) to use this package.

<!--section-end-->

### Install the package
Include the following apple_store package version in your `packages.yml` file:
> TIP: Check [dbt Hub](https://hub.getdbt.com/) for the latest installation instructions or [read the dbt docs](https://docs.getdbt.com/docs/package-management) for more information on installing packages.
```yaml
packages:
  - package: fivetran/apple_store
    version: [">=1.2.0", "<1.3.0"]
```

> All required sources and staging models are now bundled into this transformation package. Do not include `fivetran/apple_store_source` in your `packages.yml` since this package has been deprecated.

### Define database and schema variables
By default, this package runs using your destination and the `apple_store` schema. If this is not where your apple_store data is (for example, if your apple_store schema is named `apple_store_fivetran`), add the following configuration to your root `dbt_project.yml` file:

```yml
vars:
    apple_store_database: your_destination_name
    apple_store_schema: your_schema_name 
```

### Disable models for non-existent sources
Your Apple App Store connection might not sync every table that this package expects. If you use subscriptions and have the `sales_subscription_event_summary` and `sales_subscription_summary` tables synced, add the following variable to your `dbt_project.yml` file:

```yml
vars:
  apple_store__using_subscriptions: true # by default this is assumed to be false
```

### Seed `country_codes` mapping table (once)

In order to map longform territory names to their ISO country codes, we have adapted the CSV from [lukes/ISO-3166-Countries-with-Regional-Codes](https://github.com/lukes/ISO-3166-Countries-with-Regional-Codes) to align with Apple's country output [format](https://developer.apple.com/help/app-store-connect/reference/app-store-localizations/).

You will need to `dbt seed` the `apple_store_country_codes` [file](https://github.com/fivetran/dbt_apple_store/blob/main/seeds/apple_store_country_codes.csv) just once.

### (Optional) Additional configurations
<details open><summary>Expand/collapse configurations</summary>

#### Union multiple connections
If you have multiple apple_store connections in Fivetran and would like to use this package on all of them simultaneously, we have provided functionality to do so. The package will union all of the data together and pass the unioned table into the transformations. You will be able to see which source it came from in the `source_relation` column of each model. To use this functionality, you will need to set either the `apple_store_union_schemas` OR `apple_store_union_databases` variables (cannot do both) in your root `dbt_project.yml` file:

```yml
vars:
    apple_store_union_schemas: ['apple_store_usa','apple_store_canada'] # use this if the data is in different schemas/datasets of the same database/project
    apple_store_union_databases: ['apple_store_usa','apple_store_canada'] # use this if the data is in different databases/projects but uses the same schema name
```
> NOTE: The native `source.yml` connection set up in the package will not function when the union schema/database feature is utilized. Although the data will be correctly combined, you will not observe the sources linked to the package models in the Directed Acyclic Graph (DAG). This happens because the package includes only one defined `source.yml`.

To connect your multiple schema/database sources to the package models, follow the steps outlined in the [Union Data Defined Sources Configuration](https://github.com/fivetran/dbt_fivetran_utils/tree/releases/v0.4.latest#union_data-source) section of the Fivetran Utils documentation for the union_data macro. This will ensure a proper configuration and correct visualization of connections in the DAG.

#### Defining subscription events
By default, `Subscribe`, `Renew` and `Cancel` subscription events are included and required in this package for downstream usage. If you would like to add additional subscription events, please add the below to your `dbt_project.yml`:

```yml
    apple_store__subscription_events:
    - 'Renew'
    - 'Cancel'
    - 'Subscribe'
    - '<additional_event_name>'
    - '<additional_event_name>'
```

#### Change the build schema
By default, this package builds the apple_store staging models within a schema titled (`<target_schema>` + `apple_store_source`) and your apple_store modeling models within a schema titled (`<target_schema>` + `_apple_store`) in your destination. If this is not where you would like your apple_store data to be written to, add the following configuration to your root `dbt_project.yml` file:

```yml
models:
    apple_store:
      +schema: my_new_schema_name # Leave +schema: blank to use the default target_schema.
      staging:
        +schema: my_new_schema_name # Leave +schema: blank to use the default target_schema.
```

#### Change the source table references
If an individual source table has a different name than the package expects, add the table name as it appears in your destination to the respective variable:

> IMPORTANT: See this project's [`dbt_project.yml`](https://github.com/fivetran/dbt_apple_store/blob/main/dbt_project.yml) variable declarations to see the expected names.

```yml
vars:
    apple_store_<default_source_table_name>_identifier: your_table_name 
```
</details>

### (Optional) Orchestrate your models with Fivetran Transformations for dbt Core™
<details><summary>Expand for details</summary>
<br>

Fivetran offers the ability for you to orchestrate your dbt project through [Fivetran Transformations for dbt Core™](https://fivetran.com/docs/transformations/dbt). Learn how to set up your project for orchestration through Fivetran in our [Transformations for dbt Core setup guides](https://fivetran.com/docs/transformations/dbt#setupguide).
</details>

## Does this package have dependencies?
This dbt package is dependent on the following dbt packages. These dependencies are installed by default within this package. For more information on the following packages, refer to the [dbt hub](https://hub.getdbt.com/) site.
> IMPORTANT: If you have any of these dependent packages in your own `packages.yml` file, we highly recommend that you remove them from your root `packages.yml` to avoid package version conflicts.

```yml
packages:
    - package: fivetran/fivetran_utils
      version: [">=0.4.0", "<0.5.0"]

    - package: dbt-labs/dbt_utils
      version: [">=1.0.0", "<2.0.0"]

    - package: dbt-labs/spark_utils
      version: [">=0.3.0", "<0.4.0"]
```

<!--section="apple-store_maintenance"-->
## How is this package maintained and can I contribute?

### Package Maintenance
The Fivetran team maintaining this package only maintains the [latest version](https://hub.getdbt.com/fivetran/apple_store/latest/) of the package. We highly recommend you stay consistent with the latest version of the package and refer to the [CHANGELOG](https://github.com/fivetran/dbt_apple_store/blob/main/CHANGELOG.md) and release notes for more information on changes across versions.

### Contributions
A small team of analytics engineers at Fivetran develops these dbt packages. However, the packages are made better by community contributions.

We highly encourage and welcome contributions to this package. Learn how to contribute to a package in dbt's [Contributing to an external dbt package article](https://discourse.getdbt.com/t/contributing-to-a-dbt-package/657).

### Opinionated Decisions
In creating this package, which is meant for a wide range of use cases, we had to take opinionated stances on a few different questions we came across during development. We've consolidated significant choices we made in the [DECISIONLOG.md](https://github.com/fivetran/dbt_apple_store/blob/main/DECISIONLOG.md), and will continue to update as the package evolves. We are always open to and encourage feedback on these choices, and the package in general.

<!--section-end-->

## Are there any resources available?
- If you have questions or want to reach out for help, see the [GitHub Issue](https://github.com/fivetran/dbt_apple_store/issues/new/choose) section to find the right avenue of support for you.
- If you would like to provide feedback to the dbt package team at Fivetran or would like to request a new dbt package, fill out our [Feedback Form](https://www.surveymonkey.com/r/DQ7K7WW).