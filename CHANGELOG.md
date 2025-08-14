# dbt_apple_store v1.0.0
[PR #41](https://github.com/fivetran/dbt_apple_store/pull/41) includes the following updates:

## Breaking Changes

### Source Package Consolidation
- Removed the dependency on the `fivetran/apple_store_source` package.
  - All functionality from the source package has been merged into this transformation package for improved maintainability and clarity.
  - If you reference `fivetran/apple_store_source` in your `packages.yml`, you must remove this dependency to avoid conflicts.
  - Any source overrides referencing the `fivetran/apple_store_source` package will also need to be removed or updated to reference this package.
  - Update any apple_store_source-scoped variables to be scoped to only under this package. See the [README](https://github.com/fivetran/dbt_apple_store/blob/main/README.md) for how to configure the build schema of staging models.
- As part of the consolidation, vars are no longer used to reference staging models, and only sources are represented by vars. Staging models are now referenced directly with `ref()` in downstream models.

### dbt Fusion Compatibility Updates
- Updated package to maintain compatibility with dbt-core versions both before and after v1.10.6, which introduced a breaking change to multi-argument test syntax (e.g., `unique_combination_of_columns`).
- Temporarily removed unsupported tests to avoid errors and ensure smoother upgrades across different dbt-core versions. These tests will be reintroduced once a safe migration path is available.
  - Removed all `dbt_utils.unique_combination_of_columns` tests.
  - Moved `loaded_at_field: _fivetran_synced` under the `config:` block in `src_apple_store.yml`.

### Under the Hood
- Updated conditions in `.github/workflows/auto-release.yml`.
- Added `.github/workflows/generate-docs.yml`. 

# dbt_apple_store v0.6.0
[PR #36](https://github.com/fivetran/dbt_apple_store/pull/36) includes the following updates:

## Breaking Change for dbt Core < 1.9.6
> *Note: This is not relevant to Fivetran Quickstart users.*

Migrated `freshness` from a top-level source property to a source `config` in alignment with [recent updates](https://github.com/dbt-labs/dbt-core/issues/11506) from dbt Core ([Apple App Store Source v0.6.0](https://github.com/fivetran/dbt_apple_store_source/releases/tag/v0.6.0)). This will resolve the following deprecation warning that users running dbt >= 1.9.6 may have received:

```
[WARNING]: Deprecated functionality
Found `freshness` as a top-level property of `apple_store` in file
`models/src_apple_store.yml`. The `freshness` top-level property should be moved
into the `config` of `apple_store`.
```

**IMPORTANT:** Users running dbt Core < 1.9.6 will not be able to utilize freshness tests in this release or any subsequent releases, as older versions of dbt will not recognize freshness as a source `config` and therefore not run the tests.

If you are using dbt Core < 1.9.6 and want to continue running Apple App Store freshness tests, please elect **one** of the following options:
  1. (Recommended) Upgrade to dbt Core >= 1.9.6
  2. Do not upgrade your installed version of the `apple_store` package. Pin your dependency on v0.5.1 in your `packages.yml` file.
  3. Utilize a dbt [override](https://docs.getdbt.com/reference/resource-properties/overrides) to overwrite the package's `apple_store` source and apply freshness via the previous release top-level property route. This will require you to copy and paste the entirety of the previous release `src_apple_store.yml` file and add an `overrides: apple_store_source` property.

## Under the Hood
- Updates to ensure integration tests use latest version of dbt.

# dbt_apple_store v0.5.1
This release introduces the following updates:

## Under the Hood
- Prepends `materialized` configs in the package's `dbt_project.yml` file with `+` to improve compatibility with the newer versions of dbt-core starting with v1.10.0. ([PR #34](https://github.com/fivetran/dbt_apple_store/pull/34))
- Updates the package maintainer pull request template. ([PR #35](https://github.com/fivetran/dbt_apple_store/pull/35))

## Contributors
- [@b-per](https://github.com/b-per) ([PR #34](https://github.com/fivetran/dbt_apple_store/pull/34))

# dbt_apple_store v0.5.0
[PR #32](https://github.com/fivetran/dbt_apple_store/pull/32) includes the following updates:

## Breaking Changes: Schema Change
- Following the connector's [Nov 2024 Update](https://fivetran.com/docs/connectors/applications/apple-app-store/changelog#november2024) to sync from the [App Store Connect API](https://developer.apple.com/documentation/appstoreconnectapi), we've updated this dbt package to reflect the new schema which includes the following changes:

# Breaking Changes
- The `account_id` and `account_name` fields have been removed from the `apple_store__subscription_report`.
- `app_id` in `apple_store__subscription_report` has been replaced with `app_apple_id`.
- Additionally, while the structure of the end models remains largely intact, the underlying logic has been adjusted to align with the new grain of the source tables. As a result, some values may differ from previous outputs.
- For more information on the upstream breaking changes concerning the source tables, refer to the [source package release notes](https://github.com/fivetran/dbt_apple_store_source/releases/tag/0.5.0).
- The reporting grains are created in upstream intermediate models (found in the `intermediate/reporting_grain` folder). Along with the date spine (`int_apple_store__date_spine`), these reporting grain models are materialized as tables to enhance performance.

## Documentation
- Added Quickstart model counts to README. ([#31](https://github.com/fivetran/dbt_apple_store/pull/31))
- Corrected references to connectors and connections in the README. ([#31](https://github.com/fivetran/dbt_apple_store/pull/31))
- Updated the `DECISIONLOG` with information about excluded fields and the difference between Standard vs Detailed reports.

# dbt_apple_store v0.5.0-a1  
[PR #32](https://github.com/fivetran/dbt_apple_store/pull/32) includes the following updates:

## Breaking Changes: Schema Change
- Following the connector's [Nov 2024 Update](https://fivetran.com/docs/connectors/applications/apple-app-store/changelog#november2024) to sync from the [App Store Connect API](https://developer.apple.com/documentation/appstoreconnectapi), we've updated this dbt package to reflect the new schema which includes the following changes:

# Breaking Changes
- The `account_id` and `account_name` fields have been removed.
- `app_id` in apple_store__subscription_report has been replaced with `app_apple_id`.
- Additionally, while the structure of the end models remains largely intact, the underlying logic has been adjusted to align with the new grain of the source tables. As a result, some values may differ from previous outputs.
- For more information on the upstream breaking changes concerning the source tables, refer to the [source package pre-release notes](https://github.com/fivetran/dbt_apple_store_source/releases/tag/0.5.0-a1).

## Documentation
- Added Quickstart model counts to README. ([#31](https://github.com/fivetran/dbt_apple_store/pull/31))
- Corrected references to connectors and connections in the README. ([#31](https://github.com/fivetran/dbt_apple_store/pull/31))

# dbt_apple_store v0.4.0
[PR #22](https://github.com/fivetran/dbt_apple_store/pull/22) includes the following updates:

## 🚨 Breaking Changes 🚨
- Updated the source identifier format for consistency with other packages and for compatibility with the `fivetran_utils.union_data` macro. The identifier variables now are:

previous | current
--------|---------
`app_identifier` | `apple_store_app_identifier`
`app_store_platform_version_source_type_report_identifier` | `apple_store_app_store_platform_version_source_type_report_identifier`
`app_store_source_type_device_report_identifier` | `apple_store_app_store_source_type_device_report_identifier`
`app_store_territory_source_type_report_identifier` | `apple_store_app_store_territory_source_type_report_identifier`
`crashes_app_version_device_report_identifier` | `apple_store_crashes_app_version_device_report_identifier`
`crashes_platform_version_device_report_identifier` | `apple_store_crashes_platform_version_device_report_identifier`
`downloads_platform_version_source_type_report_identifier` | `apple_store_downloads_platform_version_source_type_report_identifier`
`downloads_source_type_device_report_identifier` | `apple_store_downloads_source_type_device_report_identifier`
`downloads_territory_source_type_report_identifier` | `apple_store_downloads_territory_source_type_report_identifier`
`sales_account_identifier` | `apple_store_sales_account_identifier`
`sales_subscription_event_summary_identifier` | `apple_store_sales_subscription_event_summary_identifier`
`sales_subscription_summary_identifier` | `apple_store_sales_subscription_summary_identifier`
`usage_app_version_source_type_report_identifier` | `apple_store_usage_app_version_source_type_report_identifier`
`usage_platform_version_source_type_report_identifier` | `apple_store_usage_platform_version_source_type_report_identifier`
`usage_source_type_device_report_identifier` | `apple_store_usage_source_type_device_report_identifier`
`usage_territory_source_type_report_identifier` | `apple_store_usage_territory_source_type_report_identifier`

- If you are using the previous identifier, be sure to update to the current version!

## Feature update 🎉
- Unioning capability! This adds the ability to union source data from multiple apple_store connectors. Refer to the [README](https://github.com/fivetran/dbt_apple_store/blob/main/README.md#union-multiple-connectors) for more details.
- Added a `source_relation` column in each staging model for tracking the source of each record.
  - The `source_relation` column is also persisted from the staging models to the end models.

## Under the hood 🚘
- Added the `source_relation` column to necessary joins. 
- In the source package:
  - Updated tmp models to union source data using the `fivetran_utils.union_data` macro. 
  - Applied the `fivetran_utils.source_relation` macro in each staging model to determine the `source_relation`.
  - Updated tests to account for the new `source_relation` column.

# dbt_apple_store v0.3.2

[PR #18](https://github.com/fivetran/dbt_apple_store/pull/18) includes the following updates:
## Bug Fix
- Enhanced the `state` join condition in `apple_store__subscription_report`. The new condition will now check for null values correctly. Previously this was causing wrong metrics for countries that do not specify or require a state.

## Under the Hood
- Included auto-releaser GitHub Actions workflow to automate future releases.
- Updated the maintainer PR template to resemble the most up to date format.

## Contributors
- [@awoehrl](https://github.com/awoehrl) ([PR #18](https://github.com/fivetran/dbt_apple_store/pull/18))

# dbt_apple_store v0.3.1

This package version includes the following updates:
## Bug Fix
- Shortened the field description for `source_type`. This was causing an error if the persist docs config was enabled because the description size exceeded warehouse constraints. This was updated upstream in the dbt_apple_store_source package ([PR #11](https://github.com/fivetran/dbt_apple_store_source/pull/11))

## Under the Hood:
- Added rows to seed data `app_store_territory_source_type` to test for countries with variant spellings in the  `territory` column ([PR #13](https://github.com/fivetran/dbt_apple_store/pull/13))
- Removed/added fields in the yml file ([PR #14](https://github.com/fivetran/dbt_apple_store/pull/14))

# dbt_apple_store v0.3.0

## Bug Fixes
[PR #11](https://github.com/fivetran/dbt_apple_store/pull/11) includes the following changes:
- This version of the transform package points to a [breaking change in the source package](https://github.com/fivetran/dbt_apple_store_source/blob/main/CHANGELOG.md) in which the [country code](https://github.com/fivetran/dbt_apple_store_source/blob/main/seeds/apple_store_country_codes.csv) mapping table was updated to align with Apple's [format and inclusion list](https://developer.apple.com/help/app-store-connect/reference/app-store-localizations/) of country names.
  - This is a 🚨**breaking change**🚨 as you will need to re-seed (`dbt seed --full-refresh`) the `apple_store_country_codes` file again.

## Under the Hood:
[PR #10](https://github.com/fivetran/dbt_apple_store/pull/10) includes the following changes:
- Incorporated the new `fivetran_utils.drop_schemas_automation` macro into the end of each Buildkite integration test job.
- Updated the pull request [templates](/.github).

# dbt_apple_store v0.2.0

## 🚨 Breaking Changes 🚨:
[PR #6](https://github.com/fivetran/dbt_apple_store/pull/6) includes the following breaking changes:
- Dispatch update for dbt-utils to dbt-core cross-db macros migration. Specifically `{{ dbt_utils.<macro> }}` have been updated to `{{ dbt.<macro> }}` for the below macros:
    - `any_value`
    - `bool_or`
    - `cast_bool_to_text`
    - `concat`
    - `date_trunc`
    - `dateadd`
    - `datediff`
    - `escape_single_quotes`
    - `except`
    - `hash`
    - `intersect`
    - `last_day`
    - `length`
    - `listagg`
    - `position`
    - `replace`
    - `right`
    - `safe_cast`
    - `split_part`
    - `string_literal`
    - `type_bigint`
    - `type_float`
    - `type_int`
    - `type_numeric`
    - `type_string`
    - `type_timestamp`
    - `array_append`
    - `array_concat`
    - `array_construct`
- For `current_timestamp` and `current_timestamp_in_utc` macros, the dispatch AND the macro names have been updated to the below, respectively:
    - `dbt.current_timestamp_backcompat`
    - `dbt.current_timestamp_in_utc_backcompat`
- `packages.yml` has been updated to reflect new default `fivetran/fivetran_utils` version, previously `[">=0.3.0", "<0.4.0"]` now `[">=0.4.0", "<0.5.0"]`.

# dbt_apple_store v0.1.0

## Initial Release
This is the initial release of this package. 

__What does this dbt package do?__
- Produces modeled tables that leverage Apple App Store data from [Fivetran's connector](https://fivetran.com/docs/applications/apple-app-store) in the format described by [this ERD](https://docs.google.com/presentation/d/1zeV9F1yakOQbgx-L0xQ7h8I3KRuJL_tKc7srX_ctaYw/edit?usp=sharing) and builds off the output of our [Apple App Store source package](https://github.com/fivetran/dbt_apple_store_source).
- The above mentioned models enable you to better understand your Apple App Store metrics at different granularities. It achieves this by:
  - Providing intuitive reporting at the App Version, Platform Version, Device, Source Type, Territory, Subscription and Overview levels
  - Aggregates all relevant application metrics into each of the reporting levels above
- Generates a comprehensive data dictionary of your source and modeled Apple App Store data via the [dbt docs site](https://fivetran.github.io/dbt_apple_store/)

For more information refer to the [README](/README.md).
