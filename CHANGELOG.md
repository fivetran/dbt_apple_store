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