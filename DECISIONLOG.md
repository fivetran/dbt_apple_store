# Decision Log

In creating this package, which is meant for a wide range of use cases, we had to take opinionated stances on a few different questions we came across during development. We've consolidated significant choices we made here, and will continue to update as the package evolves. 

## Not including `active_devices_last_30_days` as a field
We chose not to include this metric in the end reporting models because we create them from daily tables directly from the Apple App Store. Therefore we do not have insight into how to de-duplicate counts that would ensure devices don't get accounted for more than once over 30 days. However, if you would like to see this field supported in the future, feel free to comment or follow this respective [Github thread](https://github.com/fivetran/dbt_apple_store/issues/33).

## Subscriptions Report
This model will **not** tie out to the Apple UI's Subscriptions as there currently isn't a clear way to map the current subscription events to how Apple calculates and group their events together. [(source)](https://help.apple.com/app-store-connect/#/itc484ef82a0)