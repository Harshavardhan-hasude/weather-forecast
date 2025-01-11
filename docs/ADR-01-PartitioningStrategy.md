# ADR-01: Partitioning Strategy for Weather Data

## Context

We are storing forecast data for multiple cities over time. Efficient querying requires partitioning by city and date.

## Decision

- Partition in S3 with `city=CityName/date=YYYY-MM-DD/forecast.parquet`.
- This is straightforward and compatible with AWS analytics services (Athena, Glue).

## Consequences

- Easy to retrieve data by city and date range from S3.