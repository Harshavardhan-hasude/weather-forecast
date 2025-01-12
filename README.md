# Weather Forecast Data Pipeline (Serverless) - Using WeatherAPI

A **serverless application** that periodically fetches **weather forecast** data for major cities, stores it in **Parquet** format on S3 (partitioned by city and date), and exposes a **REST API** for querying the data by city and date range. This solution uses **AWS CDK** for infrastructure as code, **AWS Lambda** for ingestion and querying, and **Amazon API Gateway** for a public REST interface.

---

## 1. Project Overview

1. **Ingest** weather forecasts for 10 cities (Mumbai, London, New York, Tokyo, Paris, Berlin, Sydney, Toronto, Dubai and Singapore) using [WeatherAPI](https://www.weatherapi.com/).
2. **Transform** & store data in **Parquet** format on **Amazon S3**, partitioned by `city=${city}, date=${date}`.
3. **Query** the data via an **API Gateway** → **Lambda** setup, returning JSON by city and date range.
4. **Automate** this process on a scheduled basis (every 3 hours) using **EventBridge**.

**Project Information**

- bin/app.ts: Entry point for the CDK application.
- lib/weather-stack.ts: Contains the primary CDK stack.
- lambdas/\*: TypeScript Lambda handlers.
- docs/ADR-\*: Architectural Decision Records.
- .github/workflows/\*: GitHub Actions CI/CD pipeline.

**Why WeatherAPI?**

- WeatherAPI offers a **free tier** that allows up to 3 days forecast per call.
- You need to register for a free API key on [weatherapi.com](https://www.weatherapi.com/).

---

## 2. Architecture

- **EventBridge**: Schedules the ingestion Lambda every 3 hours.
- **Data Ingestion Lambda**: Fetches up to 3 days forecast from WeatherAPI for each city, stores as Parquet in S3.
- **Data API Lambda**: Reads from S3 (using the city/date partitions), filters by date/time, returns JSON.
- **API Gateway**: Fronts the Data API Lambda. Requires an **API Key** for access.

---

## 3. Features

1. **Serverless**: No servers to manage—AWS handles scaling.
2. **Parquet**: Efficient, columnar storage for weather data.
3. **Partitioning**: Makes it easy to retrieve data by city and date.
4. **Scheduled**: Automatic data refresh every 3 hours via EventBridge.
5. **Authentication**: Simple API key mechanism for the query API.
6. **IaC**: Entire stack defined using AWS CDK (TypeScript).

---

## 4. Prerequisites

1. **Node.js** >= 16
2. **AWS CDK** (installed globally)  
   `bash npm install -g aws-cdk`
3. **AWS Account** with sufficient privileges to create resources (S3, Lambda, API Gateway, etc.).
4. **AWS CLI** configured (run aws configure).
5. **Git** for version control.
6. **WeatherAPI Key** from [weatherapi.com](https://www.weatherapi.com/) (free plan).

## 5. Deployment to AWS

### Step 1: AWS Account & CLI

- Sign up / log in to AWS if you haven’t.
- Create an IAM user with admin or similar privileges.
- Run aws configure to store your credentials locally.

### Step 2: Install Dependencies

From the project root:

```bash
npm install
npm run build  # or npx tsc
```

### Step 3: Bootstrap (once, if not done before)

```bash
cdk bootstrap
```

This creates the necessary S3 buckets / roles for CDK in your AWS account.

### Step 4: Deploy

```bash
cdk deploy --require-approval never
```

The CDK will:

1. Create an S3 bucket for data.
2. Create Lambdas (ingestion, data API).
3. Set up an EventBridge rule (cron every 3 hours).
4. Create an API Gateway with a usage plan + API key.

### Step 5: Provide Your WeatherAPI Key

- In weather-stack.ts, you’ll see something like:

```ts
environment: {
 BUCKET_NAME: dataBucket.bucketName,
 API_KEY: 'YOUR_OPENWEATHERMAP_API_KEY' // used free version currently
}

```

**Replace** 'YOUR_OPENWEATHERMAP_API_KEY' with the actual key from WeatherAPI.

- **Re-deploy** if you changed the environment variable:

```bash
cdk deploy
```

### Step 6: Confirm

- After deployment, CDK outputs an API Endpoint.
- Copy/paste into a browser or Postman (see usage below).

## 6. Usage

### **API Endpoint**

CDK will provide an output like:

```bash
WeatherApi: https://<api-id>.execute-api.<region>.amazonaws.com/prod/weather
```

If you set up a resource called /weather, the final URL is something like:

```bash
GET https://<api-id>.execute-api.<region>.amazonaws.com/prod/weather
```

**Adjust** for your actual API ID/region.

### Query Parameters

- city: e.g. city=London
- start_date: e.g. 2025-01-01
- end_date: e.g. 2025-01-02

**Example** request in Postman / cURL:

```bash
GET https://abc123.execute-api.us-east-1.amazonaws.com/prod/weather?city=London&start_date=2025-01-01&end_date=2025-01-02
x-api-key: <YOUR_API_KEY>

GET https://3w98qftlka.execute-api.eu-north-1.amazonaws.com/prod/weather?city=London&start_date=2025-01-01&end_date=2025-01-02
x-api-key: yKDvZADK0x1j0ZwutxIhR8kPQwI0RFYY1fAEWEiG
```

### API Key Authentication

1. The stack sets up an API key + usage plan.
2. You must include:

```makefile
x-api-key: <YOUR_API_KEY>
```

in the request headers. 3. Retrieve the actual key from the **API Gateway** console or from the CDK logs/outputs (depending on how you configured it).

## 7. Cleaning Up

To avoid charges:

1. Remove the CloudFormation stack:

```bash
cdk destroy
```

2. This deletes the S3 bucket (and all stored data), Lambdas, API Gateway, etc.
3. Confirm in the AWS Console that resources are removed.

## 8. Replicating Another API Provider

- The key difference is in data-ingestion-lambda/index.ts.
- Change the API call (currently http://api.weatherapi.com/v1/forecast.json) to your preferred provider.
- Parse their JSON format into your record structure.
- The rest of the architecture (S3 partitions, data API) remains the same.

## 9. Contact

For questions or suggestions, please open an issue in this repository or contact [Harshavardhan] at [harshavardhan.deshmukh@outlook.com].
