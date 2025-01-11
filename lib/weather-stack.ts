import * as cdk from 'aws-cdk-lib';
import {
    Stack,
    StackProps,
    Duration
} from 'aws-cdk-lib';
import { Construct } from 'constructs';

import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as apigw from 'aws-cdk-lib/aws-apigateway';

export class WeatherStack extends Stack {
    constructor(scope: Construct, id: string, props?: StackProps) {
        super(scope, id, props);

        // 1. S3 bucket for storing Parquet data
        const dataBucket = new s3.Bucket(this, 'WeatherDataBucket', {
            removalPolicy: cdk.RemovalPolicy.DESTROY,
            autoDeleteObjects: true,
            // Optionally consider versioning for data change tracking
            versioned: false
        });

        // 2. IAM Role for Lambdas
        const lambdaRole = new iam.Role(this, 'WeatherLambdaRole', {
            assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com')
        });

        lambdaRole.addManagedPolicy(
            iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole')
        );
        dataBucket.grantReadWrite(lambdaRole);

        // 3. Data Ingestion Lambda
        const ingestionLambda = new lambda.Function(this, 'DataIngestionLambda', {
            runtime: lambda.Runtime.NODEJS_18_X,
            code: lambda.Code.fromAsset('lambdas/data-ingestion-lambda'),
            handler: 'index.handler',
            role: lambdaRole,
            timeout: Duration.seconds(300),
            environment: {
                BUCKET_NAME: dataBucket.bucketName,
                API_KEY: 'dd318793330d49fea91192456251101' // Free version
            }
        });

        // 4. Schedule ingestion every 3 hours using EventBridge
        new events.Rule(this, 'IngestionScheduleRule', {
            schedule: events.Schedule.rate(Duration.hours(3)),
            targets: [new targets.LambdaFunction(ingestionLambda)]
        });

        // 5. Data API Lambda
        const apiLambda = new lambda.Function(this, 'DataApiLambda', {
            runtime: lambda.Runtime.NODEJS_18_X,
            code: lambda.Code.fromAsset('lambdas/data-api-lambda'),
            handler: 'index.handler',
            role: lambdaRole,
            timeout: Duration.seconds(30),
            environment: {
                BUCKET_NAME: dataBucket.bucketName
            }
        });

        // 6. API Gateway
        // Create a REST API with an API key requirement and caching
        const restApi = new apigw.RestApi(this, 'WeatherApi', {
            restApiName: 'weather-api',
            description: 'REST API for weather data queries'
        });

        // Enable API Key
        const plan = restApi.addUsagePlan('UsagePlan', {
            name: 'WeatherUsagePlan',
            throttle: {
                rateLimit: 10,
                burstLimit: 20
            }
        });

        const apiKey = restApi.addApiKey('ApiKey', {
            apiKeyName: 'WeatherApiKey'
        });

        // Create "weather" resource
        const weatherResource = restApi.root.addResource('weather');

        // API Gateway integration with the data API lambda
        const lambdaIntegration = new apigw.LambdaIntegration(apiLambda, {
            requestTemplates: { 'application/json': `{"statusCode": "200"}` }
        });

        // Method-level configuration to require API key
        const getWeatherMethod = weatherResource.addMethod('GET', lambdaIntegration, {
            apiKeyRequired: true
        });

        plan.addApiKey(apiKey);
        plan.addApiStage({
            stage: restApi.deploymentStage,
            throttle: [
                {
                    method: getWeatherMethod,
                    throttle: {
                        rateLimit: 10,
                        burstLimit: 20
                    }
                }
            ]
        });

    }
}
