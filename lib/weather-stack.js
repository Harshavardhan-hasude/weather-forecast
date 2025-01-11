"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.WeatherStack = void 0;
const cdk = __importStar(require("aws-cdk-lib"));
const aws_cdk_lib_1 = require("aws-cdk-lib");
const lambda = __importStar(require("aws-cdk-lib/aws-lambda"));
const iam = __importStar(require("aws-cdk-lib/aws-iam"));
const s3 = __importStar(require("aws-cdk-lib/aws-s3"));
const events = __importStar(require("aws-cdk-lib/aws-events"));
const targets = __importStar(require("aws-cdk-lib/aws-events-targets"));
const apigw = __importStar(require("aws-cdk-lib/aws-apigateway"));
class WeatherStack extends aws_cdk_lib_1.Stack {
    constructor(scope, id, props) {
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
        lambdaRole.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'));
        dataBucket.grantReadWrite(lambdaRole);
        // 3. Data Ingestion Lambda
        const ingestionLambda = new lambda.Function(this, 'DataIngestionLambda', {
            runtime: lambda.Runtime.NODEJS_18_X,
            code: lambda.Code.fromAsset('lambdas/data-ingestion-lambda'),
            handler: 'index.handler',
            role: lambdaRole,
            timeout: aws_cdk_lib_1.Duration.seconds(300),
            environment: {
                BUCKET_NAME: dataBucket.bucketName,
                API_KEY: 'dd318793330d49fea91192456251101' // Free version
            }
        });
        // 4. Schedule ingestion every 3 hours using EventBridge
        new events.Rule(this, 'IngestionScheduleRule', {
            schedule: events.Schedule.rate(aws_cdk_lib_1.Duration.hours(3)),
            targets: [new targets.LambdaFunction(ingestionLambda)]
        });
        // 5. Data API Lambda
        const apiLambda = new lambda.Function(this, 'DataApiLambda', {
            runtime: lambda.Runtime.NODEJS_18_X,
            code: lambda.Code.fromAsset('lambdas/data-api-lambda'),
            handler: 'index.handler',
            role: lambdaRole,
            timeout: aws_cdk_lib_1.Duration.seconds(30),
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
exports.WeatherStack = WeatherStack;
