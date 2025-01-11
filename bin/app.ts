import * as cdk from 'aws-cdk-lib';
import { WeatherStack } from '../lib/weather-stack';

const app = new cdk.App();

new WeatherStack(app, 'WeatherStack', {
    env: {
        account: process.env.CDK_DEFAULT_ACCOUNT,
        region: process.env.CDK_DEFAULT_REGION
    }
});

app.synth();