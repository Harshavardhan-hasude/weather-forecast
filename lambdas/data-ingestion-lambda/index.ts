import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import axios from 'axios';
import * as path from 'path';
import * as parquet from 'parquetjs-lite';

const s3Client = new S3Client({});

export const handler = async (): Promise<any> => {
    const bucketName = process.env.BUCKET_NAME as string;
    const apiKey = process.env.API_KEY as string;
    const majorCities = [
        "Kolkata", "London", "New York", "Tokyo", "Paris", "Berlin",
        "Sydney", "Toronto", "Dubai", "Singapore"
    ];

    const records: any[] = [];

    for (const city of majorCities) {
        try {
            const res = await axios.get('http://api.openweathermap.org/data/2.5/forecast', {
                params: {
                    q: city,
                    appid: apiKey,
                    units: 'metric'
                },
                timeout: 10_000
            });
            const data = res.data;

            for (const forecastItem of data.list || []) {
                records.push({
                    city: city,
                    datetime: forecastItem.dt_txt,
                    temp: forecastItem.main.temp,
                    weather: forecastItem.weather[0].description,
                    humidity: forecastItem.main.humidity,
                    wind_speed: forecastItem.wind.speed
                });
            }
        } catch (error) {
            console.error(`Error fetching data for ${city}:`, error);
        }
    }

    if (records.length === 0) {
        console.info('No records to process.');
        return { status: 'No data' };
    }

    // Partition logic
    // Extract date from the 'datetime' field
    interface WeatherRecord {
        city: string;
        datetime: string;
        temp: number;
        weather: string;
        humidity: number;
        wind_speed: number;
        date?: string;
    }

    // Build a Parquet schema
    const schema = new parquet.ParquetSchema({
        datetime: { type: 'UTF8' },
        temp: { type: 'DOUBLE' },
        weather: { type: 'UTF8' },
        humidity: { type: 'INT64' },
        wind_speed: { type: 'DOUBLE' }
    });

    // Group by city and date
    const grouped: { [key: string]: WeatherRecord[] } = {};

    records.forEach((rec) => {
        const dateStr = rec.datetime.split(' ')[0]; // "YYYY-MM-DD" from "YYYY-MM-DD HH:mm:ss"
        const cityDateKey = `${rec.city}#${dateStr}`;

        if (!grouped[cityDateKey]) {
            grouped[cityDateKey] = [];
        }
        grouped[cityDateKey].push(rec);
    });

    for (const cityDateKey in grouped) {
        const [city, date] = cityDateKey.split('#');
        const groupRecords = grouped[cityDateKey];

        // Create a new Parquet writer
        const fileName = 'forecast.parquet';
        const writer = await parquet.ParquetWriter.openFile(schema, `/tmp/${fileName}`);

        // Write data
        for (const item of groupRecords) {
            await writer.appendRow({
                datetime: item.datetime,
                temp: item.temp,
                weather: item.weather,
                humidity: item.humidity,
                wind_speed: item.wind_speed
            });
        }

        await writer.close();

        // Upload to S3
        const key = `city=${city}/date=${date}/${fileName}`;
        const fs = require('fs');
        const fileData = fs.readFileSync(`/tmp/${fileName}`);

        try {
            await s3Client.send(
                new PutObjectCommand({
                    Bucket: bucketName,
                    Key: key,
                    Body: fileData
                })
            );
            console.info(`Uploaded Parquet file for ${city}, ${date} -> s3://${bucketName}/${key}`);
        } catch (err) {
            console.error(`Error uploading to S3 for ${city}, ${date}:`, err);
        }
    }

    return { status: 'Data Ingestion Complete' };
};
