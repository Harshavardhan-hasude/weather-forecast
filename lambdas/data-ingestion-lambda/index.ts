import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import axios from 'axios';
import * as path from 'path';
import * as parquet from 'parquetjs-lite';

const s3Client = new S3Client({});

export const handler = async (): Promise<any> => {
    const bucketName = process.env.BUCKET_NAME as string;
    const apiKey = process.env.API_KEY as string;
    const majorCities = [
        "Mumbai", "London", "New York", "Tokyo", "Paris", "Berlin",
        "Sydney", "Toronto", "Dubai", "Singapore"
    ];

    const records: any[] = [];

    for (const city of majorCities) {
        try {
            // 3-day forecast using WeatherAPI
            const weatherUrl = `http://api.weatherapi.com/v1/forecast.json?key=${apiKey}&q=${city}&days=3`;
            const res = await axios.get(weatherUrl, { timeout: 10_000 });
            const data = res.data;

            // WeatherAPI returns data in forecast.forecastday
            if (!data.forecast || !data.forecast.forecastday) {
                console.warn(`No forecast data for city=${city}`);
                continue;
            }

            for (const dayInfo of data.forecast.forecastday) {
                // dayInfo.date, dayInfo.hour[] => array of 24 hourly objects
                if (!dayInfo.hour) continue;

                for (const hourData of dayInfo.hour) {
                    // Example fields from WeatherAPI
                    const record = {
                        city: city,
                        datetime: hourData.time,      // e.g. "2025-01-01 00:00"
                        temp: hourData.temp_c,        // Celsius
                        weather: hourData.condition?.text || "N/A",
                        humidity: hourData.humidity,
                        wind_speed: hourData.wind_kph // in kph
                    };
                    records.push(record);
                }
            }
        } catch (error) {
            console.error(`Error fetching data for city=${city}:`, error);
        }
    }

    if (records.length === 0) {
        console.info('No records to process.');
        return { status: 'No data' };
    }
        
    // PARTITION & PARQUET WRITE
    // Group data by city and date to create partitioned S3 objects
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

    // Group by city and date (YYYY-MM-DD derived from datetime)
    const grouped: { [key: string]: WeatherRecord[] } = {};

    records.forEach((rec) => {        
        const datePart = rec.datetime.split(' ')[0]; // "YYYY-MM-DD" from "YYYY-MM-DD HH:mm"
        const cityDateKey = `${rec.city}#${datePart}`;

        if (!grouped[cityDateKey]) {
            grouped[cityDateKey] = [];
        }
        grouped[cityDateKey].push(rec);
    });    

    for (const cityDateKey in grouped) {
        const [city, date] = cityDateKey.split('#');
        const groupRecords = grouped[cityDateKey];

        // Create a new Parquet writer for each partition
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
                    Body: fileData,
                })
            );
            console.info(`Uploaded Parquet file for city=${city}, date=${date} -> s3://${bucketName}/${key}`);
        } catch (err) {
            console.error(`Error uploading to S3 for ${city}, date=${date}:`, err);
        }
    }

    return { status: 'Data Ingestion Complete' };
};
