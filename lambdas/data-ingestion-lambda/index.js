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
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.handler = void 0;
const client_s3_1 = require("@aws-sdk/client-s3");
const axios_1 = __importDefault(require("axios"));
const parquet = __importStar(require("parquetjs-lite"));
const s3Client = new client_s3_1.S3Client({});
const handler = () => __awaiter(void 0, void 0, void 0, function* () {
    var _a;
    const bucketName = process.env.BUCKET_NAME;
    const apiKey = process.env.API_KEY;
    const majorCities = [
        "Mumbai", "London", "New York", "Tokyo", "Paris", "Berlin",
        "Sydney", "Toronto", "Dubai", "Singapore"
    ];
    const records = [];
    for (const city of majorCities) {
        try {
            // 3-day forecast using WeatherAPI
            const weatherUrl = `http://api.weatherapi.com/v1/forecast.json?key=${apiKey}&q=${city}&days=3`;
            const res = yield axios_1.default.get(weatherUrl, { timeout: 10000 });
            const data = res.data;
            // WeatherAPI returns data in forecast.forecastday
            if (!data.forecast || !data.forecast.forecastday) {
                console.warn(`No forecast data for city=${city}`);
                continue;
            }
            for (const dayInfo of data.forecast.forecastday) {
                // dayInfo.date, dayInfo.hour[] => array of 24 hourly objects
                if (!dayInfo.hour)
                    continue;
                for (const hourData of dayInfo.hour) {
                    // Example fields from WeatherAPI
                    const record = {
                        city: city,
                        datetime: hourData.time, // e.g. "2025-01-01 00:00"
                        temp: hourData.temp_c, // Celsius
                        weather: ((_a = hourData.condition) === null || _a === void 0 ? void 0 : _a.text) || "N/A",
                        humidity: hourData.humidity,
                        wind_speed: hourData.wind_kph // in kph
                    };
                    records.push(record);
                }
            }
        }
        catch (error) {
            console.error(`Error fetching data for city=${city}:`, error);
        }
    }
    if (records.length === 0) {
        console.info('No records to process.');
        return { status: 'No data' };
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
    const grouped = {};
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
        const writer = yield parquet.ParquetWriter.openFile(schema, `/tmp/${fileName}`);
        // Write data
        for (const item of groupRecords) {
            yield writer.appendRow({
                datetime: item.datetime,
                temp: item.temp,
                weather: item.weather,
                humidity: item.humidity,
                wind_speed: item.wind_speed
            });
        }
        yield writer.close();
        // Upload to S3
        const key = `city=${city}/date=${date}/${fileName}`;
        const fs = require('fs');
        const fileData = fs.readFileSync(`/tmp/${fileName}`);
        try {
            yield s3Client.send(new client_s3_1.PutObjectCommand({
                Bucket: bucketName,
                Key: key,
                Body: fileData,
            }));
            console.info(`Uploaded Parquet file for city=${city}, date=${date} -> s3://${bucketName}/${key}`);
        }
        catch (err) {
            console.error(`Error uploading to S3 for ${city}, date=${date}:`, err);
        }
    }
    return { status: 'Data Ingestion Complete' };
});
exports.handler = handler;
