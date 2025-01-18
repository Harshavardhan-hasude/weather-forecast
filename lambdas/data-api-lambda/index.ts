import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import * as path from 'path';
import * as parquet from 'parquetjs-lite';
import { Readable } from 'stream';

const s3Client = new S3Client({});

export const handler = async (event: any): Promise<any> => {
    const bucketName = process.env.BUCKET_NAME as string;

    const queryParams = event.queryStringParameters || {};
    const city = queryParams.city;
    const startDate = queryParams.start_date;
    const endDate = queryParams.end_date;

    if (!city || !startDate || !endDate) {
        return response(400, { error: 'Missing city, start_date, or end_date' });
    }

    // Convert dates
    let start: Date, end: Date;
    try {
        start = new Date(startDate);
        end = new Date(endDate);
    } catch (err) {
        return response(400, { error: `Invalid date format: ${err}` });
    }

    // Build a list of date strings for partitions
    const dateRange = [];
    for (let dt = new Date(start); dt <= end; dt.setDate(dt.getDate() + 1)) {
        dateRange.push(dt.toISOString().split('T')[0]); // "YYYY-MM-DD"
    }

    const results: any[] = [];

    for (const dateStr of dateRange) {
        const key = `city=${city}/date=${dateStr}/forecast.parquet`;

        try {
            const getObjectRes = await s3Client.send(new GetObjectCommand({
                Bucket: bucketName,
                Key: key
            }));
            const stream = getObjectRes.Body as Readable;

            // Read parquet from the stream
            const chunks: Buffer[] = [];
            for await (const chunk of stream) {
                chunks.push(chunk as Buffer);
            }

            const buffer = Buffer.concat(chunks);
            const reader = await parquet.ParquetReader.openBuffer(buffer);
            const cursor = reader.getCursor();
            let record: any;
            while ((record = await cursor.next())) {
                // Filter by start/end date/time
                const recordDt = new Date(record.datetime);
                if (recordDt >= start && recordDt <= end) {
                    results.push(record);
                }
            }
            await reader.close();
        } catch (err) {
            // 404 or missing partition is expected sometimes
            console.info(`No data found for city=${city} date=${dateStr}:`, err);
        }
    }
    console.info(`Data returned is`, results);
    return response(200, { data: results });
};

function response(statusCode: number, body: any) {
    return {
        statusCode,
        body: JSON.stringify(body, (_, value) =>
            typeof value === 'bigint' ? value.toString() : value
        ),
        headers: {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        }
    };
}
