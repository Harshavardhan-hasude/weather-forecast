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
var __asyncValues = (this && this.__asyncValues) || function (o) {
    if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
    var m = o[Symbol.asyncIterator], i;
    return m ? m.call(o) : (o = typeof __values === "function" ? __values(o) : o[Symbol.iterator](), i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i);
    function verb(n) { i[n] = o[n] && function (v) { return new Promise(function (resolve, reject) { v = o[n](v), settle(resolve, reject, v.done, v.value); }); }; }
    function settle(resolve, reject, d, v) { Promise.resolve(v).then(function(v) { resolve({ value: v, done: d }); }, reject); }
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.handler = void 0;
const client_s3_1 = require("@aws-sdk/client-s3");
const parquet = __importStar(require("parquetjs-lite"));
const s3Client = new client_s3_1.S3Client({});
const handler = (event) => __awaiter(void 0, void 0, void 0, function* () {
    var _a, e_1, _b, _c;
    const bucketName = process.env.BUCKET_NAME;
    const queryParams = event.queryStringParameters || {};
    const city = queryParams.city;
    const startDate = queryParams.start_date;
    const endDate = queryParams.end_date;
    if (!city || !startDate || !endDate) {
        return response(400, { error: 'Missing city, start_date, or end_date' });
    }
    // Convert dates
    let start, end;
    try {
        start = new Date(startDate);
        end = new Date(endDate);
    }
    catch (err) {
        return response(400, { error: `Invalid date format: ${err}` });
    }
    // Build a list of date strings for partitions
    const dateRange = [];
    for (let dt = new Date(start); dt <= end; dt.setDate(dt.getDate() + 1)) {
        dateRange.push(dt.toISOString().split('T')[0]); // "YYYY-MM-DD"
    }
    const results = [];
    for (const dateStr of dateRange) {
        const key = `city=${city}/date=${dateStr}/forecast.parquet`;
        try {
            const getObjectRes = yield s3Client.send(new client_s3_1.GetObjectCommand({
                Bucket: bucketName,
                Key: key
            }));
            const stream = getObjectRes.Body;
            // Read parquet from the stream
            // We need to buffer the Parquet file in memory or use a streaming approach
            const chunks = [];
            try {
                for (var _d = true, stream_1 = (e_1 = void 0, __asyncValues(stream)), stream_1_1; stream_1_1 = yield stream_1.next(), _a = stream_1_1.done, !_a; _d = true) {
                    _c = stream_1_1.value;
                    _d = false;
                    const chunk = _c;
                    chunks.push(chunk);
                }
            }
            catch (e_1_1) { e_1 = { error: e_1_1 }; }
            finally {
                try {
                    if (!_d && !_a && (_b = stream_1.return)) yield _b.call(stream_1);
                }
                finally { if (e_1) throw e_1.error; }
            }
            const buffer = Buffer.concat(chunks);
            const reader = yield parquet.ParquetReader.openBuffer(buffer);
            const cursor = reader.getCursor();
            let record;
            while ((record = yield cursor.next())) {
                // Filter by start/end date/time
                const recordDt = new Date(record.datetime);
                if (recordDt >= start && recordDt <= end) {
                    results.push(record);
                }
            }
            yield reader.close();
        }
        catch (err) {
            // 404 or missing partition is expected sometimes
            console.info(`No data found for city=${city} date=${dateStr}:`, err);
        }
    }
    return response(200, { data: results });
});
exports.handler = handler;
function response(statusCode, body) {
    return {
        statusCode,
        body: JSON.stringify(body),
        headers: {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        }
    };
}
