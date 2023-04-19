const express = require('express');
const bodyParser = require('body-parser');
const { Client } = require('pg');
const dotenv = require('dotenv');
const { convertCsvToJson } = require('./helper');

// Load environment variables from .env file
dotenv.config();

// Create a new Express app
const app = express();

// Configure body-parser middleware
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

const client = new Client({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,
    port: process.env.DB_PORT,
});

try {
    client.connect().then(() => {
        console.log('Connected');
    });

} catch (error) {
    throw error;
}

const tableName = 'users';

const createTableQuery = `
    CREATE TABLE IF NOT EXISTS ${tableName} (
        "name" varchar NOT NULL,
        age int4 NOT NULL,
        address jsonb NULL,
        additional_info jsonb NULL,
        id serial4 NOT NULL
    );
`;

const selectAgeDistributionQuery = `
    SELECT 
        ROUND((count(*) FILTER (WHERE age < 20)::numeric / count(*) * 100), 2) AS percent_under_20,
        ROUND((count(*) FILTER (WHERE age BETWEEN 20 AND 39)::numeric / count(*) * 100), 2) AS percent_20_to_39,
        ROUND((count(*) FILTER (WHERE age BETWEEN 40 AND 59)::numeric / count(*) * 100), 2) AS percent_40_to_59,
        ROUND((count(*) FILTER (WHERE age >= 60)::numeric / count(*) * 100), 2) AS percent_over_60
    FROM 
        ${tableName};
`;

app.get('/', async (req, res) => {
    try {
        const objRes = await convertCsvToJson('user.csv');
        await client.query(createTableQuery); // Create Table if not exist
        for (const obj of objRes) {
            let { name, age, address = {}, ...objAdditionalInfo } = obj;
            const strName = name.firstName + ' ' + name.lastName;
            const query = {
                text: 'INSERT INTO users (name, age, address, additional_info) VALUES ($1, $2, $3, $4)',
                values: [strName, age, address, objAdditionalInfo],
            };
            await client.query(query);
        }
        const objAgeDistributionRes = await client.query(selectAgeDistributionQuery);
        console.log(objAgeDistributionRes.rows); // Console log Age Distribution
        res.json({ ageDistribution: objAgeDistributionRes.rows, csvToJson: objRes });
    } catch (error) {
        console.error(error);
        res.status(500).json({ message: 'Something went wrong' });
    }
});

// Start the server
app.listen(process.env.PORT, () => {
    console.log(`Server started on port ${process.env.PORT}`);
});
