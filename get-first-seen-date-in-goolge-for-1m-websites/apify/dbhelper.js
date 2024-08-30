import mysql from 'mysql2/promise';
import { D1Client } from '@cloudflare/d1';

// Configuration for MySQL and D1
const mysqlConfig = {
    host: 'gateway01.us-west-2.prod.aws.tidbcloud.com',
    port: 4000,
    user: '3i7meP2hYPkDk3V.root',
    password: 'xxxxxxxxxxxxx',
    database: 'test',
    ssl: {
        ca: './isrgrootx1.pem',
        verify: true,
        rejectUnauthorized: true
    }
};

const d1Config = {
    connectionString: 'https://YOUR_ACCOUNT_NAME.cloudflareaccess.com/d1/YOUR_DATABASE_NAME' // Update with your Cloudflare D1 credentials
};

const d1Client = new D1Client(d1Config);

// MySQL Database helper functions
export async function queryMySQL(query, params = []) {
    const connection = await mysql.createConnection(mysqlConfig);
    try {
        const [rows] = await connection.execute(query, params);
        return rows;
    } finally {
        await connection.end();
    }
}

// D1 Database helper functions
export async function queryD1(query, params = []) {
    try {
        const [rows] = await d1Client.execute(query, params);
        return rows;
    } catch (error) {
        console.error('D1 query error:', error);
        throw error;
    }
}

// Common functions for both databases
export async function getUndoneDomains(database) {
    const query = 'SELECT domain FROM domains WHERE title IS NULL OR title = ""';
    if (database === 'd1') {
        return queryD1(query);
    } else {
        return queryMySQL(query);
    }
}

export async function insertOrUpdateData(database, data) {
    const { domain, indexdate, Aboutthesource, Intheirownwords } = data;

    const checkQuery = 'SELECT indexdate, Aboutthesource, Intheirownwords FROM domain_index_data WHERE domain = ?';
    const insertQuery = `
        INSERT INTO domain_index_data (domain, indexdate, Aboutthesource, Intheirownwords)
        VALUES (?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE indexdate = VALUES(indexdate), Aboutthesource = VALUES(Aboutthesource), Intheirownwords = VALUES(Intheirownwords)
    `;

    if (database === 'd1') {
        const existingRecord = await queryD1(checkQuery, [domain]);
        await queryD1(insertQuery, [domain, indexdate, Aboutthesource, Intheirownwords]);
    } else {
        const existingRecord = await queryMySQL(checkQuery, [domain]);
        await queryMySQL(insertQuery, [domain, indexdate, Aboutthesource, Intheirownwords]);
    }
}
