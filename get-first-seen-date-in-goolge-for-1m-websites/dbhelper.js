// dbhelper.js

import mysql from 'mysql2/promise';
import { connect } from '@tidbcloud/serverless'; // Ensure you have this installed if using TiDB

// Configuration for MySQL and D1
const mysqlConfig = {
    host: 'gateway01.us-west-2.prod.aws.tidbcloud.com',
    port: 4000,
    user: '3i7meP2hYPkDk3V.root',
    password: 'xxxxxxxxxxxxxxxxxxxxx',
    database: 'test',
    ssl: {
        ca: './isrgrootx1.pem',
        verify: true,
        rejectUnauthorized: true
    }
};

const d1Config = {
    url: 'mysql://[username]:[password]@[host]/[database]', // Update with your D1 credentials
};

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
    const conn = connect(d1Config);
    try {
        const [rows] = await conn.execute(query, params);
        return rows;
    } finally {
        conn.end();
    }
}

// Common functions for both databases
export async function getUndoneDomains(database) {
    let query = 'SELECT domain FROM domains WHERE title IS NULL OR title = ""';
    return database === 'd1' ? await queryD1(query) : await queryMySQL(query);
}

export async function insertOrUpdateData(database, data) {
    const { domain, indexdate, Aboutthesource, Intheirownwords } = data;

    let checkQuery = 'SELECT indexdate, Aboutthesource, Intheirownwords FROM domain_index_data WHERE domain = ?';
    let insertQuery = `
        INSERT INTO domain_index_data (domain, indexdate, Aboutthesource, Intheirownwords)
        VALUES (?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE indexdate = VALUES(indexdate), Aboutthesource = VALUES(Aboutthesource), Intheirownwords = VALUES(Intheirownwords)
    `;

    if (database === 'd1') {
        let existingRecord = await queryD1(checkQuery, [domain]);
        if (existingRecord.length > 0) {
            // Update if record exists
            await queryD1(insertQuery, [domain, indexdate, Aboutthesource, Intheirownwords]);
        } else {
            // Insert new record
            await queryD1(insertQuery, [domain, indexdate, Aboutthesource, Intheirownwords]);
        }
    } else {
        let existingRecord = await queryMySQL(checkQuery, [domain]);
        if (existingRecord.length > 0) {
            // Update if record exists
            await queryMySQL(insertQuery, [domain, indexdate, Aboutthesource, Intheirownwords]);
        } else {
            // Insert new record
            await queryMySQL(insertQuery, [domain, indexdate, Aboutthesource, Intheirownwords]);
        }
    }
}
