// api/db-function.js
import { connect } from '@tidbcloud/serverless';

const conn = connect({url: 'mysql://[username]:[password]@[host]/[database]'});

export default async function handler(req, res) {
    const { query } = req.query;
    try {
        const [results] = await conn.execute(query);
        res.status(200).json({ results });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
}
