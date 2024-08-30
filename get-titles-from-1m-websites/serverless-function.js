// serverless-function.js
const { connect } = require('@tidbcloud/serverless');

const conn = connect({url: 'mysql://[username]:[password]@[host]/[database]'});

exports.handler = async (event) => {
    let response;
    try {
        const query = event.queryStringParameters.query;
        const [results] = await conn.execute(query);
        response = {
            statusCode: 200,
            body: JSON.stringify({ results })
        };
    } catch (error) {
        response = {
            statusCode: 500,
            body: JSON.stringify({ error: error.message })
        };
    }
    return response;
};
