// worker.js

async function handleRequest(request) {
    const url = 'https://my-vercel-project.vercel.app/api/db-function';
    const query = 'SHOW TABLES'; // Example query

    const fetchResults = async (query) => {
        const response = await fetch(`${url}?query=${encodeURIComponent(query)}`);
        if (!response.ok) throw new Error('Network response was not ok');
        return response.json();
    };

    try {
        const data = await fetchResults(query);
        return new Response(JSON.stringify(data), { status: 200 });
    } catch (error) {
        return new Response('Error fetching data', { status: 500 });
    }
}

addEventListener('fetch', event => {
    event.respondWith(handleRequest(event.request));
});
