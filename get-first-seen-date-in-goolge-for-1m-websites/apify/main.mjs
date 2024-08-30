import { PlaywrightCrawler, RequestQueue } from 'crawlee';
import { getUndoneDomains, insertOrUpdateData } from './dbhelper.js';
import { JSDOM } from 'jsdom';

// Configuration
const apiUrl = 'https://www.google.com/search?q=About+';

// Function to parse domain data from HTML
function parseDomainData(html, domain) {
    try {
        const dom = new JSDOM(html);
        const document = dom.window.document;

        // Extracting index date
        const indexdateElement = Array.from(document.querySelectorAll('div')).find(el => el.textContent.includes("Site first indexed by Google"));
        let indexdate = "unk";
        if (indexdateElement) {
            const indexdateText = indexdateElement.textContent;
            const indexdateMatch = indexdateText.split("Site first indexed by Google").pop();
            if (indexdateMatch && !indexdateMatch.endsWith("ago")) {
                indexdate = indexdateMatch.split("ago")[0] + "ago";
            }
        }

        // Extracting About the Source and In Their Own Words
        let aboutthesource = "";
        let intheirownwords = "";
        const aboutSourceElements = Array.from(document.querySelectorAll('div')).filter(el => el.textContent.includes("Web results about the source"));
        if (aboutSourceElements.length > 0) {
            const aboutSourceText = aboutSourceElements[0].textContent;
            const [aboutText, inTheirOwnWordsText] = aboutSourceText.split("In their own words");
            if (aboutText) {
                aboutthesource = aboutText.split("About the source").pop().trim();
            }
            if (inTheirOwnWordsText) {
                intheirownwords = inTheirOwnWordsText.replace(/\r|\n/g, "").trim();
            }
        }

        return {
            domain,
            indexdate,
            Aboutthesource: aboutthesource,
            Intheirownwords: intheirownwords
        };
    } catch (error) {
        console.error(`Error parsing data for domain ${domain}:`, error);
        return null;
    }
}

// Function to handle requests
async function requestHandler({ request, page }) {
    const domain = new URL(request.url).searchParams.get('q').split('+')[1];
    const html = await page.content();
    const domainData = parseDomainData(html, domain);
    if (domainData) {
        await insertOrUpdateData('mysql', domainData);  // Use 'mysql' to specify MySQL database
    }
}

// Main function
(async () => {
    // Initialize request queue
    const requestQueue = await RequestQueue.open();
    
    // Fetch undone domains and add to queue
    const domains = await getUndoneDomains('mysql');  // Use 'mysql' to specify MySQL database
    for (const domainRecord of domains) {
        const domain = domainRecord.domain;
        const url = `${apiUrl}${domain}&tbm=ilp&sa=X&ved=2ahUKEwj30IH2gZuHAxUPAkQIHb8sA_sQv5AHegQIABAD`;
        await requestQueue.addRequest({ url });
    }

    // Initialize Playwright crawler
    const crawler = new PlaywrightCrawler({
        requestQueue,
        requestHandler,
        maxConcurrency: 5, // Adjust the concurrency as needed
    });

    await crawler.run();
})();
