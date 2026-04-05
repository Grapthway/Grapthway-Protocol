'use server';

/**
 * Server action to get Grapthway node URLs from server-side environment variables
 */
export async function getGrapthwayNodeUrls(): Promise<string[]> {
    const urls: string[] = [];
    
    // Try GRAPTHWAY_NODE_URL first (backward compatibility)
    if (process.env.GRAPTHWAY_NODE_URL) {
        urls.push(process.env.GRAPTHWAY_NODE_URL);
    }
    
    // Then try GRAPTHWAY_NODE_URL_1, GRAPTHWAY_NODE_URL_2, etc.
    let index = 1;
    while (process.env[`GRAPTHWAY_NODE_URL_${index}`]) {
        urls.push(process.env[`GRAPTHWAY_NODE_URL_${index}`]!);
        index++;
    }
    
    if (urls.length === 0) {
        throw new Error('No Grapthway node URLs configured on server');
    }
    
    return urls;
}

export async function getDeveloperIdPrefix(): Promise<string> {
    const prefix = process.env.PREFIX ? process.env.PREFIX : '';

    if (prefix === ''){
        throw new Error('No Grapthway prefix configured on server');
    }
    return prefix;
}
