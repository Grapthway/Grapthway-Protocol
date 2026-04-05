// src/services/grapthway-client.ts

import { getGrapthwayNodeUrls, getDeveloperIdPrefix } from '../sdk/grapthway-config';
import { GrapthwayClient } from '../sdk/grapthway-sdk';

let grapthwayClientInstance: GrapthwayClient | null = null;
let nodeUrlsPromise: Promise<string[]> | null = null;
let nodePrefixPromise: Promise<string> | null = null

/**
 * Get node URLs from server via Server Action
 */
async function getNodeUrls(): Promise<string[]> {
  // Cache the promise to avoid multiple server calls
  if (!nodeUrlsPromise) {
    nodeUrlsPromise = getGrapthwayNodeUrls();
  }
  return nodeUrlsPromise;
}

export async function getNodePrefix(): Promise<string> {
  if (!nodePrefixPromise) {
    nodePrefixPromise = getDeveloperIdPrefix();
  }
  return nodePrefixPromise;
}

/**
 * Get or create singleton GrapthwayClient instance
 */
export async function getGrapthwayClient(): Promise<GrapthwayClient> {
  if (!grapthwayClientInstance) {
    const nodeUrls = await getNodeUrls();
    grapthwayClientInstance = new GrapthwayClient({ nodeUrls });
    console.log(`✅ GrapthwayClient initialized with ${nodeUrls.length} node(s)`);
  }
  
  return grapthwayClientInstance;
}

/**
 * Get the best available node URL with automatic health checking
 */
export async function getBestNodeUrl(forceCheck: boolean = false): Promise<string> {
  const client = await getGrapthwayClient();
  return await client.getBestNodeUrl(forceCheck);
}
