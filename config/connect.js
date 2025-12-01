import mysql from "mysql2/promise";
import { setTimeout as sleep } from "timers/promises";
import { recoverOldMaster } from "../services/recovery.service.js";
import "dotenv/config";

let dbnodes = null;

const MAX_RETRIES = 5;

const createPool = (port) => {
  const pool = mysql.createPool({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    port,
    waitForConnections: true,
    connectionLimit: 10,
  });

  return pool;
};

async function connectWithRetry(pool, label) {
  let retries = MAX_RETRIES;
  let lastError = null;

  while (retries > 0) {
    try {
      const conn = await pool.getConnection();
      console.log(`Connected to ${label}`);
      conn.release();
      return;
    } catch (err) {
      lastError = err;
      retries--;

      if (retries > 0) {
        const delay = Math.pow(2, MAX_RETRIES - retries) * 1000;

        console.log(
          `Failed to connect to ${label}. Retrying in ${delay / 1000}s... (${retries} retries left)`,
        );

        await sleep(delay);
      }
    }
  }

  console.error(`Failed to connect to ${label} after multiple attempts.`);
  throw lastError;
}

export const initPools = async () => {
  if (dbnodes != null && dbnodes.length === 3) return { dbnodes };

  console.log("Initializing Nodes 1, 2, and 3...");
    dbnodes = [
      {pool: createPool(process.env.NODE1_PORT),host: process.env.NODE1_IP, role: 'SLAVE 06', status: 'UP', node: '1'},
      {pool: createPool(process.env.NODE2_PORT),host: process.env.NODE2_IP, role: 'MASTER', status: 'UP', node: '2'},
      {pool: createPool(process.env.NODE3_PORT),host: process.env.NODE3_IP, role: 'SLAVE 07', status: 'UP', node: '3'},
    ];

  await Promise.all([
    connectWithRetry(dbnodes[0], `Node ${dbnodes[0].node}, ${dbnodes[0].role}`),
    connectWithRetry(dbnodes[1], `Node ${dbnodes[1].node}, ${dbnodes[1].role}`),
    connectWithRetry(dbnodes[2], `Node ${dbnodes[2].node}, ${dbnodes[2].role}`),
  ]);

  console.log("Connected to all nodes successfully.");

  return { dbnodes };
};

export const closeNode = async (nodeId) => {
  const chosenNode = dbnodes.find(dbnode => dbnode.node === nodeId);

  if (!chosenNode) {
    console.log(`Node ${nodeId} not found in the servers.`);
    return;
  }

  if (chosenNode.status === 'DOWN') {
    console.log(`Node ${nodeId} is already down.`);
    return;
  }

  try {
    await chosenNode.pool.end();
    chosenNode.status = "DOWN";
    console.log(`Node ${nodeId} (${chosenNode.role}) DB pool closed.`);

    return { node: chosenNode.node, status: chosenNode.status };

  } catch (err) {
    console.error("Error closing downed pool:", err);
    chosenNode.status = "DOWN";
    throw err;
  }
};

export const closePools = async () => {
  try {

    const masterNode = dbnodes.find(node => node.role === 'MASTER');
    if (masterNode) {
      await closeNode(masterNode.node);
    }
    
    const slaveClosures = dbnodes
        .filter(node => node.role.startsWith('SLAVE') && node.status === 'UP')
        .map(node => closeNode(node.node));

    await Promise.all(slaveClosures);

    console.log("All DB pools closed.");
  } catch (err) {
    console.error("Error closing pools:", err);
  }
};

//Checks for downed master nodes until it goes back up
setInterval(async () => {
  if (!dbnodes) return;
  const deadNode = dbnodes.find(node => node.status === 'DOWN');

  if (deadNode) {
    try {
      const conn = await deadNode.pool.getConnection();
      conn.release();
      await recoverOldMaster(deadNode.node);

    } catch (err) {console.log(`Node ${deadNode.node} is still unreachable.`);
    }
  }
}, 10000);