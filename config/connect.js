import mysql from "mysql2/promise";
import { setTimeout as sleep } from "timers/promise";
import "dotenv/config";

let node1 = null;
let node2 = null;
let node3 = null;

const MAX_RETRIES = 5;

const createPool = (host) => {
  mysql.createPool({
    host,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    waitForConnections: true,
    connectionLimit: 10,
  });
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
  if (node1 && node2 && node3) return { node1, node2, node3 };

  console.log("Initializing Nodes 1, 2, and 3...");

  node1 = createPool(process.env.NODE1_HOST);
  node2 = createPool(process.env.NODE2_HOST);
  node3 = createPool(process.env.NODE3_HOST);

  await Promise.all([
    connectWithRetry(node1, "Node 1 (Full Replica)"),
    connectWithRetry(node2, "Node 2 (2007 Fragment)"),
    connectWithRetry(node3, "Node 3 (2006 Fragment)"),
  ]);

  console.log("Connected to all nodes successfully.");

  return { node1, node2, node3 };
};

export const closePools = async () => {
  try {
    if (node1) await node1.end();
    if (node2) await node2.end();
    if (node3) await node3.end();

    console.log("All DB pools closed.");
  } catch (err) {
    console.error("Error closing pools:", err);
  }
};
