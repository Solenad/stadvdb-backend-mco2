import { initPools, getNode1, getNode3 } from "./config/connect.js";

async function upsertRowsToFragment(conn, rows) {
  if (rows.length === 0) return 0;
  const connection = await conn.getConnection();
  try {
    await connection.beginTransaction();
    for (const row of rows) {
      // skip rows without DOB or outside shards
      if (!row.dateOfBirth) continue;
      const year = new Date(row.dateOfBirth).getFullYear();
      if (year !== 2006 && year !== 2007) continue;

      const cols = Object.keys(row);
      const placeholders = cols.map(() => "?").join(", ");
      const values = cols.map((c) => row[c]);
      const updateParts = cols.map((c) => `\`${c}\` = VALUES(\`${c}\`)`).join(", ");

      const columnList = cols.map((c) => `\`${c}\``).join(", ");
      const sql = `INSERT INTO Users (${columnList})
        VALUES (${placeholders})
        ON DUPLICATE KEY UPDATE ${updateParts}`;

      await connection.execute(sql, values);
    }
    await connection.commit();
    return rows.length;
  } catch (err) {
    try { await connection.rollback(); } catch {}
    throw err;
  } finally {
    connection.release();
  }
}

async function main() {
  const { dbnodes } = await initPools();
  const masterNode = dbnodes.find(n => n.role === "MASTER" && n.status === "UP");
  if (!masterNode) throw new Error("No MASTER available.");

  const masterPool = masterNode.pool;
  const [rows] = await masterPool.query("SELECT * FROM Users");

  const node1Pool = getNode1();
  const node3Pool = getNode3();

  const rowsNode1 = [];
  const rowsNode3 = [];
  for (const r of rows) {
    if (!r.dateOfBirth) continue;
    const year = new Date(r.dateOfBirth).getFullYear();
    if (year === 2006) rowsNode1.push(r);
    else if (year === 2007) rowsNode3.push(r);
  }

  console.log(`Master rows: ${rows.length}, node1: ${rowsNode1.length}, node3: ${rowsNode3.length}`);

  const synced1 = await upsertRowsToFragment(node1Pool, rowsNode1);
  const synced3 = await upsertRowsToFragment(node3Pool, rowsNode3);

  console.log(`Synced: node1=${synced1}, node3=${synced3}`);
}

main().catch((e) => {
  console.error("Hard sync failed:", e);
  process.exit(1);
});