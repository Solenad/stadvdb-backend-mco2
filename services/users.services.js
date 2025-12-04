import { getNode1, getNode3, initPools } from "../config/connect.js";
import { changeMasterNode } from "../services/recovery.service.js";

export const getWritePool = async () => {
  const { dbnodes }  = await initPools();

  let masterNode = dbnodes.find(
    (node) => node.role === "MASTER"
  );

  if (!masterNode) {
    throw new Error("No master node found.");
  }

  try {
    const testConn = await masterNode.pool.getConnection();
    testConn.release();
    return masterNode.pool;
  }
  catch (err) {
    console.error("Error connecting to master node:", err);
    try {
      await changeMasterNode(masterNode.node);

      const { dbnodes: updatedNodes } = await initPools();
      const newMaster = updatedNodes.find(
        (node) => node.role === "MASTER" && node.status === "UP"
      );

      if (!newMaster) {
        throw new Error("Failover failed: No new Master found.");
      }

      const verifyConn = await newMaster.pool.getConnection();
      verifyConn.release();

      console.log(`Failover complete. New master is Node ${newMaster.node}.`);
      return newMaster.pool;
    } catch (failoverErr) {
      console.error("System is completely down. Failover failed.", failoverErr);
      throw failoverErr;
    }
  }
};

export const getReadPool = async (option) => {
  const { dbnodes } = await initPools();

  // marks down if node unreachable
  const checkNode = async (node) => {
    if (!node) return false;
    try {
      const conn = await node.pool.getConnection();
      conn.release();
      return true;
    } catch (err) {
      console.error(
        `Read node ${node.node} (${node.role}) unreachable:`,
        err.message,
      );
      node.status = "DOWN";
      return false;
    }
  };

  // specific slave role
  let candidate = dbnodes.find(
    (n) => n.role === option && n.role.startsWith("SLAVE"),
  );

  if (candidate && (await checkNode(candidate))) {
    return candidate.pool;
  }

  const slaves = dbnodes.filter((n) => n.role.startsWith("SLAVE"));
  for (const node of slaves) {
    if (await checkNode(node)) {
      console.warn(
        `Read pool ${option} not available, using ${node.role} on Node ${node.node} instead.`,
      );
      return node.pool;
    }
  }

  // fall back to master node
  throw new Error("No available slave node for read operations.");
};

//removed fragment as its not allowed to be inserted into, only primary which is the master node
export const createUser = async (data) => {
  const masterNode = await getWritePool();
  /*const { slave06 } = await getReadPool("SLAVE 06");
  const { slave07 } = await getReadPool("SLAVE 07");*/
  let connPrimary = null;
  //let connFragment = null;

  try {
    if (!data.dateOfBirth) {
      throw new Error("dateOfBirth is required (must be 2006 or 2007).");
    }

    const year = new Date(data.dateOfBirth).getFullYear();
    if (year !== 2006 && year !== 2007) {
      throw new Error("Only DOB years 2006 or 2007 are allowed.");
    }

    const columns = Object.keys(data);
    const placeholders = columns.map(() => "?").join(", ");
    const values = Object.values(data);
    const insertSQL = `INSERT INTO Users (${columns.join(", ")}) VALUES (${placeholders})`;

    connPrimary = await masterNode.getConnection();
    await connPrimary.beginTransaction();

    const [result] = await connPrimary.execute(insertSQL, values);
    const insertedId = result.insertId;

    /*connFragment =
      year === 2006 ? await node1.getConnection() : await node3.getConnection();

    await connFragment.beginTransaction();

    await connFragment.execute(insertSQL, values);

    await connFragment.commit();*/
    await connPrimary.commit();

    return { success: true, id: insertedId };
  } catch (err) {
    console.error("Insert failed. Rolling back...", err);
    if (connPrimary) {
      try {
        await connPrimary.rollback();
      } catch {}
    }
    /*if (connFragment) {
      try {
        await connFragment.rollback();
      } catch {}
    }*/
    throw err;
  } finally {
    if (connPrimary) connPrimary.release();
    //if (connFragment) connFragment.release();
  }
};

export const getAllUsers = async () => {
  const slave06 = await getReadPool("SLAVE 06");
  const slave07 = await getReadPool("SLAVE 07");
  const [rows06, rows07] = await Promise.all([
    slave06.query("SELECT * FROM Users"),
    slave07.query("SELECT * FROM Users"),
  ]);

  return [...rows06, ...rows07] || null;
};

export const getUserById = async (id) => {
  // read from slave first
  try {
    const slave06 = await getReadPool("SLAVE 06");
    const slave07 = await getReadPool("SLAVE 07");

    const [rows06, rows07] = await Promise.all([
      slave06.query("SELECT * FROM Users WHERE id = ?", [id]),
      slave07.query("SELECT * FROM Users WHERE id = ?", [id]),
    ]);

    const users06 = rows06[0];
    const users07 = rows07[0];
    const user = (users06 && users06[0]) || (users07 && users07[0]);

    if (user) {
      return user;
    }
  } catch (err) {
    console.error("Read from slaves failed, will try master:", err.message);
  }

  // read from central if nothing works from slave nodes
  const masterPool = await getWritePool();
  const [rows] = await masterPool.query(
    "SELECT * FROM Users WHERE id = ?",
    [id],
  );
  return rows[0] || null;
};

export const getAllUsersByDate = async (year) => {
  const slave06 = await getReadPool("SLAVE 06");
  const slave07 = await getReadPool("SLAVE 07");

  if (year === 2006) {
    const [rows] = await slave06.query("SELECT * FROM Users");
    return rows;
  } else if (year === 2007) {
    const [rows] = await slave07.query("SELECT * FROM Users");
    return rows;
  } else {
    return null;
  }
};

export const updateUserById = async (
  id,
  data,
  { isolation = null, syncReplicate = false } = {},
) => {
  const masterNode = await getWritePool();
  let connPrimary = null;
  let connFragment = null;

  try {
    connPrimary = await masterNode.getConnection();

    if (isolation) {
      await connPrimary.query(
        `SET SESSION TRANSACTION ISOLATION LEVEL ${isolation}`,
      );
    }
    await connPrimary.beginTransaction();

    const [[user]] = await connPrimary.execute(
      `SELECT id, YEAR(STR_TO_DATE(dateOfBirth, '%m/%d/%Y')) AS year
      FROM Users WHERE id = ? FOR UPDATE`,
      [id],
    );

    if (!user) {
      await connPrimary.rollback();
      return null;
    }

    if (user.year !== 2006 && user.year !== 2007) {
      await connPrimary.rollback();
      throw new Error("Only users with DOB 2006 or 2007 can be updated.");
    }

    if (data.dateOfBirth) {
      const newYear = new Date(data.dateOfBirth).getFullYear();
      if (newYear !== 2006 && newYear !== 2007) {
        await connPrimary.rollback();
        throw new Error("DOB must remain 2006 or 2007.");
      }
      if (newYear !== user.year) {
        await connPrimary.rollback();
        throw new Error(
          "Changing DOB year across shards is not supported in this operation.",
        );
      }
    }

    const allowedColumns = [
      "firstName",
      "lastName",
      "address1",
      "address2",
      "city",
      "country",
      "zipCode",
      "phoneNumber",
      "gender",
      "dateOfBirth",
    ];

    const setParts = [];
    const values = [];

    for (const column in data) {
      if (!allowedColumns.includes(column)) {
        await connPrimary.rollback();
        throw new Error(`Unauthorized column: ${column}`);
      }
      setParts.push(`\`${column}\` = ?`);
      values.push(data[column]);
    }

    if (setParts.length === 0) {
      await connPrimary.rollback();
      throw new Error("No valid columns provided.");
    }

    values.push(id);
    const updateSQL = `UPDATE Users SET ${setParts.join(", ")} WHERE id = ?`;

    const fragmentPool = user.year === 2006 ? getNode1() : getNode3();
    const isSamePool = fragmentPool === masterNode;

    await connPrimary.execute(updateSQL, values);
    await connPrimary.commit();

    if (!isSamePool) {
      try {
        connFragment = await fragmentPool.getConnection();

        if (isolation) {
          await connFragment.query(
            `SET SESSION TRANSACTION ISOLATION LEVEL ${isolation}`,
          );
        }
        await connFragment.beginTransaction();
        await connFragment.execute(updateSQL, values);
        await connFragment.commit();
      } catch (fragErr) {
        console.error(
          "Fragment update failed (will rely on recovery service):",
          fragErr,
        );
        if (connFragment) {
          try {
            await connFragment.rollback();
          } catch {}
        }
      }
    } 
    return { success: true };
  } catch (err) {
    if (connPrimary) {
      try {
        await connPrimary.rollback();
      } catch {}
    }
    if (connFragment) {
      try {
        await connFragment.rollback();
      } catch {}
    }
    throw err;
  } finally {
    if (connPrimary) connPrimary.release();
    if (connFragment) connFragment.release();
  }
};

export const deleteUserById = async (id) => {
  const masterNode = await getWritePool();
  let connPrimary = null;
  //let connFragment = null;

  try {
    connPrimary = await masterNode.getConnection();

    await connPrimary.beginTransaction();

    const [[user]] = await connPrimary.execute(
      `SELECT id, YEAR(dateOfBirth) AS year FROM Users WHERE id = ? FOR UPDATE`,
      [id],
    );

    if (!user) {
      await connPrimary.rollback();
      return null;
    }

    if (user.year !== 2006 && user.year !== 2007) {
      await connPrimary.rollback();
      throw new Error("Only users with DOB 2006 or 2007 can be deleted.");
    }

    /*connFragment =
      user.year === 2006
        ? await node1.getConnection()
        : await node3.getConnection();

    await connFragment.beginTransaction();*/

    const deletePrimaryPromise = connPrimary.execute(
      `DELETE FROM Users WHERE id = ?`,
      [id],
    );
    /*const deleteFragmentPromise = connFragment.execute(
      `DELETE FROM Users WHERE id = ?`,
      [id],
    );*/

    await Promise.all([deletePrimaryPromise/*, deleteFragmentPromise*/]);

    //await connFragment.commit();
    await connPrimary.commit();

    return { success: true };
  } catch (err) {
    console.error("Delete failed. Rolling back...", err);
    if (connPrimary) {
      try {
        await connPrimary.rollback();
      } catch {}
    }
    /*if (connFragment) {
      try {
        await connFragment.rollback();
      } catch {}
    }*/
    throw err;
  } finally {
    if (connPrimary) connPrimary.release();
    //if (connFragment) connFragment.release();
  }
};