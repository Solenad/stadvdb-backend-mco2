import { initPools } from "../config/connect.js";

export const createUser = async (data) => {
  const { node1, node2, node3 } = await initPools();
  const connPrimary = await node2.getConnection();
  let connFragment = null;

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

    const insertSQL = `
      INSERT INTO Users (${columns.join(", ")})
      VALUES (${placeholders})
    `;

    await connPrimary.beginTransaction();

    const [result] = await connPrimary.execute(insertSQL, values);
    const insertedId = result.insertId;

    connFragment =
      year === 2006 ? await node1.getConnection() : await node3.getConnection();

    await connFragment.beginTransaction();

    await connFragment.execute(insertSQL, values);

    await connPrimary.commit();
    await connFragment.commit();

    connPrimary.release();
    connFragment.release();

    return { success: true, id: insertedId };
  } catch (err) {
    console.error("Insert failed. Rolling back...", err);

    try {
      await connPrimary.rollback();
    } catch { }
    try {
      if (connFragment) await connFragment.rollback();
    } catch { }

    connPrimary.release();
    if (connFragment) connFragment.release?.();
    throw err;
  }
};

export const getAllUsers = async () => {
  const pools = await initPools();
  const node1 = pools.node1;
  const [rows] = await node1.query("SELECT * FROM Users");
  return rows || null;
};

export const getUserById = async (id) => {
  const pools = await initPools();
  const node1 = pools.node1;
  const [rows] = await node1.query("SELECT * FROM Users WHERE id = ?", [id]);
  return rows[0] || null;
};

export const getAllUsersByDate = async (year) => {
  const pools = await initPools();
  const node1 = pools.node1;
  const node3 = pools.node3;

  if (year === 2006) {
    const [rows] = await node1.query("SELECT * FROM Users");
    return rows;
  } else if (year === 2007) {
    const [rows] = await node3.query("SELECT * FROM Users");
    return rows;
  } else {
    return null;
  }
};

export const updateUserById = async (id, data) => {
  const { node1, node2, node3 } = await initPools();

  const connPrimary = await node2.getConnection();

  const [[user]] = await connPrimary.execute(
    `SELECT id, YEAR(dateOfBirth) AS year FROM Users WHERE id = ?`,
    [id],
  );

  if (!user) {
    connPrimary.release();
    return null;
  }

  if (user.year !== 2006 && user.year !== 2007) {
    connPrimary.release();
    throw new Error("Only users with DOB 2006 or 2007 can be updated.");
  }

  if (data.dateOfBirth) {
    const newYear = new Date(data.dateOfBirth).getFullYear();
    if (newYear !== 2006 && newYear !== 2007) {
      connPrimary.release();
      throw new Error("DOB must remain 2006 or 2007.");
    }
  }

  const connFragment =
    user.year === 2006
      ? await node1.getConnection()
      : await node3.getConnection();

  try {
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
        throw new Error(`Unauthorized column: ${column}`);
      }
      setParts.push(`\`${column}\` = ?`);
      values.push(data[column]);
    }

    if (setParts.length === 0) {
      throw new Error("No valid columns provided.");
    }

    const updateSQL = `UPDATE Users SET ${setParts.join(", ")} WHERE id = ?`;
    values.push(id);

    await connPrimary.beginTransaction();
    await connFragment.beginTransaction();

    await connPrimary.execute(updateSQL, values);
    await connFragment.execute(updateSQL, values);

    await connPrimary.commit();
    await connFragment.commit();

    connPrimary.release();
    connFragment.release();

    return { success: true };
  } catch (err) {
    console.error("Update failed. Rolling back...", err);

    try {
      await connPrimary.rollback();
    } catch { }
    try {
      await connFragment.rollback();
    } catch { }

    connPrimary.release();
    if (connFragment) connFragment.release();
    throw err;
  }
};

export const deleteUserById = async (id) => {
  const { node1, node2, node3 } = await initPools();

  const connPrimary = await node2.getConnection();

  const [[user]] = await connPrimary.execute(
    `SELECT id, YEAR(dateOfBirth) AS year FROM Users WHERE id = ?`,
    [id]
  );

  if (!user) {
    connPrimary.release();
    return null;
  }

  if (user.year !== 2006 && user.year !== 2007) {
    connPrimary.release();
    throw new Error("Only users with DOB 2006 or 2007 can be deleted.");
  }

  const connFragment =
    user.year === 2006
      ? await node1.getConnection()
      : await node3.getConnection();

  try {
    await connPrimary.beginTransaction();
    await connFragment.beginTransaction();

    await connPrimary.execute(`DELETE FROM Users WHERE id = ?`, [id]);

    await connFragment.execute(`DELETE FROM Users WHERE id = ?`, [id]);

    await connPrimary.commit();
    await connFragment.commit();

    connPrimary.release();
    connFragment.release();

    return { success: true };
  } catch (err) {
    console.error("Delete failed. Rolling back...", err);

    try {
      await connPrimary.rollback();
    } catch { }
    try {
      await connFragment.rollback();
    } catch { }

    connPrimary.release();
    connFragment.release();

    throw err;
  }
};
