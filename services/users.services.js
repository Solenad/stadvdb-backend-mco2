import { initPools } from "../config/connect.js";

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
  const pools = await initPools();
  const node1 = pools.node1;
  const node2 = pools.node2;
  const node3 = pools.node3;

  const connPrimary = await node2.getConnection();

  const [[user]] = await connPrimary.execute(
    `SELECT id, YEAR(createdAt) AS year FROM Users WHERE id = ?`,
    [id],
  );

  if (!user) {
    connPrimary.release();
    return null;
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
    } catch {}

    try {
      await connFragment.rollback();
    } catch {}

    connPrimary.release();
    if (connFragment) connFragment.release();

    throw err;
  }
};
