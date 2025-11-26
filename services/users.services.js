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
  const node2 = pools.node2;
  const node3 = pools.node3;

  if (year === 2006) {
    const [rows] = await node2.query("SELECT * FROM Users");
    return rows;
  } else if (year === 2007) {
    const [rows] = await node3.query("SELECT * FROM Users");
    return rows;
  } else {
    return null;
  }
};
