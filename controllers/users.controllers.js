import * as UsersService from "../services/users.services.js";

export async function createUser(req, res) {
  try {
    const data = req.body;

    const created_user = await UsersService.createUser(data);

    res.status(201).json(created_user);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
}

export async function getUsers(req, res) {
  try {
    const users = await UsersService.getAllUsers();

    res.status(200).json(users);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
}

export const getUserById = async (req, res) => {
  try {
    const id = req.params.id;
    const user = await UsersService.getUserById(id);
    if (!user) return res.status(404).json({ error: "User not found" });
    res.status(200).json(user);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

export const getUsersByYear = async (req, res) => {
  try {
    const year = parseInt(req.params.year);
    const users = await UsersService.getAllUsersByDate(year);
    res.status(200).json(users);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

export const updateUserById = async (req, res) => {
  try {
    const { id } = req.params;
    const updates = req.body;

    const result = await UsersService.updateUserById(updates);

    if (result.affectedRows === 0) {
      res.status(404).json({ message: `No user found with id: ${id}` });
    } else {
      res
        .status(200)
        .json({ success: true, affected_rows: result.affectedRows });
    }
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

export const deleteUserById = async (req, res) => {
  try {
    const { id } = req.params;

    const result = await UsersService.deleteUserById(id);

    if (result.affectedRows === 0) {
      res.status(404).json({ message: `No user found with id: ${id}` });
    } else {
      res
        .status(200)
        .json({ success: true, affected_rows: result.affectedRows });
    }
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};
