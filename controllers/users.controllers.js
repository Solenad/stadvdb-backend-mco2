import * as UsersService from "../services/users.services.js";

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
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};

export const getUsersByYear = async (req, res) => {
  try {
    const year = parseInt(req.params.year);
    const users = await UsersService.getAllUsersByDate(year);
    res.status(200).json(users);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};
