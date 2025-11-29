import * as UsersService from "../services/users.services.js";

export async function createUser(req, res) {
  try {
    const data = req.body;

    const result = await UsersService.createUser(data);

    res.status(201).json({
      message: "User created successfully",
      data: result,
    });
  } catch (error) {
    if (
      error.message.includes("dateOfBirth") ||
      error.message.includes("DOB")
    ) {
      return res.status(400).json({ error: error.message });
    }
    res.status(500).json({ error: error.message });
  }
}

export async function getUsers(req, res) {
  try {
    const users = await UsersService.getAllUsers();

    res.status(200).json(users || []);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
}

export const getUserById = async (req, res) => {
  try {
    const { id } = req.params;
    const user = await UsersService.getUserById(id);

    if (!user) {
      return res.status(404).json({ error: "User not found" });
    }

    res.status(200).json(user);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

export const getUsersByYear = async (req, res) => {
  try {
    const year = parseInt(req.params.year);

    if (isNaN(year)) {
      return res.status(400).json({ error: "Invalid year format" });
    }

    const users = await UsersService.getAllUsersByDate(year);

    if (users === null) {
      return res.status(400).json({ error: "Year must be 2006 or 2007" });
    }

    res.status(200).json(users);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

export const updateUserById = async (req, res) => {
  try {
    const { id } = req.params;
    const updates = req.body;

    const isolation = req.query.isolation || null;

    const result = await UsersService.updateUserById(id, updates, {
      isolation,
    });

    if (!result) {
      return res.status(404).json({ message: `no user found with id: ${id}` });
    }

    res.status(200).json({
      success: true,
      message: "user updated successfully",
    });
  } catch (error) {
    if (
      error.message.includes("dob") ||
      error.message.includes("unauthorized")
    ) {
      return res.status(400).json({ error: error.message });
    }
    res.status(500).json({ error: error.message });
  }
};

export const deleteUserById = async (req, res) => {
  try {
    const { id } = req.params;

    const result = await UsersService.deleteUserById(id);

    if (!result) {
      return res.status(404).json({ message: `no user found with id: ${id}` });
    }

    res.status(200).json({
      success: true,
      message: "user deleted successfully",
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};
