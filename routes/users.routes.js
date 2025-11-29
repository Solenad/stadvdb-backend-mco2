import { Router } from "express";
import * as UsersController from "../controllers/users.controllers.js";

const router = Router();

router.get("/year/:year", UsersController.getUsersByYear);
router.get("/:id", UsersController.getUserById);
router.put("/:id", UsersController.updateUserById);
router.get("/", UsersController.getUsers);
router.post("/", UsersController.createUser);
router.delete("/:id", UsersController.deleteUserById);

export default router;
