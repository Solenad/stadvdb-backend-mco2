import { Router } from "express";
import * as UsersController from "../controllers/users.controllers.js";

const router = Router();

router.get("/year/:year", UsersController.getUsersByYear);
router.get("/:id", UsersController.getUserById);
router.get("/", UsersController.getUsers);

export default router;
