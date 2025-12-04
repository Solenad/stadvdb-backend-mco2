import { initPools } from '../config/connect.js';   
import "dotenv/config";

export const promoteSlave = async (node) => {
    let pool = node.pool;

    try {
        
        await pool.query("STOP SLAVE");
        await pool.query("RESET SLAVE ALL");
        await pool.query("RESET MASTER");
        await pool.query("SET PERSIST read_only = OFF");

        console.log("Slave promoted");

        return node;
    } catch (err) {
        console.error("Error promoting slave:", err);
        throw err;
    }
};

export const assignSlave = async (slaveNodes, masterNode) => {
    let slavePool = slaveNodes.pool;

    try {
        const [masterStatus] = await masterNode.pool.query("SHOW MASTER STATUS");
        const logFile = masterStatus[0].File;
        const logPos = masterStatus[0].Position;

        await slavePool.query("STOP SLAVE");

        const sql = `
            CHANGE MASTER TO 
            MASTER_HOST = ?, 
            MASTER_USER = 'root', 
            MASTER_PASSWORD = ?, 
            MASTER_LOG_FILE = ?, 
            MASTER_LOG_POS = ?
        `;

        await slavePool.query(sql, [
            masterNode.host,         
            process.env.DB_PASSWORD,
            logFile,
            logPos
        ]);

        await slavePool.query("START SLAVE");

        console.log("Slave redirected successfully.");
        return true;

    } catch(err) {
        console.error("Error assigning slave:", err);
        throw err;
    }
};

export const changeMasterNode = async (downNodeId) => {
    const { dbnodes } = await initPools();

    let chosenNodeNumber = null;
    let newMasterNode = null;
    let nodeRole = null;

    const candidates = dbnodes.filter(n => n.node !== downNodeId && n.status === 'UP');

    if (candidates.length === 0) {
        throw new Error("No available nodes to promote!");
    }

    // Pick a random candidate from the valid list or if node 2 is down, pick node 1 (2006 user id, just change)
    if (downNodeId === "2") {
        newMasterNode =
            candidates.find((n) => n.node === "1") || candidates[0];
    } else {
        // otherwise, keep previous random behavior
        const randomIndex = Math.floor(Math.random() * candidates.length);
        newMasterNode = candidates[randomIndex];
    }
    chosenNodeNumber = newMasterNode.node;

    console.log(`Elected Node ${chosenNodeNumber} as the new Master.`);
    
    try {
        await promoteSlave(newMasterNode);
        
        let remainingSlaves = dbnodes.filter(node => 
            node.node !== chosenNodeNumber && node.node !== downNodeId
        );

        for (const slave of remainingSlaves) {
            if (slave.status === 'UP') {
                await assignSlave(slave, newMasterNode);
            }
        }

        dbnodes.forEach(node => {
            if (node.node === chosenNodeNumber) {
                nodeRole = node.role;
                node.role = 'MASTER';
                node.status = 'UP';
            } else if (node.node === downNodeId) {
                node.role = nodeRole; 
                node.status = 'DOWN';
            } else {
                node.status = 'UP';
            }
        });

        console.log("Roles updated and Master Node is now online.");
        return dbnodes;

    } catch (err) {
        console.error("Master-Slave switch was unsuccessful:", err);
        throw err;
    }
};

export const recoverOldMaster = async (crashedNodeId) => {

    const { dbnodes } = await initPools();
    const crashedNode = dbnodes.find(n => n.node === crashedNodeId);    
    const currentMaster = dbnodes.find(n => n.role === 'MASTER' && n.status === 'UP');

    if (!crashedNode) throw new Error("Node not found.");
    if (!currentMaster) throw new Error("No active Master to follow.");

    try {
        const [status] = await currentMaster.pool.query("SHOW MASTER STATUS");
        const logFile = status[0].File;
        const logPos = status[0].Position;

        await crashedNode.pool.query("STOP SLAVE");
        await crashedNode.pool.query("RESET MASTER"); 

        const sql = `
            CHANGE MASTER TO 
            MASTER_HOST = ?, MASTER_USER = 'root', MASTER_PASSWORD = ?, 
            MASTER_LOG_FILE = ?, MASTER_LOG_POS = ?
        `;
        await crashedNode.pool.query(sql, [
            currentMaster.host, 
            process.env.DB_PASSWORD, 
            logFile, 
            logPos
        ]);

        await crashedNode.pool.query("START SLAVE");
        crashedNode.status = 'UP';

        console.log(`Node ${crashedNodeId} recovered as Slave.`);
        return { success: true };
    } catch (err) {
        console.error("Recovery failed:", err);
        throw err;
    }
}