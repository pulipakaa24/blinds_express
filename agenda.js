// agenda.js - Using pg-boss for PostgreSQL-based job scheduling
const PgBoss = require('pg-boss');

let boss;
let socketIoInstance;
let sharedPgPool; // This will hold the pool instance passed from server.js

const initializeAgenda = async (pool, io) => { // Only needs pool and io now
  socketIoInstance = io;
  sharedPgPool = pool; // Store the passed pool

  boss = new PgBoss({
    db: {
      executeSql: async (text, values) => {
        const result = await pool.query(text, values);
        return result;
      }
    }
  });

  boss.on('error', error => console.error('PgBoss error:', error));

  await boss.start();
  console.log('PgBoss started and ready!');

  // Create queues for all job types
  await boss.createQueue('calib');
  await boss.createQueue('cancel_calib');
  await boss.createQueue('posChange');
  await boss.createQueue('posChangeScheduled');
  await boss.createQueue('groupPosChangeScheduled');
  console.log('Job queues created');

  // Define job handlers
  await boss.work('calib', async (job) => {
    const { periphID, userID } = job.data;
    try {
      const result = await sharedPgPool.query("update peripherals set await_calib=TRUE, calibrated=FALSE where id=$1 and user_id=$2 returning device_id, peripheral_number, id",
        [periphID, userID]
      );
      if (result.rowCount != 1) throw new Error("No such peripheral in database");

      const { rows } = await sharedPgPool.query("select socket from device_tokens where device_id=$1 and connected=TRUE", [result.rows[0].device_id]);
      if (rows.length != 1) {
        console.log("No device with that ID connected to Socket.");
        // Notify user that device is not connected
        const { rows: userRows } = await sharedPgPool.query("select socket from user_tokens where user_id=$1", [userID]);
        if (userRows.length == 1 && userRows[0]) {
          socketIoInstance.to(userRows[0].socket).emit("calib_error", {
            periphID: result.rows[0].id,
            message: "Device not connected"
          });
        }
        // Reset await_calib since calibration cannot proceed
        await sharedPgPool.query("update peripherals set await_calib=FALSE where id=$1", [periphID]);
      }
      else {
        const socket = rows[0].socket;
        if (socket) {
          socketIoInstance.to(socket).emit("calib_start", { port: result.rows[0].peripheral_number });
        }
      }

      const { rows: userRows } = await sharedPgPool.query("select socket from user_tokens where user_id=$1", [userID]);

      if (userRows.length == 1) {
        if (userRows[0]) {
          const userSocket = userRows[0].socket;
          socketIoInstance.to(userSocket).emit("calib", { periphID: result.rows[0].id });
        }
      }
      else console.log("No App connected");
    } catch (error) {
      console.error(`Error processing job:`, error);
      throw error;
    }
  });

  await boss.work('cancel_calib', async (job) => {
    const { periphID, userID } = job.data;
    try {
      const result = await sharedPgPool.query("update peripherals set await_calib=FALSE where id=$1 and user_id=$2 returning device_id, peripheral_number",
        [periphID, userID]
      );
      if (result.rowCount != 1) throw new Error("No such peripheral in database");

      const { rows } = await sharedPgPool.query("select socket from device_tokens where device_id=$1 and connected=TRUE", [result.rows[0].device_id]);
      if (rows.length != 1) console.log("No device with that ID connected to Socket.");
      else {
        const socket = rows[0].socket;
        if (socket) {
          socketIoInstance.to(socket).emit("cancel_calib", { port: result.rows[0].peripheral_number });
        }
      }
    } catch (error) {
      console.error(`Error processing job:`, error);
      throw error;
    }
  });

  await boss.work('posChange', async (job) => {
    const { deviceID, changedPosList, userID } = job.data;
    // changedPosList is of the structure [{periphNum, periphID, pos}]
    const dateTime = new Date();
    if (!changedPosList) console.log("undefined list");
    try {
      const posWithoutID = changedPosList.map(pos => {
        const { periphID, ...rest } = pos;
        return rest;
      });

      // const posWithoutNumber = changedPosList.map(pos => {
      //     const { periphNum, ...rest } = pos;
      //     return rest;
      // });

      for (const pos of changedPosList) {
        const result = await sharedPgPool.query("update peripherals set last_pos=$1, last_set=$2 where id=$3 and user_id=$4",
          [pos.pos, dateTime, pos.periphID, userID]
        );
        if (result.rowCount != 1) throw new Error("No such peripheral in database");
      }

      const { rows } = await sharedPgPool.query("select socket from device_tokens where device_id=$1 and connected=TRUE", [deviceID]);
      if (rows.length != 1) console.log("No device with that ID connected to Socket.");
      else {
        const socket = rows[0].socket;
        if (socket) {
          socketIoInstance.to(socket).emit("posUpdates", posWithoutID);
        }
      }

    } catch (error) {
      console.error(`Error processing job:`, error);
      throw error;
    }
  });

  await boss.work('posChangeScheduled', async (job) => {
    console.log("posChangeScheduled job received");
    const { deviceID, changedPosList, userID } = job.data;
    // changedPosList is of the structure [{periphNum, periphID, pos}]
    const dateTime = new Date();
    if (!changedPosList) console.log("undefined list");
    try {
      const posWithoutID = changedPosList.map(pos => {
        const { periphID, ...rest } = pos;
        return rest;
      });

      const posWithoutNumber = changedPosList.map(pos => {
        const { periphNum, ...rest } = pos;
        return rest;
      });

      for (const pos of changedPosList) {
        const result = await sharedPgPool.query("update peripherals set last_pos=$1, last_set=$2 where id=$3 and user_id=$4",
          [pos.pos, dateTime, pos.periphID, userID]
        );
        if (result.rowCount != 1) throw new Error("No such peripheral in database");
      }

      const { rows } = await sharedPgPool.query("select socket from device_tokens where device_id=$1 and connected=TRUE", [deviceID]);
      if (rows.length != 1) console.log("No device with that ID connected to Socket.");
      else {
        const socket = rows[0].socket;
        if (socket) {
          socketIoInstance.to(socket).emit("posUpdates", posWithoutID);
        }
      }

      const { rows: userRows } = await sharedPgPool.query("select socket from user_tokens where user_id=$1", [userID]);

      if (userRows.length == 1) {
        if (userRows[0]) {
          const userSocket = userRows[0].socket;
          socketIoInstance.to(userSocket).emit("posUpdates", posWithoutNumber);
        }
      }
      else console.log("No App connected");

    } catch (error) {
      console.error(`Error processing job:`, error);
      throw error;
    }
  });

  await boss.work('groupPosChangeScheduled', async (job) => {
    const { groupID, newPos, userID } = job.data;
    const dateTime = new Date();
    try {
      // Query current group members at execution time
      const { rows: peripheralRows } = await sharedPgPool.query(
        `SELECT p.id, p.peripheral_number, p.device_id
         FROM peripherals p
         JOIN group_peripherals gp ON p.id = gp.peripheral_id
         JOIN groups g ON gp.group_id = g.id
         WHERE g.id = $1 AND g.user_id = $2`,
        [groupID, userID]
      );

      if (peripheralRows.length === 0) {
        console.log(`No peripherals found in group ${groupID}`);
        return;
      }

      // Group peripherals by device_id
      const deviceMap = new Map();
      for (const periph of peripheralRows) {
        if (!deviceMap.has(periph.device_id)) {
          deviceMap.set(periph.device_id, []);
        }
        deviceMap.get(periph.device_id).push({
          periphNum: periph.peripheral_number,
          periphID: periph.id,
          pos: newPos
        });
      }

      // Update database for all peripherals
      for (const periph of peripheralRows) {
        await sharedPgPool.query(
          "UPDATE peripherals SET last_pos=$1, last_set=$2 WHERE id=$3",
          [newPos, dateTime, periph.id]
        );
      }

      // Send socket events to each device
      for (const [deviceId, changedPosList] of deviceMap.entries()) {
        const { rows: deviceRows } = await sharedPgPool.query(
          "SELECT socket FROM device_tokens WHERE device_id=$1 AND connected=TRUE",
          [deviceId]
        );

        if (deviceRows.length === 1 && deviceRows[0].socket) {
          const posWithoutID = changedPosList.map(pos => {
            const { periphID, ...rest } = pos;
            return rest;
          });
          socketIoInstance.to(deviceRows[0].socket).emit("posUpdates", posWithoutID);
        }
      }

      // Notify user app
      const { rows: userRows } = await sharedPgPool.query(
        "SELECT socket FROM user_tokens WHERE user_id=$1",
        [userID]
      );

      if (userRows.length === 1 && userRows[0] && userRows[0].socket) {
        const posWithoutNumber = peripheralRows.map(p => ({
          periphID: p.id,
          pos: newPos
        }));
        socketIoInstance.to(userRows[0].socket).emit("posUpdates", posWithoutNumber);
      }

    } catch (error) {
      console.error(`Error processing group schedule job:`, error);
      throw error;
    }
  });

  console.log('PgBoss job processing started.');
  return boss;
};

module.exports = {
  agenda: () => boss, // Export a function that returns boss for compatibility
  initializeAgenda
};