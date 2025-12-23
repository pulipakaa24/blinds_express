// agenda.js (modified to receive the already created pool)
const Agenda = require('agenda');

let agenda;
let socketIoInstance;
let sharedPgPool; // This will hold the pool instance passed from server.js

const initializeAgenda = async (mongoUri, pool, io) => { // Now accepts pgPool
  socketIoInstance = io;
  sharedPgPool = pool; // Store the passed pool

  agenda = new Agenda({
    db: {
      address: mongoUri || 'mongodb://localhost:27017/myScheduledApp',
      collection: 'agendaJobs',
    }
  });

  agenda.define('calib', async (job) => {
    const {periphID, userID } = job.attrs.data;
    try {
      const result = await sharedPgPool.query("update peripherals set await_calib=TRUE, calibrated=FALSE where id=$1 and user_id=$2 returning device_id, peripheral_number, id",
        [periphID, userID]
      );
      if (result.rowCount != 1) throw new Error("No such peripheral in database");

      const {rows} = await sharedPgPool.query("select socket from device_tokens where device_id=$1 and connected=TRUE", [result.rows[0].device_id]);
      if (rows.length != 1) console.log("No device with that ID connected to Socket.");
      else {
        const socket = rows[0].socket;
        if (socket) {
          socketIoInstance.to(socket).emit("calib", {periphNum: result.rows[0].peripheral_number});
        }
      }

      const {rows: userRows} = await sharedPgPool.query("select socket from user_tokens where user_id=$1", [userID]);

      if (userRows.length == 1) {
        if (userRows[0]){
          const userSocket = userRows[0].socket;
          socketIoInstance.to(userSocket).emit("calib", {periphID: result.rows[0].id});
        }
      }
      else console.log("No App connected");
    } catch (error) {
      console.error(`Error processing job:`, error);
      throw error;
    }
  });

  agenda.define('cancel_calib', async (job) => {
    const {periphID, userID } = job.attrs.data;
    try {
      const result = await sharedPgPool.query("update peripherals set await_calib=FALSE where id=$1 and user_id=$2 returning device_id, peripheral_number",
        [periphID, userID]
      );
      if (result.rowCount != 1) throw new Error("No such peripheral in database");

      const {rows} = await sharedPgPool.query("select socket from device_tokens where device_id=$1 and connected=TRUE", [result.rows[0].device_id]);
      if (rows.length != 1) console.log("No device with that ID connected to Socket.");
      else {
        const socket = rows[0].socket;
        if (socket) {
          socketIoInstance.to(socket).emit("cancel_calib", {periphNum: result.rows[0].peripheral_number});
        }
      }
    } catch (error) {
      console.error(`Error processing job:`, error);
      throw error;
    }
  })

  agenda.define('posChange', async (job) => {
    const { deviceID, changedPosList, userID } = job.attrs.data;
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

      const {rows} = await sharedPgPool.query("select socket from device_tokens where device_id=$1 and connected=TRUE", [deviceID]);
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

  agenda.define('posChangeScheduled', async (job) => {
    const { deviceID, changedPosList, userID } = job.attrs.data;
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

      const {rows} = await sharedPgPool.query("select socket from device_tokens where device_id=$1 and connected=TRUE", [deviceID]);
      if (rows.length != 1) console.log("No device with that ID connected to Socket.");
      else {
        const socket = rows[0].socket;
        if (socket) {
          socketIoInstance.to(socket).emit("posUpdates", posWithoutID);
        }
      }

      const {rows: userRows} = await sharedPgPool.query("select socket from user_tokens where user_id=$1", [userID]);

      if (userRows.length == 1) {
        if (userRows[0]){
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

  agenda.on('ready', () => console.log('Agenda connected to MongoDB and ready!'));
  agenda.on('start', (job) => console.log(`Job "${job.attrs.name}" starting`));
  agenda.on('complete', (job) => console.log(`Job "${job.attrs.name}" complete`));
  agenda.on('success', (job) => console.log(`Job "${job.attrs.name}" succeeded`));
  agenda.on('fail', (err, job) => console.error(`Job "${job.attrs.name}" failed: ${err.message}`));

  await agenda.start();
  console.log('Agenda job processing started.');
  return agenda;
};

module.exports = {
  agenda,
  initializeAgenda
};