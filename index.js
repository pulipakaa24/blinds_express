const { verify, hash } = require('argon2');
const express = require('express');
const { json } = require('express');
const { Pool } = require('pg');
require('dotenv').config();
const jwt = require('jsonwebtoken');
const http = require('http');
const socketIo = require('socket.io');
const { initializeAgenda } = require('./agenda'); // pg-boss setup
const format = require('pg-format');
const cronParser = require('cron-parser');
const crypto = require('crypto');

const app = express();
const port = 3000;
app.use(json());
const pool = new Pool();
const server = http.createServer(app);
const io = socketIo(server, {
  pingInterval: 15000,
  pingTimeout: 10000,
});

let agenda;

(async () => {
  // Initialize pg-boss with PostgreSQL pool
  agenda = await initializeAgenda(pool, io);
})();

(async () => {
  await pool.query("update user_tokens set connected=FALSE where connected=TRUE");
  await pool.query("update device_tokens set connected=FALSE where connected=TRUE");
})();
const JWT_SECRET = process.env.JWT_SECRET;
const TOKEN_EXPIRY = '5d';

io.on('connection', async (socket) => {
  console.log("periph connected");
  const token = socket.handshake.auth.token ?? socket.handshake.headers['authorization']?.split(' ')[1];
  try {
    if (!token) throw new Error("no token!");
    const payload = jwt.verify(token, JWT_SECRET);
    let table;
    let idCol;
    let id;

    if (payload.type === "user") {
      table = "user_tokens";
      idCol = "user_id";
      socket.user = true;
      id = payload.userId;
    }
    else if (payload.type === "peripheral") {
      table = "device_tokens";
      idCol = "device_id";
      socket.user = false;
      id = payload.peripheralId;
    }
    else {
      throw new Error("Unauthorized");
    }

    const query = format(`update %I set connected=TRUE, socket=$1 where %I=$2 and token=$3 and connected=FALSE`,
      table, idCol
    );

    const result = await pool.query(query, [socket.id, id, token]);

    if (result.rowCount != 1) {
      const errorResponse = {
        type: 'error',
        code: 404,
        message: 'Device not found or already connected'
      };
      socket.emit("error", errorResponse);
      socket.disconnect(true);
    }

    else {
      console.log("success - sending device state");

      // For peripheral devices, send current device state
      if (payload.type === "peripheral") {
        try {
          // Get peripheral states for this device (device will report calibration status back)
          const { rows: periphRows } = await pool.query(
            "select last_pos, peripheral_number from peripherals where device_id=$1",
            [id]
          );

          const successResponse = {
            type: 'success',
            code: 200,
            message: 'Device authenticated',
            deviceState: periphRows.map(row => ({
              port: row.peripheral_number,
              lastPos: row.last_pos
            }))
          };

          socket.emit("device_init", successResponse);

          // Notify user app that device is now connected
          const { rows: deviceUserRows } = await pool.query("select user_id from devices where id=$1", [id]);
          if (deviceUserRows.length === 1) {
            const { rows: userRows } = await pool.query("select socket from user_tokens where user_id=$1 and connected=TRUE", [deviceUserRows[0].user_id]);
            if (userRows.length === 1 && userRows[0]) {
              io.to(userRows[0].socket).emit("device_connected", { deviceID: id });
            }
          }
        } catch (err) {
          console.error("Error fetching device state:", err);
          socket.emit("device_init", {
            type: 'success',
            code: 200,
            message: 'Device authenticated',
            deviceState: []
          });
        }
      } else {
        // User connection
        const successResponse = {
          type: 'success',
          code: 200,
          message: 'User connected'
        };
        socket.emit("success", successResponse);
      }
    }

  } catch (error) {
    console.error('Error during periph authentication:', error);

    // Send an error message to the client
    socket.emit('error', { type: 'error', code: 500 });

    // Disconnect the client
    socket.disconnect(true);
  }

  // Device reports its calibration status after connection
  socket.on('report_calib_status', async (data) => {
    console.log(`Device reporting calibration status: ${socket.id}`);
    console.log(data);
    try {
      const { rows } = await pool.query("select device_id from device_tokens where socket=$1 and connected=TRUE", [socket.id]);
      if (rows.length != 1) throw new Error("No device with that ID connected to Socket.");

      // Update calibration status in database based on device's actual state
      if (data.port && typeof data.calibrated === 'boolean') {
        await pool.query(
          "update peripherals set calibrated=$1 where device_id=$2 and peripheral_number=$3",
          [data.calibrated, rows[0].device_id, data.port]
        );
        console.log(`Updated port ${data.port} calibrated status to ${data.calibrated}`);
      }
    } catch (error) {
      console.error(`Error in report_calib_status:`, error);
    }
  });

  // Device reports calibration error (motor stall, sensor failure, etc.)
  socket.on('device_calib_error', async (data) => {
    console.log(`Device reporting calibration error: ${socket.id}`);
    console.log(data);
    try {
      const { rows } = await pool.query("select device_id from device_tokens where socket=$1 and connected=TRUE", [socket.id]);
      if (rows.length != 1) throw new Error("No device with that ID connected to Socket.");

      const result = await pool.query(
        "update peripherals set await_calib=FALSE where device_id=$1 and peripheral_number=$2 returning id, user_id",
        [rows[0].device_id, data.port]
      );
      if (result.rowCount != 1) throw new Error("No such peripheral in database");

      // Notify user app about the error
      const { rows: userRows } = await pool.query("select socket from user_tokens where user_id=$1 and connected=TRUE", [result.rows[0].user_id]);
      if (userRows.length === 1 && userRows[0]) {
        io.to(userRows[0].socket).emit("calib_error", {
          periphID: result.rows[0].id,
          message: data.message || "Device error during calibration"
        });
      }
      console.log(`Calibration cancelled for port ${data.port} due to device error`);
    } catch (error) {
      console.error(`Error in device_calib_error:`, error);
    }
  });

  // Device acknowledges ready for stage 1 (tilt up)
  socket.on('calib_stage1_ready', async (data) => {
    console.log(`Device ready for stage 1 (tilt up): ${socket.id}`);
    console.log(data);
    try {
      const { rows } = await pool.query("select device_id from device_tokens where socket=$1 and connected=TRUE", [socket.id]);
      if (rows.length != 1) throw new Error("No device with that ID connected to Socket.");

      const result = await pool.query(
        "select id, user_id from peripherals where device_id=$1 and peripheral_number=$2",
        [rows[0].device_id, data.port]
      );
      if (result.rowCount != 1) throw new Error("No such peripheral in database");

      const { rows: userRows } = await pool.query("select socket from user_tokens where user_id=$1", [result.rows[0].user_id]);
      if (userRows.length == 1 && userRows[0]) {
        const userSocket = userRows[0].socket;
        io.to(userSocket).emit("calib_stage1_ready", { periphID: result.rows[0].id });
      }
    } catch (error) {
      console.error(`Error in calib_stage1_ready:`, error);
    }
  });

  // Device acknowledges ready for stage 2 (tilt down)
  socket.on('calib_stage2_ready', async (data) => {
    console.log(`Device ready for stage 2 (tilt down): ${socket.id}`);
    console.log(data);
    try {
      const { rows } = await pool.query("select device_id from device_tokens where socket=$1 and connected=TRUE", [socket.id]);
      if (rows.length != 1) throw new Error("No device with that ID connected to Socket.");

      const result = await pool.query(
        "select id, user_id from peripherals where device_id=$1 and peripheral_number=$2",
        [rows[0].device_id, data.port]
      );
      if (result.rowCount != 1) throw new Error("No such peripheral in database");

      const { rows: userRows } = await pool.query("select socket from user_tokens where user_id=$1", [result.rows[0].user_id]);
      if (userRows.length == 1 && userRows[0]) {
        const userSocket = userRows[0].socket;
        io.to(userSocket).emit("calib_stage2_ready", { periphID: result.rows[0].id });
      }
    } catch (error) {
      console.error(`Error in calib_stage2_ready:`, error);
    }
  });

  // User confirms stage 1 complete (tilt up done)
  socket.on('user_stage1_complete', async (data) => {
    console.log(`User confirms stage 1 complete: ${socket.id}`);
    console.log(data);
    try {
      const { rows } = await pool.query("select socket from device_tokens where device_id=$1 and connected=TRUE", [data.deviceID]);
      if (rows.length != 1) {
        // Device not connected - notify app
        socket.emit("calib_error", {
          periphID: data.periphID,
          message: "Device disconnected during calibration"
        });
        // Reset peripheral state
        await pool.query("update peripherals set await_calib=FALSE where id=$1", [data.periphID]);
        return;
      }

      const deviceSocket = rows[0].socket;
      io.to(deviceSocket).emit("user_stage1_complete", { port: data.periphNum });
    } catch (error) {
      console.error(`Error in user_stage1_complete:`, error);
      socket.emit("calib_error", {
        periphID: data.periphID,
        message: "Error during calibration"
      });
    }
  });

  // User confirms stage 2 complete (tilt down done)
  socket.on('user_stage2_complete', async (data) => {
    console.log(`User confirms stage 2 complete: ${socket.id}`);
    console.log(data);
    try {
      const { rows } = await pool.query("select socket from device_tokens where device_id=$1 and connected=TRUE", [data.deviceID]);
      if (rows.length != 1) {
        // Device not connected - notify app
        socket.emit("calib_error", {
          periphID: data.periphID,
          message: "Device disconnected during calibration"
        });
        // Reset peripheral state
        await pool.query("update peripherals set await_calib=FALSE where id=$1", [data.periphID]);
        return;
      }

      const deviceSocket = rows[0].socket;
      io.to(deviceSocket).emit("user_stage2_complete", { port: data.periphNum });
    } catch (error) {
      console.error(`Error in user_stage2_complete:`, error);
      socket.emit("calib_error", {
        periphID: data.periphID,
        message: "Error during calibration"
      });
    }
  });

  // Device acknowledges calibration complete
  socket.on('calib_done', async (data) => {
    console.log(`Received 'calib_done' event from client ${socket.id}:`);
    console.log(data);
    try {
      const { rows } = await pool.query("select device_id from device_tokens where socket=$1 and connected=TRUE", [socket.id]);
      if (rows.length != 1) throw new Error("No device with that ID connected to Socket.");
      const result = await pool.query("update peripherals set await_calib=FALSE, calibrated=TRUE where device_id=$1 and peripheral_number=$2 returning id, user_id",
        [rows[0].device_id, data.port]
      );
      if (result.rowCount != 1) throw new Error("No such peripheral in database");

      const { rows: userRows } = await pool.query("select socket from user_tokens where user_id=$1", [result.rows[0].user_id]);
      if (userRows.length == 1) {
        if (userRows[0]) {
          const userSocket = userRows[0].socket;
          io.to(userSocket).emit("calib_done", { periphID: result.rows[0].id });
        }
      }
      else console.log("No App connected");

    } catch (error) {
      console.error(`Error processing job:`, error);
      throw error;
    }
  });

  socket.on('pos_hit', async (data) => {
    console.log(`Received 'pos_hit' event from client ${socket.id}:`);
    console.log(data);
    const dateTime = new Date();
    try {
      const { rows } = await pool.query("select device_id from device_tokens where socket=$1 and connected=TRUE", [socket.id]);
      if (rows.length != 1) throw new Error("No device with that ID connected to Socket.");
      const result = await pool.query("update peripherals set last_pos=$1, last_set=$2 where device_id=$3 and peripheral_number=$4 returning id, user_id",
        [data.pos, dateTime, rows[0].device_id, data.port]
      );
      if (result.rowCount != 1) throw new Error("No such peripheral in database");

      const { rows: userRows } = await pool.query("select socket from user_tokens where user_id=$1", [result.rows[0].user_id]);
      if (userRows.length == 1) {
        if (userRows[0]) {
          const userSocket = userRows[0].socket;
          io.to(userSocket).emit("pos_hit", { periphID: result.rows[0].id });
        }
      }
      else console.log("No App connected");

    } catch (error) {
      console.error(`Error processing job:`, error);
      throw error;
    }
  });

  socket.on("disconnect", async () => {
    if (socket.user) {
      console.log("user disconnect");
      await pool.query("update user_tokens set connected=FALSE where socket=$1", [socket.id]);
    }
    else {
      console.log("device disconnect");
      const { rows } = await pool.query(
        "update device_tokens set connected=FALSE where socket=$1 returning device_id",
        [socket.id]
      );

      // Notify user app that device disconnected
      if (rows.length === 1) {
        const { rows: deviceRows } = await pool.query("select user_id from devices where id=$1", [rows[0].device_id]);
        if (deviceRows.length === 1) {
          const { rows: userRows } = await pool.query("select socket from user_tokens where user_id=$1 and connected=TRUE", [deviceRows[0].user_id]);
          if (userRows.length === 1 && userRows[0]) {
            io.to(userRows[0].socket).emit("device_disconnected", { deviceID: rows[0].device_id });
          }
        }
      }
    }
  });
});

async function createToken(userId) {
  const token = jwt.sign({ type: 'user', userId }, JWT_SECRET, { expiresIn: TOKEN_EXPIRY });
  await pool.query("delete from user_tokens where user_id=$1", [userId]);
  await pool.query("insert into user_tokens (user_id, token) values ($1, $2)", [userId, token]);
  return token;
}

async function createPeripheralToken(peripheralId) {
  const token = jwt.sign({ type: 'peripheral', peripheralId }, JWT_SECRET);
  await pool.query("insert into device_tokens (device_id, token) values ($1, $2)", [peripheralId, token]);
  return token;
}

async function createTempPeriphToken(peripheralId) {
  const token = jwt.sign({ type: 'peripheral', peripheralId }, JWT_SECRET, { expiresIn: '2m' });
  await pool.query("insert into device_tokens (device_id, token) values ($1, $2)", [peripheralId, token]);
  return token;
}

async function authenticateToken(req, res, next) {
  const authHeader = req.headers['authorization'];
  const token = authHeader?.split(' ')[1];

  if (!token) return res.sendStatus(401);

  try {
    const payload = jwt.verify(token, JWT_SECRET);
    if (payload.type === 'user') {
      const { rows } = await pool.query("select user_id from user_tokens where token=$1", [token]);
      if (rows.length != 1) throw new Error("Invalid/Expired Token");
      req.user = payload.userId; // make Id accessible in route handlers
    }
    else if (payload.type === 'peripheral') {
      const { rows } = await pool.query("select device_id from device_tokens where token=$1", [token]);
      if (rows.length != 1) throw new Error("Invalid/Expired Token");
      req.peripheral = payload.peripheralId;
    }
    else {
      throw new Error("Invalid/Expired Token");
    }
    next();
  } catch {
    return res.sendStatus(403); // invalid/expired token
  }
}

app.get('/', (req, res) => {
  res.send('Hello World!');
});

app.post('/login', async (req, res) => {
  const { email, password } = req.body;
  console.log('login');
  if (!email || !password) return res.status(400).json({ error: 'email and password required' });
  try {
    const { rows } = await pool.query('select id, password_hash_string from users where email = $1', [email]);
    if (rows.length === 0) return res.status(401).json({ error: 'Invalid Credentials' });
    const user = rows[0]
    console.log('user found');
    const verified = await verify(user.password_hash_string, password);

    if (!verified) return res.status(401).json({ error: 'Invalid credentials' });
    console.log("password correct");
    const token = await createToken(user.id); // token is now tied to ID

    res.status(200).json({ token });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/create_user', async (req, res) => {
  console.log("got post req");
  const { name, email, password } = req.body
  try {

    const hashedPass = await hash(password);

    await pool.query("insert into users (name, email, password_hash_string) values (nullif($1, ''), $2, $3)",
      [name, email, hashedPass]
    );
    return res.sendStatus(201);
  } catch (err) {
    console.error(err);
    if (err.code === '23505') {
      return res.status(409).json({ error: 'Email already in use' });
    }
    return res.sendStatus(500);
  }
});

app.get('/verify', authenticateToken, async (req, res) => {
  try {
    // Issue a new token to extend session
    const newToken = await createToken(req.user);
    res.status(200).json({ token: newToken });
  } catch {
    res.status(500).json({ error: 'server error' });
  }
});

app.get('/device_list', authenticateToken, async (req, res) => {
  try {
    console.log("device List request");
    console.log(req.user);
    const { rows } = await pool.query('select id, device_name, max_ports from devices where user_id = $1', [req.user]);
    const deviceNames = rows.map(row => row.device_name);
    const deviceIds = rows.map(row => row.id);
    const maxPorts = rows.map(row => row.max_ports);
    res.status(200).json({ device_ids: deviceIds, devices: deviceNames, max_ports: maxPorts });
  } catch {
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

app.get('/device_name', authenticateToken, async (req, res) => {
  console.log("deviceName");
  try {
    const { deviceId } = req.query;
    const { rows } = await pool.query('select device_name, max_ports from devices where id=$1 and user_id=$2',
      [deviceId, req.user]);
    if (rows.length != 1) return res.sendStatus(404);
    const deviceName = rows[0].device_name;
    const maxPorts = rows[0].max_ports;
    res.status(200).json({ device_name: deviceName, max_ports: maxPorts });
  } catch {
    res.sendStatus(500);
  }
});

app.get('/peripheral_list', authenticateToken, async (req, res) => {
  console.log("periph list")
  try {
    const { deviceId } = req.query;
    const { rows } = await pool.query('select id, peripheral_number, peripheral_name from peripherals where device_id=$1 and user_id=$2',
      [deviceId, req.user]);
    const peripheralIds = rows.map(row => row.id);
    const portNums = rows.map(row => row.peripheral_number);
    const peripheralNames = rows.map(row => row.peripheral_name);
    res.status(200).json({ peripheral_ids: peripheralIds, port_nums: portNums, peripheral_names: peripheralNames });
  } catch {
    res.sendStatus(500);
  }
})

app.post('/add_device', authenticateToken, async (req, res) => {
  try {
    console.log("add device request");
    console.log(req.user);
    console.log(req.peripheral);
    const { deviceName, maxPorts } = req.body;
    console.log(deviceName);
    const ports = maxPorts || 4; // Default to 4 for multi-port devices
    const { rows } = await pool.query("insert into devices (user_id, device_name, max_ports) values ($1, $2, $3) returning id",
      [req.user, deviceName, ports]
    ); // finish token return based on device ID.
    const deviceInitToken = await createTempPeriphToken(rows[0].id);
    res.status(201).json({ token: deviceInitToken });
  } catch (err) {
    console.log(err);
    if (err.code == '23505') {
      return res.status(409).json({ error: 'Device Name in use' });
    }
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

app.post('/add_peripheral', authenticateToken, async (req, res) => {
  try {
    const { device_id, port_num, peripheral_name } = req.body;
    await pool.query("insert into peripherals (device_id, peripheral_number, peripheral_name, user_id) values ($1, $2, $3, $4)",
      [device_id, port_num, peripheral_name, req.user]
    );
    res.sendStatus(201);
  } catch (err) {
    if (err.code == '23505') return res.sendStatus(409);
    res.sendStatus(500);
  }
});

app.get('/verify_device', authenticateToken, async (req, res) => {
  console.log("device verify");
  try {
    console.log(req.peripheral);

    // Check if this is a single-port device
    const { rows: deviceRows } = await pool.query("select max_ports, user_id from devices where id=$1", [req.peripheral]);
    if (deviceRows.length === 0) {
      return res.status(404).json({ error: "Device not found" });
    }

    const maxPorts = deviceRows[0].max_ports;
    const userId = deviceRows[0].user_id;

    // For single-port devices, automatically create the peripheral if it doesn't exist
    if (maxPorts === 1) {
      const { rows: periphRows } = await pool.query(
        "select id from peripherals where device_id=$1",
        [req.peripheral]
      );

      if (periphRows.length === 0) {
        // Create default peripheral for C6 device
        await pool.query(
          "insert into peripherals (device_id, peripheral_number, peripheral_name, user_id) values ($1, 1, $2, $3)",
          [req.peripheral, "Main Blind", userId]
        );
        console.log("Created default peripheral for single-port device");
      }
    }

    await pool.query("delete from device_tokens where device_id=$1", [req.peripheral]);
    const newToken = await createPeripheralToken(req.peripheral);
    console.log("New Token", newToken);
    res.json({ token: newToken, id: req.peripheral });
  } catch (error) {
    console.error("Error in verify_device:", error);
    res.status(500).json({ error: "server error" });
  }
});

// no longer polling server - using webSocket
// app.get('/position', authenticateToken, async (req, res) => {
//   console.log("devicepos");
//   try {
//     const {rows} = await pool.query("select last_pos, peripheral_number, await_calib from peripherals where device_id=$1",
//       [req.peripheral]);

//     if (rows.length == 0) {
//       return res.sendStatus(404);
//     }

//     res.status(200).json(rows);
//   } catch {
//     res.status(500).json({error: "server error"});
//   }
// });

app.post('/manual_position_update', authenticateToken, async (req, res) => {
  console.log("setpos");
  try {
    const { periphId, periphNum, deviceId, newPos } = req.body;
    const changedPosList = [{ periphNum: periphNum, periphID: periphId, pos: newPos }];

    // Schedule the job to run immediately
    const jobId = await agenda.send('posChange', { deviceID: deviceId, changedPosList: changedPosList, userID: req.user });

    res.status(202).json({ // 202 Accepted, as processing happens in background
      success: true,
      message: 'Request accepted for immediate processing.',
      jobId: jobId
    });
  } catch (error) {
    console.error('Error triggering immediate action:', error);
    res.status(500).json({ success: false, message: 'Failed to trigger immediate action', error: error.message });
  }
});

app.post('/calib', authenticateToken, async (req, res) => {
  console.log("calibrate");
  try {
    const { periphId } = req.body;
    // Schedule the job to run immediately
    const jobId = await agenda.send('calib', { periphID: periphId, userID: req.user });

    res.status(202).json({ // 202 Accepted, as processing happens in background
      success: true,
      message: 'Request accepted for immediate processing.',
      jobId: jobId
    });
  } catch (err) {
    console.error('Error triggering immediate action:', err);
    res.sendStatus(500);
  }
})

app.post('/cancel_calib', authenticateToken, async (req, res) => {
  console.log("cancelCalib");
  try {
    const { periphId } = req.body;
    const jobId = await agenda.send('cancel_calib', { periphID: periphId, userID: req.user });

    res.status(202).json({ // 202 Accepted, as processing happens in background
      success: true,
      message: 'Request accepted for immediate processing.',
      jobId: jobId
    });
  } catch {
    res.sendStatus(500);
  }
});

app.get('/peripheral_status', authenticateToken, async (req, res) => {
  console.log("status");
  try {
    const { periphId } = req.query;
    const { rows } = await pool.query("select last_pos, last_set, calibrated, await_calib from peripherals where id=$1 and user_id=$2",
      [periphId, req.user]
    );
    if (rows.length != 1) return res.sendStatus(404);
    res.status(200).json({
      last_pos: rows[0].last_pos, last_set: rows[0].last_set,
      calibrated: rows[0].calibrated, await_calib: rows[0].await_calib
    });
  } catch {
    res.sendStatus(500);
  }
});

app.get('/peripheral_name', authenticateToken, async (req, res) => {
  console.log("urmom");
  try {
    const { periphId } = req.query;
    const { rows } = await pool.query("select peripheral_name from peripherals where id=$1 and user_id=$2",
      [periphId, req.user]
    );
    if (rows.length != 1) return res.sendStatus(404);
    res.status(200).json({ name: rows[0].peripheral_name });
  } catch {
    res.sendStatus(500);
  }
})

app.get('/device_connection_status', authenticateToken, async (req, res) => {
  console.log("device connection status");
  try {
    const { deviceId } = req.query;
    // Verify device belongs to user
    const { rows: deviceRows } = await pool.query("select id from devices where id=$1 and user_id=$2",
      [deviceId, req.user]
    );
    if (deviceRows.length != 1) return res.sendStatus(404);

    // Check if device has an active socket connection
    const { rows } = await pool.query("select connected from device_tokens where device_id=$1 and connected=TRUE",
      [deviceId]
    );
    res.status(200).json({ connected: rows.length > 0 });
  } catch {
    res.sendStatus(500);
  }
})

app.post('/completed_calib', authenticateToken, async (req, res) => {
  console.log("calibration complete");
  try {
    const { portNum } = req.body;
    const result = await pool.query("update peripherals set calibrated=true, await_calib=false where device_id=$1 and peripheral_number=$2",
      [req.peripheral, portNum]
    );
    if (result.rowCount === 0) return res.sendStatus(404);
    res.sendStatus(204);
  } catch (error) {
    console.error(error);
    res.sendStatus(500);
  }
});

app.post('/rename_device', authenticateToken, async (req, res) => {
  console.log("Hub rename");
  try {
    const { deviceId, newName } = req.body;
    const result = await pool.query("update devices set device_name=$1 where id=$2 and user_id=$3", [newName, deviceId, req.user]);
    if (result.rowCount === 0) return res.sendStatus(404);
    res.sendStatus(204);
  } catch (err) {
    if (err.code == '23505') return res.sendStatus(409);
    console.error(err);
    res.sendStatus(500);
  }
});

app.post('/rename_peripheral', authenticateToken, async (req, res) => {
  console.log("Hub rename");
  try {
    const { periphId, newName } = req.body;
    const result = await pool.query("update peripherals set peripheral_name=$1 where id=$2 and user_id=$3", [newName, periphId, req.user]);
    if (result.rowCount === 0) return res.sendStatus(404);
    res.sendStatus(204);
  } catch (err) {
    if (err.code == '23505') return res.sendStatus(409);
    console.error(err);
    res.sendStatus(500);
  }
});

app.post('/rename_group', authenticateToken, async (req, res) => {
  console.log("Rename group");
  try {
    const { groupId, newName } = req.body;
    const result = await pool.query("update groups set name=$1 where id=$2 and user_id=$3", [newName, groupId, req.user]);
    if (result.rowCount === 0) return res.status(404).json({ error: 'Group not found' });
    res.sendStatus(204);
  } catch (err) {
    if (err.code == '23505') return res.status(409).json({ error: 'Group name must be unique' });
    console.error(err);
    res.sendStatus(500);
  }
});

app.post('/update_group', authenticateToken, async (req, res) => {
  console.log("Update group members");
  try {
    const { groupId, peripheral_ids } = req.body;

    if (!groupId || !peripheral_ids || !Array.isArray(peripheral_ids)) {
      return res.status(400).json({ error: 'Invalid request parameters' });
    }

    // Verify group belongs to user
    const { rows: groupRows } = await pool.query(
      'SELECT id FROM groups WHERE id=$1 AND user_id=$2',
      [groupId, req.user]
    );

    if (groupRows.length === 0) {
      return res.status(404).json({ error: 'Group not found' });
    }

    // Verify all peripherals belong to user
    const { rows: periphRows } = await pool.query(
      'SELECT id FROM peripherals WHERE id = ANY($1::int[]) AND user_id=$2',
      [peripheral_ids, req.user]
    );

    if (periphRows.length !== peripheral_ids.length) {
      return res.status(403).json({ error: 'One or more peripherals do not belong to you' });
    }

    // Check if this exact peripheral set already exists in another group
    const { rows: duplicateRows } = await pool.query(
      `SELECT g.id, g.name
       FROM groups g
       JOIN (
         SELECT group_id, array_agg(peripheral_id ORDER BY peripheral_id) as periph_set
         FROM group_peripherals
         GROUP BY group_id
       ) gp ON g.id = gp.group_id
       WHERE gp.periph_set = $1::int[]
       AND g.user_id = $2
       AND g.id != $3`,
      [peripheral_ids.sort((a, b) => a - b), req.user, groupId]
    );

    if (duplicateRows.length > 0) {
      return res.status(409).json({
        error: 'This combination of blinds already exists in another group',
        existing_group: duplicateRows[0].name
      });
    }

    // Delete existing group_peripherals entries
    await pool.query('DELETE FROM group_peripherals WHERE group_id=$1', [groupId]);

    // Insert new group_peripherals entries
    const insertValues = peripheral_ids.map(pid => `(${groupId}, ${pid})`).join(',');
    await pool.query(`INSERT INTO group_peripherals (group_id, peripheral_id) VALUES ${insertValues}`);

    res.status(200).json({ success: true, message: 'Group updated successfully' });
  } catch (error) {
    console.error('Error updating group:', error);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

app.post('/delete_device', authenticateToken, async (req, res) => {
  console.log("delete device");
  try {
    const { deviceId } = req.body;
    const { rows } = await pool.query("delete from devices where user_id=$1 and id=$2 returning id",
      [req.user, deviceId]
    );
    if (rows.length != 1) {
      return res.status(404).json({ error: 'Device not found' });
    }

    // Get socket ID before deleting tokens
    const { rows: tokenRows } = await pool.query(
      "select socket from device_tokens where device_id=$1 and connected=TRUE",
      [rows[0].id]
    );

    // Delete device tokens
    await pool.query("delete from device_tokens where device_id=$1", [rows[0].id]);

    // Forcefully disconnect the Socket.IO connection if device is connected
    if (tokenRows.length > 0) {
      tokenRows.forEach(row => {
        if (row.socket) {
          const socket = io.sockets.sockets.get(row.socket);
          if (socket) {
            console.log(`Forcefully disconnecting device socket ${row.socket}`);
            socket.emit('device_deleted', { message: 'Device has been deleted from the account' });
            socket.disconnect(true);
          }
        }
      });
    }

    res.sendStatus(204);
  } catch (error) {
    console.error("Error deleting device:", error);
    res.status(500).json({ error: "server error" });
  }
});

app.post('/delete_peripheral', authenticateToken, async (req, res) => {
  console.log("delete peripheral");
  try {
    const { periphId } = req.body;
    const { rows } = await pool.query("delete from peripherals where user_id = $1 and id=$2 returning id",
      [req.user, periphId]
    );
    if (rows.length != 1) {
      return res.status(404).json({ error: 'Device not found' });
    }
    res.sendStatus(204);
  } catch {
    res.sendStatus(500);
  }
})

// Helper function to create cron expression from time and days
function createCronExpression(time, daysOfWeek) {
  // Sort days to ensure consistency (e.g. [1,2] matches [2,1])
  const sortedDays = [...daysOfWeek].sort((a, b) => a - b);
  const cronDays = sortedDays.join(',');
  return `${time.minute} ${time.hour} * * ${cronDays}`;
}

// Helper to check for schedule overlaps
// Returns true if an overlap is found, false otherwise
async function checkScheduleOverlap(userId, scheduleType, entityId, cronMinute, cronHour, daysOfWeek, excludeScheduleId = null) {
  try {
    const idField = scheduleType === 'peripheral' ? 'peripheral_id' : 'group_id';

    // Efficient SQL-based overlap check using array intersection (&& operator)
    // We convert the comma-separated cron_days string to an array and check overlap 
    // with the input days array.
    let query = `
      SELECT 1 
      FROM schedules 
      WHERE user_id = $1 
      AND schedule_type = $2 
      AND ${idField} = $3 
      AND cron_minute = $4 
      AND cron_hour = $5
      AND string_to_array(cron_days, ',')::int[] && $6::int[]
    `;

    // Prepare params. Note: daysOfWeek is [1, 2, 5], we pass it directly for valid int[] casting in pg
    const params = [userId, scheduleType, entityId, cronMinute, cronHour, daysOfWeek];

    if (excludeScheduleId) {
      query += ` AND id != $${params.length + 1}`;
      params.push(excludeScheduleId);
    }

    query += ' LIMIT 1'; // optimizing: we only need to know if ANY exist

    const { rows } = await pool.query(query, params);

    return rows.length > 0;
  } catch (error) {
    console.error('Error checking schedule overlap:', error);
    // In case of error, fail safe (assume overlap/error) or let DB constraint catch it?
    // Let's return false to avoid blocking valid ops during transient DB errors,
    // relying on the hard DB constraint as backstop.
    return false;
  }
}


// Helper to remove schedule from pg-boss via SQL
// We match by job name and the scheduleId stored in the job data
async function unscheduleJob(jobName, scheduleId) {
  await pool.query(
    `DELETE FROM pgboss.schedule 
     WHERE name = $1 
     AND data->>'scheduleId' = $2`,
    [jobName, scheduleId.toString()]
  );
}

app.post('/add_schedule', authenticateToken, async (req, res) => {
  console.log("add schedule");
  try {
    const { periphId, periphNum, deviceId, newPos, time, daysOfWeek } = req.body;

    // Validate required fields
    if (!periphId || periphNum === undefined || !deviceId || newPos === undefined || !time || !daysOfWeek || daysOfWeek.length === 0) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    // Create cron expression
    const cronExpression = createCronExpression(time, daysOfWeek);

    // Check for overlaps strictly (application level check)
    if (await checkScheduleOverlap(req.user, 'peripheral', periphId, time.minute.toString(), time.hour.toString(), daysOfWeek)) {
      return res.status(409).json({
        success: false,
        message: 'A schedule with overlapping days already exists for this blind at this time',
        duplicate: true
      });
    }

    // Insert into our schedules table (auto-incrementing ID)
    // Database unique index will prevent duplicates
    const { rows: scheduleRows } = await pool.query(
      `INSERT INTO schedules 
       (user_id, schedule_type, peripheral_id, device_id, peripheral_number, target_position, 
        cron_expression, cron_minute, cron_hour, cron_days) 
       VALUES ($1, 'peripheral', $2, $3, $4, $5, $6, $7, $8, $9) 
       RETURNING id`,
      [req.user, periphId, deviceId, periphNum, newPos, cronExpression,
      time.minute.toString(), time.hour.toString(), daysOfWeek.join(',')]
    );

    const scheduleId = scheduleRows[0].id;

    // Schedule with pg-boss using SHARED queue name
    const changedPosList = [{ periphNum: periphNum, periphID: periphId, pos: newPos }];
    const jobName = 'posChangeScheduled';

    await agenda.schedule(jobName, cronExpression, {
      deviceID: deviceId,
      changedPosList: changedPosList,
      userID: req.user,
      scheduleId: scheduleId // Include schedule ID for reference and deletion
    });

    res.status(201).json({
      success: true,
      message: 'Schedule created successfully',
      jobId: scheduleId
    });
  } catch (error) {
    console.error('Error creating schedule:', error);
    if (error.code === '23505') { // Unique constraint violation
      return res.status(409).json({
        success: false,
        message: 'A schedule with the same time and days already exists for this blind',
        duplicate: true
      });
    }
    res.status(500).json({ success: false, message: 'Failed to create schedule', error: error.message });
  }
});

app.post('/delete_schedule', authenticateToken, async (req, res) => {
  console.log("delete schedule");
  try {
    const { jobId } = req.body;

    if (!jobId) {
      return res.status(400).json({ error: 'Missing jobId' });
    }

    // Verify ownership
    const { rows: existingRows } = await pool.query(
      'SELECT id FROM schedules WHERE id = $1 AND user_id = $2',
      [jobId, req.user]
    );

    if (existingRows.length !== 1) {
      return res.status(404).json({ error: 'Schedule not found' });
    }

    // Unschedule from pg-boss using SQL
    await unscheduleJob('posChangeScheduled', jobId);

    // Delete from our schedules table
    await pool.query('DELETE FROM schedules WHERE id = $1', [jobId]);
    console.log("Schedule deleted successfully:", jobId);

    res.status(200).json({
      success: true,
      message: 'Schedule deleted successfully'
    });
  } catch (error) {
    console.error('Error deleting schedule:', error);
    res.status(500).json({ success: false, message: 'Failed to delete schedule', error: error.message });
  }
});

app.post('/update_schedule', authenticateToken, async (req, res) => {
  console.log("update schedule");
  console.log("Request body:", req.body);
  try {
    const { jobId, periphId, periphNum, deviceId, newPos, time, daysOfWeek } = req.body;

    console.log("jobId type:", typeof jobId, "value:", jobId);

    // Validate required fields
    if (!jobId || !periphId || periphNum === undefined || !deviceId || newPos === undefined || !time || !daysOfWeek || daysOfWeek.length === 0) {
      console.log("Missing required fields");
      return res.status(400).json({ error: 'Missing required fields' });
    }

    // Find the existing schedule in our database
    const { rows: existingRows } = await pool.query(
      'SELECT id, cron_expression FROM schedules WHERE id = $1 AND user_id = $2',
      [jobId, req.user]
    );

    if (existingRows.length !== 1) {
      console.log("Schedule not found");
      return res.status(404).json({ error: 'Schedule not found' });
    }
    const existingJob = existingRows[0];

    // Create cron expression
    const cronExpression = createCronExpression(time, daysOfWeek);

    // Check for overlap, excluding current job
    if (await checkScheduleOverlap(req.user, 'peripheral', periphId, time.minute.toString(), time.hour.toString(), daysOfWeek, jobId)) {
      return res.status(409).json({
        success: false,
        message: 'A schedule with overlapping days already exists for this blind at this time',
        duplicate: true
      });
    }

    if (cronExpression === existingJob.cron_expression) {
      // 1. Same Time: Update in place (optimizes efficiency, avoids delete+insert)
      console.log("Time unchanged, updating position in place");

      await pool.query(
        'UPDATE schedules SET target_position = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2',
        [newPos, jobId]
      );

      // We need to update the job data in pg-boss. 
      // Unscheduling and Rescheduling is safer than trying to update data json.
      await unscheduleJob('posChangeScheduled', jobId);

      const changedPosList = [{ periphNum: periphNum, periphID: periphId, pos: newPos }];
      const jobName = 'posChangeScheduled';
      await agenda.schedule(jobName, cronExpression, {
        deviceID: deviceId,
        changedPosList: changedPosList,
        userID: req.user,
        scheduleId: jobId
      });

      return res.status(200).json({
        success: true,
        message: 'Schedule updated successfully',
        jobId: jobId
      });
    } else {
      // 2. Different Time: Insert new, then delete old (Safe replacement)
      console.log("Time changed, inserting new schedule before deleting old");

      try {
        // Insert new schedule (DB Unique Index checks duplicates for us)
        const { rows: scheduleRows } = await pool.query(
          `INSERT INTO schedules 
           (user_id, schedule_type, peripheral_id, device_id, peripheral_number, target_position, 
            cron_expression, cron_minute, cron_hour, cron_days) 
           VALUES ($1, 'peripheral', $2, $3, $4, $5, $6, $7, $8, $9) 
           RETURNING id`,
          [req.user, periphId, deviceId, periphNum, newPos, cronExpression,
          time.minute.toString(), time.hour.toString(), daysOfWeek.join(',')]
        );

        const newScheduleId = scheduleRows[0].id;

        // Schedule new job
        const changedPosList = [{ periphNum: periphNum, periphID: periphId, pos: newPos }];
        const newJobName = 'posChangeScheduled';
        await agenda.schedule(newJobName, cronExpression, {
          deviceID: deviceId,
          changedPosList: changedPosList,
          userID: req.user,
          scheduleId: newScheduleId
        });

        // Now safe to delete old schedule
        console.log("New schedule created, deleting old:", jobId);
        await unscheduleJob('posChangeScheduled', jobId);
        await pool.query('DELETE FROM schedules WHERE id = $1', [jobId]);

        return res.status(200).json({
          success: true,
          message: 'Schedule updated successfully',
          jobId: newScheduleId
        });

      } catch (error) {
        if (error.code === '23505') {
          return res.status(409).json({
            success: false,
            message: 'A schedule with the same time and days already exists for this blind',
            duplicate: true
          });
        }
        throw error;
      }
    }
  } catch (error) {
    console.error('Error updating schedule:', error);
    if (error.code === '23505') { // Unique constraint violation
      return res.status(409).json({
        success: false,
        message: 'A schedule with the same time and days already exists for this blind',
        duplicate: true
      });
    }
    res.status(500).json({ success: false, message: 'Failed to update schedule', error: error.message });
  }
});

server.listen(port, () => {
  console.log(`Example app listening at http://localhost:${port}`);
});

app.post('/periph_schedule_list', authenticateToken, async (req, res) => {
  try {
    console.log("Schedule List request for user:", req.user);
    const { periphId } = req.body;

    if (!periphId) {
      return res.status(400).json({ error: 'periphId is required in the request body.' });
    }

    // Query our schedules table
    const { rows: schedules } = await pool.query(
      `SELECT id, target_position, cron_minute, cron_hour, cron_days, cron_expression
       FROM schedules 
       WHERE schedule_type = 'peripheral' 
       AND peripheral_id = $1
       AND user_id = $2`,
      [periphId, req.user]
    );

    // Parse the schedule data and build response
    const details = schedules.map(schedule => {
      try {
        const interval = cronParser.parseExpression(schedule.cron_expression);
        const fields = interval.fields;

        const parsedSchedule = {
          minutes: fields.minute,
          hours: fields.hour,
          daysOfMonth: fields.dayOfMonth,
          months: fields.month,
          daysOfWeek: fields.dayOfWeek,
        };

        return {
          id: schedule.id, // Auto-incrementing ID from database
          schedule: parsedSchedule,
          pos: schedule.target_position
        };
      } catch (err) {
        console.error(`Could not parse cron "${schedule.cron_expression}" for schedule ${schedule.id}. Skipping.`, err);
        return null;
      }
    }).filter(detail => detail !== null);

    res.status(200).json({ scheduledUpdates: details });

  } catch (error) {
    console.error("Error in /periph_schedule_list:", error);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

app.get('/group_list', authenticateToken, async (req, res) => {
  console.log("group_list request for user:", req.user);
  try {
    // Get all groups for the user - no joins needed for menu view
    const { rows } = await pool.query(
      'SELECT id, name, created_at FROM groups WHERE user_id = $1 ORDER BY name',
      [req.user]
    );

    res.status(200).json({ groups: rows });
  } catch (error) {
    console.error("Error in /group_list:", error);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

app.post('/add_group', authenticateToken, async (req, res) => {
  console.log("add_group request for user:", req.user);
  try {
    const { name, peripheral_ids } = req.body;

    // Validate input
    if (!name || !name.trim()) {
      return res.status(400).json({ error: 'Group name is required' });
    }
    // if (!peripheral_ids || !Array.isArray(peripheral_ids) || peripheral_ids.length < 2) {

    if (!peripheral_ids || !Array.isArray(peripheral_ids)) {
      return res.status(400).json({ error: 'At least 2 peripherals are required' });
    }

    // Verify all peripherals belong to the user
    const verifyQuery = `
      SELECT id FROM peripherals 
      WHERE id = ANY($1) AND user_id = $2
    `;
    const { rows: verifiedPeripherals } = await pool.query(verifyQuery, [peripheral_ids, req.user]);

    if (verifiedPeripherals.length !== peripheral_ids.length) {
      return res.status(403).json({ error: 'Invalid peripheral IDs' });
    }

    // Sort peripheral IDs for consistent comparison
    const sortedPeripheralIds = [...peripheral_ids].sort((a, b) => a - b);

    // Check if a group with this exact set of peripherals already exists
    const duplicateCheckQuery = `
      SELECT g.id, g.name
      FROM groups g
      JOIN group_peripherals gp ON g.id = gp.group_id
      WHERE g.user_id = $1
      GROUP BY g.id
      HAVING array_agg(gp.peripheral_id ORDER BY gp.peripheral_id) = $2::int[]
    `;
    const { rows: duplicateGroups } = await pool.query(duplicateCheckQuery, [req.user, sortedPeripheralIds]);

    if (duplicateGroups.length > 0) {
      return res.status(409).json({
        error: `A group named "${duplicateGroups[0].name}" already contains these exact blinds`,
        duplicate: true,
        existing_group_name: duplicateGroups[0].name
      });
    }

    // Insert into groups table
    const insertGroupQuery = `
      INSERT INTO groups (user_id, name) 
      VALUES ($1, $2) 
      RETURNING id
    `;
    const { rows: groupRows } = await pool.query(insertGroupQuery, [req.user, name.trim()]);
    const groupId = groupRows[0].id;

    // Insert into group_peripherals table
    const insertPeripheralsQuery = `
      INSERT INTO group_peripherals (group_id, peripheral_id) 
      VALUES ${peripheral_ids.map((_, i) => `($1, $${i + 2})`).join(', ')}
    `;
    await pool.query(insertPeripheralsQuery, [groupId, ...peripheral_ids]);

    res.status(201).json({
      success: true,
      group_id: groupId,
      message: 'Group created successfully'
    });
  } catch (error) {
    console.error("Error in /add_group:", error);

    // Handle unique constraint violation (duplicate group name)
    if (error.code === '23505') {
      return res.status(409).json({ error: 'A group with this name already exists' });
    }

    res.status(500).json({ error: 'Internal Server Error' });
  }
});

app.post('/delete_group', authenticateToken, async (req, res) => {
  console.log("delete_group request for user:", req.user);
  try {
    const { groupId } = req.body;

    if (!groupId) {
      return res.status(400).json({ error: 'Group ID is required' });
    }

    // Delete the group - CASCADE will automatically delete group_peripherals entries
    const { rows } = await pool.query(
      'DELETE FROM groups WHERE id = $1 AND user_id = $2 RETURNING id, name',
      [groupId, req.user]
    );

    if (rows.length === 0) {
      return res.status(404).json({ error: 'Group not found' });
    }

    console.log(`Group "${rows[0].name}" deleted successfully`);
    res.sendStatus(204);
  } catch (error) {
    console.error("Error in /delete_group:", error);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

app.get('/group_details', authenticateToken, async (req, res) => {
  console.log("group_details request for user:", req.user);
  try {
    const { groupId } = req.query;

    if (!groupId) {
      return res.status(400).json({ error: 'Group ID is required' });
    }

    // Get group info and all peripherals with their device info
    const { rows } = await pool.query(`
      SELECT 
        g.id as group_id,
        g.name as group_name,
        json_agg(
          json_build_object(
            'peripheral_id', p.id,
            'peripheral_name', p.peripheral_name,
            'peripheral_number', p.peripheral_number,
            'device_id', p.device_id,
            'last_pos', p.last_pos,
            'calibrated', p.calibrated
          ) ORDER BY p.peripheral_name
        ) as peripherals
      FROM groups g
      JOIN group_peripherals gp ON g.id = gp.group_id
      JOIN peripherals p ON gp.peripheral_id = p.id
      WHERE g.id = $1 AND g.user_id = $2
      GROUP BY g.id, g.name
    `, [groupId, req.user]);

    if (rows.length === 0) {
      return res.status(404).json({ error: 'Group not found' });
    }

    res.status(200).json(rows[0]);
  } catch (error) {
    console.error("Error in /group_details:", error);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

app.post('/group_position_update', authenticateToken, async (req, res) => {
  console.log("group_position_update request for user:", req.user);
  try {
    const { groupId, newPos } = req.body;

    if (!groupId || newPos === undefined) {
      return res.status(400).json({ error: 'Group ID and position are required' });
    }

    // Get all peripherals in the group with their device info
    const { rows: peripherals } = await pool.query(`
      SELECT p.id, p.peripheral_number, p.device_id
      FROM group_peripherals gp
      JOIN peripherals p ON gp.peripheral_id = p.id
      JOIN groups g ON gp.group_id = g.id
      WHERE g.id = $1 AND g.user_id = $2
    `, [groupId, req.user]);

    if (peripherals.length === 0) {
      return res.status(404).json({ error: 'Group not found or empty' });
    }

    // Group peripherals by device to send commands efficiently
    const deviceMap = new Map();
    peripherals.forEach(p => {
      if (!deviceMap.has(p.device_id)) {
        deviceMap.set(p.device_id, []);
      }
      deviceMap.get(p.device_id).push({
        periphNum: p.peripheral_number,
        periphID: p.id,
        pos: newPos
      });
    });

    // Schedule position change jobs for each device
    const jobPromises = Array.from(deviceMap.entries()).map(([deviceId, changedPosList]) =>
      agenda.send('posChange', { deviceID: deviceId, changedPosList, userID: req.user })
    );

    await Promise.all(jobPromises);

    res.status(202).json({
      success: true,
      message: 'Group position update accepted',
      peripherals_updated: peripherals.length
    });
  } catch (error) {
    console.error("Error in /group_position_update:", error);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

app.post('/add_group_schedule', authenticateToken, async (req, res) => {
  console.log("add_group_schedule");
  try {
    const { groupId, newPos, time, daysOfWeek } = req.body;

    if (!groupId || newPos === undefined || !time || !daysOfWeek || !Array.isArray(daysOfWeek)) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    // Verify group belongs to user
    const { rows: groupRows } = await pool.query(
      'SELECT id FROM groups WHERE id=$1 AND user_id=$2',
      [groupId, req.user]
    );

    if (groupRows.length === 0) {
      return res.status(404).json({ error: 'Group not found' });
    }

    // Check for overlaps strictly
    if (await checkScheduleOverlap(req.user, 'group', groupId, time.minute.toString(), time.hour.toString(), daysOfWeek)) {
      return res.status(409).json({
        success: false,
        error: 'A schedule with overlapping days already exists for this group at this time'
      });
    }

    // Create cron expression and insert into our schedules table
    // Database unique index will prevent duplicates
    const cronExpression = createCronExpression(time, daysOfWeek);
    const { rows: scheduleRows } = await pool.query(
      `INSERT INTO schedules 
       (user_id, schedule_type, group_id, target_position, 
        cron_expression, cron_minute, cron_hour, cron_days) 
       VALUES ($1, 'group', $2, $3, $4, $5, $6, $7) 
       RETURNING id`,
      [req.user, groupId, newPos, cronExpression,
      time.minute.toString(), time.hour.toString(), daysOfWeek.join(',')]
    );

    const scheduleId = scheduleRows[0].id;

    // Schedule with pg-boss using SHARED queue name
    const jobName = 'groupPosChangeScheduled';
    await agenda.schedule(jobName, cronExpression, {
      groupID: groupId,
      newPos,
      userID: req.user,
      scheduleId: scheduleId
    });

    res.status(201).json({
      success: true,
      message: 'Group schedule created successfully',
      jobId: scheduleId
    });
  } catch (error) {
    console.error('Error creating group schedule:', error);
    if (error.code === '23505') { // Unique constraint violation
      return res.status(409).json({
        success: false,
        error: 'A schedule already exists for this group at this time'
      });
    }
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/delete_group_schedule', authenticateToken, async (req, res) => {
  console.log("delete_group_schedule");
  try {
    const { jobId } = req.body;

    if (!jobId) {
      return res.status(400).json({ error: 'Missing jobId' });
    }

    // Verify ownership
    const { rows: existingRows } = await pool.query(
      'SELECT id FROM schedules WHERE id = $1 AND user_id = $2',
      [jobId, req.user]
    );

    if (existingRows.length !== 1) {
      return res.status(404).json({ error: 'Schedule not found' });
    }

    // Unschedule from pg-boss using SQL
    await unscheduleJob('groupPosChangeScheduled', jobId);

    await pool.query('DELETE FROM schedules WHERE id = $1', [jobId]);
    res.status(200).json({ success: true, message: 'Group schedule deleted successfully' });
  } catch (error) {
    console.error('Error deleting group schedule:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/update_group_schedule', authenticateToken, async (req, res) => {
  console.log("update_group_schedule");
  try {
    const { jobId, newPos, time, daysOfWeek } = req.body;

    if (!jobId || newPos === undefined || !time || !daysOfWeek) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    // Find the existing schedule in our database
    const { rows: existingRows } = await pool.query(
      'SELECT id, group_id, cron_expression FROM schedules WHERE id = $1 AND user_id = $2',
      [jobId, req.user]
    );

    if (existingRows.length !== 1) {
      return res.status(404).json({ error: 'Schedule not found' });
    }
    const existingJob = existingRows[0];
    const groupId = existingJob.group_id;

    // Check for overlaps, excluding current job
    if (await checkScheduleOverlap(req.user, 'group', groupId, time.minute.toString(), time.hour.toString(), daysOfWeek, jobId)) {
      return res.status(409).json({
        success: false,
        error: 'A schedule with overlapping days already exists for this group at this time'
      });
    }

    const cronExpression = createCronExpression(time, daysOfWeek);

    if (cronExpression === existingJob.cron_expression) {
      // 1. Same Time: Update in place
      console.log("Time unchanged, updating group position in place");

      await pool.query(
        'UPDATE schedules SET target_position = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2',
        [newPos, jobId]
      );

      // Update schedule job data via Unschedule+Reschedule
      await unscheduleJob('groupPosChangeScheduled', jobId);

      const jobName = 'groupPosChangeScheduled';
      await agenda.schedule(jobName, cronExpression, {
        groupID: groupId,
        newPos,
        userID: req.user,
        scheduleId: jobId
      });

      return res.status(200).json({
        success: true,
        message: 'Group schedule updated successfully',
        jobId: jobId
      });
    } else {
      // 2. Different Time: Insert new, then delete old
      console.log("Time changed, inserting new group schedule before deleting old");

      try {
        // Insert new schedule
        const { rows: scheduleRows } = await pool.query(
          `INSERT INTO schedules 
           (user_id, schedule_type, group_id, target_position, 
            cron_expression, cron_minute, cron_hour, cron_days) 
           VALUES ($1, 'group', $2, $3, $4, $5, $6, $7) 
           RETURNING id`,
          [req.user, groupId, newPos, cronExpression,
          time.minute.toString(), time.hour.toString(), daysOfWeek.join(',')]
        );

        const newScheduleId = scheduleRows[0].id;

        // Schedule new job
        const newJobName = 'groupPosChangeScheduled';
        await agenda.schedule(newJobName, cronExpression, {
          groupID: groupId,
          newPos,
          userID: req.user,
          scheduleId: newScheduleId
        });

        // Delete old schedule
        console.log("Deleting old group schedule:", jobId);
        await unscheduleJob('groupPosChangeScheduled', jobId);
        await pool.query('DELETE FROM schedules WHERE id = $1', [jobId]);

        return res.status(200).json({
          success: true,
          message: 'Group schedule updated successfully',
          jobId: newScheduleId
        });
      } catch (error) {
        if (error.code === '23505') {
          return res.status(409).json({
            success: false,
            error: 'A schedule already exists for this group at this time'
          });
        }
        throw error;
      }
    }
  } catch (error) {
    console.error('Error updating group schedule:', error);
    if (error.code === '23505') { // Unique constraint violation
      return res.status(409).json({
        success: false,
        error: 'A schedule already exists for this group at this time'
      });
    }
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/group_schedule_list', authenticateToken, async (req, res) => {
  try {
    const { groupId } = req.body;

    if (!groupId) {
      return res.status(400).json({ error: 'Missing groupId' });
    }

    // Verify group belongs to user
    const { rows: groupRows } = await pool.query(
      'SELECT id FROM groups WHERE id=$1 AND user_id=$2',
      [groupId, req.user]
    );

    if (groupRows.length === 0) {
      return res.status(404).json({ error: 'Group not found' });
    }

    // Query our schedules table
    const { rows: schedules } = await pool.query(
      `SELECT id, target_position, cron_minute, cron_hour, cron_days, cron_expression
       FROM schedules 
       WHERE schedule_type = 'group' 
       AND group_id = $1
       AND user_id = $2`,
      [groupId, req.user]
    );

    const scheduledUpdates = schedules.map(schedule => {
      const interval = cronParser.parseExpression(schedule.cron_expression);
      const parsedSchedule = {
        minutes: interval.fields.minute,
        hours: interval.fields.hour,
        daysOfWeek: interval.fields.dayOfWeek
      };

      return {
        id: schedule.id, // Auto-incrementing ID from database
        pos: schedule.target_position,
        schedule: parsedSchedule
      };
    });

    res.status(200).json({ scheduledUpdates });
  } catch (error) {
    console.error('Error fetching group schedules:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

