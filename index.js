const { verify, hash } = require('argon2');
const express = require('express');
const { json } = require('express');
const { Pool } = require('pg');
require('dotenv').config();
const jwt = require('jsonwebtoken');
const http = require('http');
const socketIo = require('socket.io');
const connectDB = require('./db'); // Your Mongoose DB connection
const { initializeAgenda } = require('./agenda'); // Agenda setup
const format = require('pg-format');
const cronParser = require('cron-parser');

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
    // 1. Connect to MongoDB (for your application data)
    await connectDB();
    
    agenda = await initializeAgenda('mongodb://localhost:27017/myScheduledApp', pool, io);
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
      const successResponse = {
        type: 'success',
        code: 200,
        message: 'Device found'
      };
      console.log("success");
      socket.emit("success", successResponse);
    }

  } catch (error) {
    console.error('Error during periph authentication:', error);

    // Send an error message to the client
    socket.emit('error', { type: 'error', code: 500 });

    // Disconnect the client
    socket.disconnect(true);
  }

  socket.on('calib_done', async (data) => {
    console.log(`Received 'calib_done' event from client ${socket.id}:`);
    console.log(data); 
    try {
      const {rows} = await pool.query("select device_id from device_tokens where socket=$1 and connected=TRUE", [socket.id]);
      if (rows.length != 1) throw new Error("No device with that ID connected to Socket.");
      const result = await pool.query("update peripherals set await_calib=FALSE, calibrated=TRUE where device_id=$1 and peripheral_number=$2 returning id, user_id",
        [rows[0].device_id, data.port]
      );
      if (result.rowCount != 1) throw new Error("No such peripheral in database");

      const {rows: userRows} = await pool.query("select socket from user_tokens where user_id=$1", [result.rows[0].user_id]);
      if (userRows.length == 1) {
        if (userRows[0]){
          const userSocket = userRows[0].socket;
          io.to(userSocket).emit("calib_done", {periphID: result.rows[0].id});
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
      const {rows} = await pool.query("select device_id from device_tokens where socket=$1 and connected=TRUE", [socket.id]);
      if (rows.length != 1) throw new Error("No device with that ID connected to Socket.");
      const result = await pool.query("update peripherals set last_pos=$1, last_set=$2 where device_id=$3 and peripheral_number=$4 returning id, user_id",
        [data.pos, dateTime, rows[0].device_id, data.port]
      );
      if (result.rowCount != 1) throw new Error("No such peripheral in database");

      const {rows: userRows} = await pool.query("select socket from user_tokens where user_id=$1", [result.rows[0].user_id]);
      if (userRows.length == 1) {
        if (userRows[0]){
          const userSocket = userRows[0].socket;
          io.to(userSocket).emit("pos_hit", {periphID: result.rows[0].id});
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
      await pool.query("update device_tokens set connected=FALSE where socket=$1", [socket.id]);
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
  const token = jwt.sign({type: 'peripheral', peripheralId}, JWT_SECRET, {expiresIn: '2m'} );
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
      const {rows} = await pool.query("select user_id from user_tokens where token=$1", [token]);
      if (rows.length != 1) throw new Error("Invalid/Expired Token");
      req.user = payload.userId; // make Id accessible in route handlers
    }
    else if (payload.type === 'peripheral'){
      const {rows} = await pool.query("select device_id from device_tokens where token=$1", [token]);
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
  if (!email || !password) return res.status(400).json({error: 'email and password required'});
  try {
    const {rows} = await pool.query('select id, password_hash_string from users where email = $1', [email]);
    if (rows.length === 0) return res.status(401).json({error: 'Invalid Credentials'});
    const user = rows[0]
    console.log('user found');
    const verified = await verify(user.password_hash_string, password);

    if (!verified) return res.status(401).json({ error: 'Invalid credentials' });
    console.log("password correct");
    const token = await createToken(user.id); // token is now tied to ID

    res.status(200).json({token});
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/create_user', async (req, res) => {
  console.log("got post req");
  const {name, email, password} = req.body
  try {
    
    const hashedPass = await hash(password);

    await pool.query("insert into users (name, email, password_hash_string) values (nullif($1, ''), $2, $3)",
      [name, email,  hashedPass]
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
    res.status(200).json({token: newToken});
  } catch {
    res.status(500).json({ error: 'server error' });
  }
});

app.get('/device_list', authenticateToken, async (req, res) => {
  try {
    console.log("device List request");
    console.log(req.user);
    const {rows} = await pool.query('select id, device_name from devices where user_id = $1', [req.user]);
    const deviceNames = rows.map(row => row.device_name);
    const deviceIds = rows.map(row => row.id);
    res.status(200).json({ device_ids: deviceIds, devices: deviceNames });
  } catch {
    res.status(500).json({error: 'Internal Server Error'});
  }
});

app.get('/device_name', authenticateToken, async (req, res) => {
  console.log("deviceName");
  try {
    const {deviceId} = req.query;
    const {rows} = await pool.query('select device_name from devices where id=$1 and user_id=$2',
      [deviceId, req.user]);
    if (rows.length != 1) return res.sendStatus(404);
    const deviceName = rows[0].device_name;
    res.status(200).json({device_name: deviceName});
  } catch {
    res.sendStatus(500);
  }
});

app.get('/peripheral_list', authenticateToken, async (req, res) => {
  console.log("periph list")
  try {
    const {deviceId} = req.query;
    const {rows} = await pool.query('select id, peripheral_number, peripheral_name from peripherals where device_id=$1 and user_id=$2',
      [deviceId, req.user]);
    const peripheralIds = rows.map(row => row.id);
    const portNums = rows.map(row => row.peripheral_number);
    const peripheralNames = rows.map(row => row.peripheral_name);
    res.status(200).json({peripheral_ids: peripheralIds, port_nums: portNums, peripheral_names: peripheralNames});
  } catch {
    res.sendStatus(500);
  }
})

app.post('/add_device', authenticateToken, async (req, res) => {
  try {
    console.log("add device request");
    console.log(req.user);
    console.log(req.peripheral);
    const {deviceName} = req.body;
    console.log(deviceName);
    const {rows} = await pool.query("insert into devices (user_id, device_name) values ($1, $2) returning id",
      [req.user, deviceName]
    ); // finish token return based on device ID.
    const deviceInitToken = await createTempPeriphToken(rows[0].id);
    res.status(201).json({token: deviceInitToken});
  } catch (err) {
    console.log(err);
    if (err.code == '23505') {
      return res.status(409).json({ error: 'Device Name in use' });
    }
    res.status(500).json({error: 'Internal Server Error'});
  }
});

app.post('/add_peripheral', authenticateToken, async (req, res) => {
  try {
    const {device_id, port_num, peripheral_name} = req.body;
    await pool.query("insert into peripherals (device_id, peripheral_number, peripheral_name, user_id) values ($1, $2, $3, $4)",
      [device_id, port_num, peripheral_name, req.user]
    );
    res.sendStatus(201);
  } catch (err){
    if (err.code == '23505') return res.sendStatus(409);
    res.sendStatus(500);
  }
});

app.get('/verify_device', authenticateToken, async (req, res) => {
  console.log("device verify");
  try{
    console.log(req.peripheral);
    await pool.query("delete from device_tokens where device_id=$1", [req.peripheral]);
    const newToken = await createPeripheralToken(req.peripheral);
    console.log("New Token", newToken);
    res.json({token: newToken, id: req.peripheral});
  } catch {
    res.status(500).json({error: "server error"});
  }
});

app.get('/position', authenticateToken, async (req, res) => {
  console.log("devicepos");
  try {
    const {rows} = await pool.query("select last_pos, peripheral_number, await_calib from peripherals where device_id=$1",
      [req.peripheral]);

    if (rows.length == 0) {
      return res.sendStatus(404);
    }

    res.status(200).json(rows);
  } catch {
    res.status(500).json({error: "server error"});
  }
});

app.post('/manual_position_update', authenticateToken, async (req, res) => {
  console.log("setpos");
  try {
    const {periphId, periphNum, deviceId, newPos} = req.body;
    const changedPosList = [{periphNum: periphNum, periphID: periphId, pos: newPos}];

    // Schedule the job to run immediately
    const job = await agenda.now('posChange', {deviceID: deviceId, changedPosList: changedPosList, userID: req.user});

    res.status(202).json({ // 202 Accepted, as processing happens in background
      success: true,
      message: 'Request accepted for immediate processing.',
      jobId: job.attrs._id
    });
  } catch (error) {
    console.error('Error triggering immediate action:', error);
    res.status(500).json({ success: false, message: 'Failed to trigger immediate action', error: error.message });
  }
});

app.post('/calib', authenticateToken, async (req, res) => {
  console.log("calibrate");
  try {
    const {periphId} = req.body;
    // Schedule the job to run immediately
    const job = await agenda.now('calib', {periphID: periphId, userID: req.user});

    res.status(202).json({ // 202 Accepted, as processing happens in background
      success: true,
      message: 'Request accepted for immediate processing.',
      jobId: job.attrs._id
    });
  } catch (err) {
    console.error('Error triggering immediate action:', err);
    res.sendStatus(500);
  }
})

app.post('/cancel_calib', authenticateToken, async (req, res) => {
  console.log("cancelCalib");
  try {
    const {periphId} = req.body;
    const job = await agenda.now('cancel_calib', {periphID: periphId, userID: req.user});

    res.status(202).json({ // 202 Accepted, as processing happens in background
      success: true,
      message: 'Request accepted for immediate processing.',
      jobId: job.attrs._id
    });
  } catch {
    res.sendStatus(500);
  }
});

app.get('/peripheral_status', authenticateToken, async (req, res) => {
  console.log("status");
  try {
    const {periphId} = req.query;
    const {rows} = await pool.query("select last_pos, last_set, calibrated, await_calib from peripherals where id=$1 and user_id=$2",
      [periphId, req.user]
    );
    if (rows.length != 1) return res.sendStatus(404);
    res.status(200).json({last_pos: rows[0].last_pos, last_set: rows[0].last_set,
      calibrated: rows[0].calibrated, await_calib: rows[0].await_calib});
  } catch {
    res.sendStatus(500);
  }
});

app.get('/peripheral_name', authenticateToken, async (req, res) => {
  console.log("urmom");
  try {
    const {periphId} = req.query;
    const {rows} = await pool.query("select peripheral_name from peripherals where id=$1 and user_id=$2",
      [periphId, req.user]
    );
    if (rows.length != 1) return res.sendStatus(404);
    res.status(200).json({name: rows[0].peripheral_name});
  } catch {
    res.sendStatus(500);
  }
})

app.post('/completed_calib', authenticateToken, async (req, res) => {
  console.log("calibration complete");
  try {
    const {portNum} = req.body;
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
    const {deviceId, newName} = req.body;
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
    const {periphId, newName} = req.body;
    const result = await pool.query("update peripherals set peripheral_name=$1 where id=$2 and user_id=$3", [newName, periphId, req.user]);
    if (result.rowCount === 0) return res.sendStatus(404);
    res.sendStatus(204);
  } catch (err) {
    if (err.code == '23505') return res.sendStatus(409);
    console.error(err);
    res.sendStatus(500);
  }
});

app.post('/delete_device', authenticateToken, async (req, res) => {
  console.log("delete device");
  try {
    const {deviceId} = req.body;
    const {rows} = await pool.query("delete from devices where user_id=$1 and id=$2 returning id",
      [req.user, deviceId]
    );
    if (rows.length != 1) {
      return res.status(404).json({ error: 'Device not found' });
    }

    await pool.query("delete from device_tokens where device_id=$1", [rows[0].id]);

    res.sendStatus(204);
  } catch {
    res.status(500).json({error: "server error"});
  }
});

app.post('/delete_peripheral', authenticateToken, async (req, res) => {
  console.log("delete peripheral");
  try {
    const {periphId} = req.body;
    const {rows} = await pool.query("delete from peripherals where user_id = $1 and id=$2 returning id",
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

    // FIX 1: Assign the returned array directly to a variable (e.g., 'jobs')
    const jobs = await agenda.jobs({
      'name': 'posChangeScheduled',
      'data.changedPosList': { $size: 1 }, 
      'data.changedPosList.0.periphID': periphId,
      'data.userID': req.user
    });

    // FIX 2: Use .filter() and .map() to handle all cases cleanly.
    // This creates a cleaner, more predictable transformation pipeline.
    const details = jobs
      // Step 1: Filter out any jobs that are not recurring.
      .filter(job => job.attrs.repeatInterval) 
      // Step 2: Map the remaining recurring jobs to our desired format.
      .map(job => {
        const { repeatInterval, data, repeatTimezone } = job.attrs;
        try {
          const interval = cronParser.parseExpression(repeatInterval, {
            tz: repeatTimezone || undefined 
          });
          const fields = interval.fields;

          // Make sure to declare the variable
          const parsedSchedule = {
            minutes: fields.minute,
            hours: fields.hour,
            daysOfMonth: fields.dayOfMonth,
            months: fields.month,
            daysOfWeek: fields.dayOfWeek,
          };

          return {
            id: job.attrs._id, // It's good practice to return the job ID
            schedule: parsedSchedule,
            pos: data.changedPosList[0].pos
          };
        } catch (err) {
          // If parsing fails for a specific job, log it and filter it out.
          console.error(`Could not parse "${repeatInterval}" for job ${job.attrs._id}. Skipping.`);
          return null; // Return null for now
        }
      })
      // Step 3: Filter out any nulls that resulted from a parsing error.
      .filter(detail => detail !== null);

    res.status(200).json({ scheduledUpdates: details });

  } catch (error) { // FIX 3: Capture and log the actual error
    console.error("Error in /periph_schedule_list:", error);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});
