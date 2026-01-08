const { verify, hash } = require('argon2');
const express = require('express');
const { json } = require('express');
const { Pool } = require('pg');
require('dotenv').config();
const jwt = require('jsonwebtoken');
const http = require('http');
const socketIo = require('socket.io');
const connectDB = require('./db');
const { initializeAgenda } = require('./agenda'); // Agenda setup (CHANGE THIS FOR BOSS)
const format = require('pg-format');
const cronParser = require('cron-parser');
const { ObjectId } = require('mongodb');
const { RateLimiterMemory } = require('rate-limiter-flexible');
const crypto = require('crypto');
const { sendVerificationEmail, sendPasswordResetEmail } = require('./mailer');

// HTTP rate limiter: 5 requests per second per IP
const httpRateLimiter = new RateLimiterMemory({
  points: 5, 
  duration: 1, 
});

// Auth endpoints rate limiter: 10 attempts per hour per IP
const authRateLimiter = new RateLimiterMemory({
  points: 10,
  duration: 3600, // 1 hour in seconds
});

// Resend verification email rate limiter: 1 request per 20 seconds per IP
const resendVerificationRateLimiter = new RateLimiterMemory({
  points: 1,
  duration: 20,
});

// Password reset rate limiter: 10 attempts per 15 minutes per IP
const passwordResetRateLimiter = new RateLimiterMemory({
  points: 10,
  duration: 900, // 15 minutes in seconds
});

// WebSocket connection rate limiter: 1 connection per second per IP
const wsConnectionRateLimiter = new RateLimiterMemory({
  points: 5,
  duration: 1,
});

// WebSocket message rate limiter: 5 messages per second per socket
const wsMessageRateLimiter = new RateLimiterMemory({
  points: 5,
  duration: 1,
});

const app = express();
const port = 3000;
app.use(json());

// Rate limiting middleware for HTTP requests
app.use(async (req, res, next) => {
  const ip = req.ip || req.connection.remoteAddress;
  try {
    await httpRateLimiter.consume(ip);
    next();
  } catch (rejRes) {
    res.status(429).json({ error: 'Too many requests' });
  }
});

const pool = new Pool();
const server = http.createServer(app);
const io = socketIo(server, {
  pingInterval: 15000,
  pingTimeout: 10000,
});

let agenda;

(async () => {
    // 1. Connect to MongoDB
    await connectDB();
    agenda = await initializeAgenda('mongodb://localhost:27017/myScheduledApp', pool, io);
})();

(async () => {
  await pool.query("update user_tokens set connected=FALSE where connected=TRUE");
  await pool.query("update device_tokens set connected=FALSE where connected=TRUE");
  
  // Clear all expired password reset tokens on startup
  await pool.query("DELETE FROM password_reset_tokens WHERE expires_at < NOW()");
  console.log("Cleared expired password reset tokens");
  
  // Clear all expired pending email changes on startup
  await pool.query("DELETE FROM user_pending_emails WHERE expires_at < NOW()");
  console.log("Cleared expired pending email changes");
})();
const JWT_SECRET = process.env.JWT_SECRET;
const TOKEN_EXPIRY = '5d';

// Helper function to rate limit WebSocket messages
async function rateLimitSocketMessage(socket, eventName) {
  try {
    await wsMessageRateLimiter.consume(socket.id);
    return true;
  } catch (rejRes) {
    socket.emit('error', { 
      type: 'error', 
      code: 429, 
      message: 'Too many messages. Please slow down.',
      event: eventName 
    });
    return false;
  }
}

io.on('connection', async (socket) => {
  console.log("periph connected");
  
  // Rate limit WebSocket connections by IP
  const ip = socket.handshake.address;
  try {
    await wsConnectionRateLimiter.consume(ip);
  } catch (rejRes) {
    socket.emit('error', { 
      type: 'error', 
      code: 429, 
      message: 'Too many connection attempts. Please wait.' 
    });
    socket.disconnect(true);
    return;
  }
  
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
          const {rows: periphRows} = await pool.query(
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
          const {rows: deviceUserRows} = await pool.query("select user_id from devices where id=$1", [id]);
          if (deviceUserRows.length === 1) {
            const {rows: userRows} = await pool.query("select socket from user_tokens where user_id=$1 and connected=TRUE", [deviceUserRows[0].user_id]);
            if (userRows.length === 1 && userRows[0]) {
              io.to(userRows[0].socket).emit("device_connected", {deviceID: id});
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
    if (!await rateLimitSocketMessage(socket, 'report_calib_status')) return;
    
    console.log(`Device reporting calibration status: ${socket.id}`);
    console.log(data);
    try {
      const {rows} = await pool.query("select device_id from device_tokens where socket=$1 and connected=TRUE", [socket.id]);
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
    if (!await rateLimitSocketMessage(socket, 'device_calib_error')) return;
    
    console.log(`Device reporting calibration error: ${socket.id}`);
    console.log(data);
    try {
      const {rows} = await pool.query("select device_id from device_tokens where socket=$1 and connected=TRUE", [socket.id]);
      if (rows.length != 1) throw new Error("No device with that ID connected to Socket.");
      
      const result = await pool.query(
        "update peripherals set await_calib=FALSE where device_id=$1 and peripheral_number=$2 returning id, user_id",
        [rows[0].device_id, data.port]
      );
      if (result.rowCount != 1) throw new Error("No such peripheral in database");
      
      // Notify user app about the error
      const {rows: userRows} = await pool.query("select socket from user_tokens where user_id=$1 and connected=TRUE", [result.rows[0].user_id]);
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
    if (!await rateLimitSocketMessage(socket, 'calib_stage1_ready')) return;
    
    console.log(`Device ready for stage 1 (tilt up): ${socket.id}`);
    console.log(data);
    try {
      const {rows} = await pool.query("select device_id from device_tokens where socket=$1 and connected=TRUE", [socket.id]);
      if (rows.length != 1) throw new Error("No device with that ID connected to Socket.");
      
      const result = await pool.query(
        "select id, user_id from peripherals where device_id=$1 and peripheral_number=$2",
        [rows[0].device_id, data.port]
      );
      if (result.rowCount != 1) throw new Error("No such peripheral in database");

      const {rows: userRows} = await pool.query("select socket from user_tokens where user_id=$1", [result.rows[0].user_id]);
      if (userRows.length == 1 && userRows[0]) {
        const userSocket = userRows[0].socket;
        io.to(userSocket).emit("calib_stage1_ready", {periphID: result.rows[0].id});
      }
    } catch (error) {
      console.error(`Error in calib_stage1_ready:`, error);
    }
  });

  // Device acknowledges ready for stage 2 (tilt down)
  socket.on('calib_stage2_ready', async (data) => {
    if (!await rateLimitSocketMessage(socket, 'calib_stage2_ready')) return;
    
    console.log(`Device ready for stage 2 (tilt down): ${socket.id}`);
    console.log(data);
    try {
      const {rows} = await pool.query("select device_id from device_tokens where socket=$1 and connected=TRUE", [socket.id]);
      if (rows.length != 1) throw new Error("No device with that ID connected to Socket.");
      
      const result = await pool.query(
        "select id, user_id from peripherals where device_id=$1 and peripheral_number=$2",
        [rows[0].device_id, data.port]
      );
      if (result.rowCount != 1) throw new Error("No such peripheral in database");

      const {rows: userRows} = await pool.query("select socket from user_tokens where user_id=$1", [result.rows[0].user_id]);
      if (userRows.length == 1 && userRows[0]) {
        const userSocket = userRows[0].socket;
        io.to(userSocket).emit("calib_stage2_ready", {periphID: result.rows[0].id});
      }
    } catch (error) {
      console.error(`Error in calib_stage2_ready:`, error);
    }
  });

  // User confirms stage 1 complete (tilt up done)
  socket.on('user_stage1_complete', async (data) => {
    if (!await rateLimitSocketMessage(socket, 'user_stage1_complete')) return;
    
    console.log(`User confirms stage 1 complete: ${socket.id}`);
    console.log(data);
    try {
      const {rows} = await pool.query("select socket from device_tokens where device_id=$1 and connected=TRUE", [data.deviceID]);
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
      io.to(deviceSocket).emit("user_stage1_complete", {port: data.periphNum});
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
    if (!await rateLimitSocketMessage(socket, 'user_stage2_complete')) return;
    
    console.log(`User confirms stage 2 complete: ${socket.id}`);
    console.log(data);
    try {
      const {rows} = await pool.query("select socket from device_tokens where device_id=$1 and connected=TRUE", [data.deviceID]);
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
      io.to(deviceSocket).emit("user_stage2_complete", {port: data.periphNum});
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
    if (!await rateLimitSocketMessage(socket, 'calib_done')) return;
    
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
    if (!await rateLimitSocketMessage(socket, 'pos_hit')) return;
    
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
      const {rows} = await pool.query(
        "update device_tokens set connected=FALSE where socket=$1 returning device_id",
        [socket.id]
      );
      
      // Notify user app that device disconnected
      if (rows.length === 1) {
        const {rows: deviceRows} = await pool.query("select user_id from devices where id=$1", [rows[0].device_id]);
        if (deviceRows.length === 1) {
          const {rows: userRows} = await pool.query("select socket from user_tokens where user_id=$1 and connected=TRUE", [deviceRows[0].user_id]);
          if (userRows.length === 1 && userRows[0]) {
            io.to(userRows[0].socket).emit("device_disconnected", {deviceID: rows[0].device_id});
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
  
  // Rate limit login attempts
  const ip = req.ip || req.connection.remoteAddress;
  try {
    await authRateLimiter.consume(ip);
  } catch (rejRes) {
    return res.status(429).json({ 
      error: 'Too many login attempts. Please try again later.',
      retryAfter: Math.ceil(rejRes.msBeforeNext / 1000 / 60) // minutes
    });
  }
  
  if (!email || !password) return res.status(400).json({error: 'email and password required'});
  try {
    const {rows} = await pool.query('select id, password_hash_string, is_verified from users where email = $1', [email]);
    if (rows.length === 0) return res.status(401).json({error: 'Invalid Credentials'});
    const user = rows[0]
    console.log('user found');
    const verified = await verify(user.password_hash_string, password);

    if (!verified) return res.status(401).json({ error: 'Invalid credentials' });
    console.log("password correct");

    if (!user.is_verified) {
      return res.status(403).json({ 
        error: 'Email not verified. Please check your email and verify your account before logging in.' 
      });
    }
    
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
  
  // Rate limit account creation attempts
  const ip = req.ip || req.connection.remoteAddress;
  try {
    await authRateLimiter.consume(ip);
  } catch (rejRes) {
    return res.status(429).json({ 
      error: 'Too many account creation attempts. Please try again later.',
      retryAfter: Math.ceil(rejRes.msBeforeNext / 1000 / 60) // minutes
    });
  }
  
  // Validate password length
  if (!password || password.length < 8) {
    return res.status(400).json({ error: 'Password must be at least 8 characters' });
  }
  
  // Compute hash and token once for reuse
  const hashedPass = await hash(password);
  const token = crypto.randomBytes(32).toString('hex');
  
  try {
    const newUser = await pool.query(
      `insert into users (name, email, password_hash_string, verification_token, is_verified)
      values (nullif($1, ''), $2, $3, $4, false)
      RETURNING id, email`,
      [name, email,  hashedPass, token]
    );

    // Schedule deletion of unverified user after 15 minutes
    const expiryTime = new Date(Date.now() + 15 * 60 * 1000);
    await agenda.schedule(expiryTime, 'deleteUnverifiedUser', { userId: newUser.rows[0].id });

    await sendVerificationEmail(email, token, name);
    
    // Create temporary token for verification checking
    const tempToken = await createToken(newUser.rows[0].id);

    res.status(201).json({ message: "User registered. Please check your email.", token: tempToken });
  } catch (err) {
    console.error(err);
    if (err.code === '23505') {
      // Email already exists - check if verified
      try {
        const {rows} = await pool.query('SELECT is_verified FROM users WHERE email = $1', [email]);
        if (rows.length === 1 && !rows[0].is_verified) {
          // User exists but not verified - replace their record (reuse hashedPass and token)
          await pool.query(
            `UPDATE users 
             SET name = nullif($1, ''), password_hash_string = $2, verification_token = $3
             WHERE email = $4`,
            [name, hashedPass, token, email]
          );
          
          await sendVerificationEmail(email, token, name);
          
          // Get user ID and create temp token
          const {rows: userRows} = await pool.query('SELECT id FROM users WHERE email = $1', [email]);
          const tempToken = await createToken(userRows[0].id);
          
          return res.status(201).json({ message: "User registered. Please check your email.", token: tempToken });
        }
      } catch (updateErr) {
        console.error('Error updating unverified user:', updateErr);
      }
      
      // User is verified or something went wrong - email is truly in use
      return res.status(409).json({ error: 'Email already in use' });
    }
    return res.sendStatus(500);
  }
});

app.get('/verify-email', async (req, res) => {
  const { token } = req.query;

  if (!token) {
    return res.status(400).send('Missing token');
  }

  try {
    // 1. Find the user with this token
    const result = await pool.query(
      `SELECT * FROM users WHERE verification_token = $1`,
      [token]
    );

    if (result.rows.length === 0) {
      return res.status(400).send('Invalid or expired token.');
    }

    const user = result.rows[0];

    // 2. Verify them and clear the token
    // We clear the token so the link cannot be used twice
    await pool.query(
      `UPDATE users 
       SET is_verified = true, verification_token = NULL 
       WHERE id = $1`,
      [user.id]
    );

    // Cancel any scheduled deletion job for this user
    await agenda.cancel({ name: 'deleteUnverifiedUser', 'data.userId': user.id });

    // 3. Send them to a success page or back to the app
    res.send(`
      <h1>Email Verified!</h1>
      <p>You can now close this window and log in to the app.</p>
    `);

  } catch (err) {
    console.error(err);
    res.status(500).send('Server Error');
  }
});

app.get('/verification_status', authenticateToken, async (req, res) => {
  try {
    const {rows} = await pool.query('SELECT is_verified FROM users WHERE id = $1', [req.user]);
    if (rows.length === 0) return res.status(404).json({error: 'User not found'});
    res.status(200).json({is_verified: rows[0].is_verified});
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/resend_verification', authenticateToken, async (req, res) => {
  const ip = req.ip || req.connection.remoteAddress;
  try {
    await resendVerificationRateLimiter.consume(ip);
  } catch (rejRes) {
    return res.status(429).json({ 
      error: 'Please wait before requesting another verification email.',
      retryAfter: Math.ceil(rejRes.msBeforeNext / 1000) // seconds
    });
  }

  try {
    const { localHour } = req.body;
    const {rows} = await pool.query('SELECT email, name, is_verified FROM users WHERE id = $1', [req.user]);
    if (rows.length === 0) return res.status(404).json({error: 'User not found'});
    
    const user = rows[0];
    
    if (user.is_verified) {
      return res.status(400).json({error: 'Email already verified'});
    }

    const token = crypto.randomBytes(32).toString('hex');
    await pool.query('UPDATE users SET verification_token = $1 WHERE id = $2', [token, req.user]);
    
    await sendVerificationEmail(user.email, token, user.name, localHour);
    
    res.status(200).json({ message: 'Verification email sent' });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Internal server error' });
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

app.post('/logout', authenticateToken, async (req, res) => {
  try {
    // Delete all tokens for this user
    await pool.query("DELETE FROM user_tokens WHERE user_id=$1", [req.user]);
    res.status(200).json({ message: 'Logged out successfully' });
  } catch (error) {
    console.error('Error during logout:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/account_info', authenticateToken, async (req, res) => {
  try {
    const {rows} = await pool.query(
      'SELECT name, email, created_at FROM users WHERE id = $1',
      [req.user]
    );
    if (rows.length === 0) {
      return res.status(404).json({ error: 'User not found' });
    }
    res.status(200).json({
      name: rows[0].name,
      email: rows[0].email,
      created_at: rows[0].created_at
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/change_password', authenticateToken, async (req, res) => {
  try {
    const { oldPassword, newPassword } = req.body;

    // Validate that both passwords are provided
    if (!oldPassword || !newPassword) {
      return res.status(400).json({ error: 'Old password and new password are required' });
    }

    // Validate new password length
    if (newPassword.length < 8) {
      return res.status(400).json({ error: 'New password must be at least 8 characters' });
    }

    // Get current password hash from database
    const {rows} = await pool.query(
      'SELECT password_hash_string FROM users WHERE id = $1',
      [req.user]
    );

    if (rows.length === 0) {
      return res.status(404).json({ error: 'User not found' });
    }

    // Verify old password
    const verified = await verify(rows[0].password_hash_string, oldPassword);
    if (!verified) {
      return res.status(401).json({ error: 'Current password is incorrect' });
    }

    // Hash the new password
    const hashedNewPassword = await hash(newPassword);

    // Update password in database
    await pool.query(
      'UPDATE users SET password_hash_string = $1 WHERE id = $2',
      [hashedNewPassword, req.user]
    );

    res.status(200).json({ message: 'Password changed successfully' });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/request-email-change', authenticateToken, async (req, res) => {
  const { newEmail, localHour } = req.body;

  if (!newEmail) {
    return res.status(400).json({ error: 'New email is required' });
  }

  // Validate email format
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!emailRegex.test(newEmail)) {
    return res.status(400).json({ error: 'Invalid email format' });
  }

  try {
    // Check if new email is already in use
    const {rows: emailCheck} = await pool.query('SELECT id FROM users WHERE email = $1', [newEmail]);
    if (emailCheck.length > 0) {
      return res.status(409).json({ error: 'Email already in use' });
    }

    // Get user info
    const {rows: userRows} = await pool.query('SELECT name, email FROM users WHERE id = $1', [req.user]);
    if (userRows.length === 0) {
      return res.status(404).json({ error: 'User not found' });
    }

    const user = userRows[0];
    const token = crypto.randomBytes(32).toString('hex');
    const expiresAt = new Date(Date.now() + 15 * 60 * 1000); // 15 minutes

    // Insert or update pending email with ON CONFLICT
    await pool.query(
      `INSERT INTO user_pending_emails (user_id, pending_email, token, expires_at)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (user_id) 
       DO UPDATE SET pending_email = $2, token = $3, expires_at = $4`,
      [req.user, newEmail, token, expiresAt]
    );

    // Cancel any existing job for this user
    await agenda.cancel({ name: 'deleteUserPendingEmail', 'data.userId': req.user });

    // Schedule job to delete pending email after 15 minutes
    await agenda.schedule(expiresAt, 'deleteUserPendingEmail', { userId: req.user });

    // Send verification email
    const { sendEmailChangeVerification } = require('./mailer');
    await sendEmailChangeVerification(newEmail, token, user.name, user.email, localHour);

    res.status(200).json({ message: 'Verification email sent to new address' });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/verify-email-change', async (req, res) => {
  const { token } = req.query;

  if (!token) {
    return res.status(400).send('Missing token');
  }

  try {
    // Find the pending email change with this token
    const result = await pool.query(
      `SELECT user_id, pending_email FROM user_pending_emails WHERE token = $1 AND expires_at > NOW()`,
      [token]
    );

    if (result.rows.length === 0) {
      return res.status(400).send('Invalid or expired token.');
    }

    const { user_id, pending_email } = result.rows[0];

    // Check if new email is now taken (race condition check)
    const {rows: emailCheck} = await pool.query('SELECT id FROM users WHERE email = $1', [pending_email]);
    if (emailCheck.length > 0 && emailCheck[0].id !== user_id) {
      await pool.query('DELETE FROM user_pending_emails WHERE user_id = $1', [user_id]);
      await agenda.cancel({ name: 'deleteUserPendingEmail', 'data.userId': user_id });
      return res.status(400).send('Email address is no longer available.');
    }

    // Update the user's email
    await pool.query(
      `UPDATE users SET email = $1 WHERE id = $2`,
      [pending_email, user_id]
    );

    // Delete the pending email record
    await pool.query('DELETE FROM user_pending_emails WHERE user_id = $1', [user_id]);

    // Cancel the scheduled deletion job
    await agenda.cancel({ name: 'deleteUserPendingEmail', 'data.userId': user_id });

    res.send(`
      <h1>Email Changed Successfully!</h1>
      <p>Your email has been updated. You can now close this window.</p>
    `);

  } catch (err) {
    console.error(err);
    res.status(500).send('Internal server error');
  }
});

app.get('/pending-email-status', authenticateToken, async (req, res) => {
  try {
    const {rows} = await pool.query(
      'SELECT pending_email FROM user_pending_emails WHERE user_id = $1',
      [req.user]
    );
    
    if (rows.length === 0) {
      return res.status(200).json({ hasPending: false });
    }
    
    res.status(200).json({ 
      hasPending: true, 
      pendingEmail: rows[0].pending_email 
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Helper function to generate 6-character alphanumeric code
function generateResetCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // Exclude confusing chars like 0, O, 1, I
  let code = '';
  for (let i = 0; i < 6; i++) {
    code += chars.charAt(crypto.randomInt(chars.length));
  }
  return code;
}

app.post('/forgot-password', async (req, res) => {
  const ip = req.ip || req.connection.remoteAddress;
  try {
    await resendVerificationRateLimiter.consume(ip);
  } catch (rejRes) {
    return res.status(429).json({ 
      error: 'Please wait before requesting another code.',
      retryAfter: Math.ceil(rejRes.msBeforeNext / 1000), // seconds
      remainingAttempts: 0
    });
  }

  const { email, localHour } = req.body;

  if (!email) {
    return res.status(400).json({ error: 'Email is required' });
  }

  try {
    // Check if user exists
    const {rows} = await pool.query('SELECT id, name FROM users WHERE email = $1', [email]);
    
    // Always return success to prevent email enumeration
    if (rows.length === 0) {
      return res.status(200).json({ message: 'If an account exists, a reset code has been sent' });
    }

    const user = rows[0];
    const code = generateResetCode();
    const expiresAt = new Date(Date.now() + 15 * 60 * 1000); // 15 minutes

    // Insert or update token with ON CONFLICT
    await pool.query(
      `INSERT INTO password_reset_tokens (email, token, expires_at)
       VALUES ($1, $2, $3)
       ON CONFLICT (email) 
       DO UPDATE SET token = $2, expires_at = $3, created_at = CURRENT_TIMESTAMP`,
      [email, code, expiresAt]
    );

    // Cancel any existing job for this email
    await agenda.cancel({ name: 'deletePasswordResetToken', 'data.email': email });

    // Schedule job to delete token after 15 minutes
    await agenda.schedule(expiresAt, 'deletePasswordResetToken', { email });

    // Send email
    await sendPasswordResetEmail(email, code, user.name, localHour);

    res.status(200).json({ message: 'If an account exists, a reset code has been sent' });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/verify-reset-code', async (req, res) => {
  const { email, code } = req.body;

  if (!email || !code) {
    return res.status(400).json({ error: 'Email and code are required' });
  }

  // Rate limit verification attempts
  const ip = req.ip || req.connection.remoteAddress;
  try {
    const rateLimiterRes = await passwordResetRateLimiter.consume(ip);
    // Store remaining attempts for later use
    res.locals.remainingAttempts = rateLimiterRes.remainingPoints;
  } catch (rejRes) {
    return res.status(429).json({ 
      error: 'Too many verification attempts. Please try again later.',
      retryAfter: Math.ceil(rejRes.msBeforeNext / 1000 / 60), // minutes
      remainingAttempts: 0
    });
  }

  try {
    const {rows} = await pool.query(
      'SELECT token, expires_at FROM password_reset_tokens WHERE email = $1',
      [email]
    );

    if (rows.length === 0) {
      return res.status(401).json({ error: 'Invalid or expired code' });
    }

    const resetToken = rows[0];

    if (new Date() > new Date(resetToken.expires_at)) {
      await pool.query('DELETE FROM password_reset_tokens WHERE email = $1', [email]);
      return res.status(401).json({ error: 'Code has expired' });
    }

    if (resetToken.token !== code.toUpperCase()) {
      return res.status(401).json({ 
        error: 'Invalid code',
        remainingAttempts: res.locals.remainingAttempts || 0
      });
    }

    res.status(200).json({ message: 'Code verified' });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/reset-password', async (req, res) => {
  const { email, code, newPassword } = req.body;

  if (!email || !code || !newPassword) {
    return res.status(400).json({ error: 'Email, code, and new password are required' });
  }

  if (newPassword.length < 8) {
    return res.status(400).json({ error: 'Password must be at least 8 characters' });
  }

  // Rate limit password reset attempts
  const ip = req.ip || req.connection.remoteAddress;
  try {
    await passwordResetRateLimiter.consume(ip);
  } catch (rejRes) {
    return res.status(429).json({ 
      error: 'Too many password reset attempts. Please try again later.',
      retryAfter: Math.ceil(rejRes.msBeforeNext / 1000 / 60), // minutes
      remainingAttempts: 0
    });
  }

  try {
    const {rows} = await pool.query(
      'SELECT token, expires_at FROM password_reset_tokens WHERE email = $1',
      [email]
    );

    if (rows.length === 0) {
      return res.status(401).json({ error: 'Invalid or expired code' });
    }

    const resetToken = rows[0];

    if (new Date() > new Date(resetToken.expires_at)) {
      await pool.query('DELETE FROM password_reset_tokens WHERE email = $1', [email]);
      return res.status(401).json({ error: 'Code has expired' });
    }

    if (resetToken.token !== code.toUpperCase()) {
      return res.status(401).json({ error: 'Invalid code' });
    }

    // Hash new password
    const hashedPassword = await hash(newPassword);

    // Update password
    await pool.query(
      'UPDATE users SET password_hash_string = $1 WHERE email = $2',
      [hashedPassword, email]
    );

    // Delete the used token
    await pool.query('DELETE FROM password_reset_tokens WHERE email = $1', [email]);

    // Cancel scheduled deletion job
    await agenda.cancel({ name: 'deletePasswordResetToken', 'data.email': email });

    res.status(200).json({ message: 'Password reset successfully' });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/device_list', authenticateToken, async (req, res) => {
  try {
    console.log("device List request");
    console.log(req.user);
    const {rows} = await pool.query('select id, device_name, max_ports from devices where user_id = $1', [req.user]);
    const deviceNames = rows.map(row => row.device_name);
    const deviceIds = rows.map(row => row.id);
    const maxPorts = rows.map(row => row.max_ports);
    res.status(200).json({ device_ids: deviceIds, devices: deviceNames, max_ports: maxPorts });
  } catch {
    res.status(500).json({error: 'Internal Server Error'});
  }
});

app.get('/device_name', authenticateToken, async (req, res) => {
  console.log("deviceName");
  try {
    const {deviceId} = req.query;
    const {rows} = await pool.query('select device_name, max_ports from devices where id=$1 and user_id=$2',
      [deviceId, req.user]);
    if (rows.length != 1) return res.sendStatus(404);
    const deviceName = rows[0].device_name;
    const maxPorts = rows[0].max_ports;
    res.status(200).json({device_name: deviceName, max_ports: maxPorts});
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
    const {deviceName, maxPorts} = req.body;
    console.log(deviceName);
    const ports = maxPorts || 4; // Default to 4 for multi-port devices
    const {rows} = await pool.query("insert into devices (user_id, device_name, max_ports) values ($1, $2, $3) returning id",
      [req.user, deviceName, ports]
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
    
    // Check if this is a single-port device
    const {rows: deviceRows} = await pool.query("select max_ports, user_id from devices where id=$1", [req.peripheral]);
    if (deviceRows.length === 0) {
      return res.status(404).json({error: "Device not found"});
    }
    
    const maxPorts = deviceRows[0].max_ports;
    const userId = deviceRows[0].user_id;
    
    // For single-port devices, automatically create the peripheral if it doesn't exist
    if (maxPorts === 1) {
      const {rows: periphRows} = await pool.query(
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
    res.json({token: newToken, id: req.peripheral});
  } catch (error) {
    console.error("Error in verify_device:", error);
    res.status(500).json({error: "server error"});
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

app.get('/device_connection_status', authenticateToken, async (req, res) => {
  console.log("device connection status");
  try {
    const {deviceId} = req.query;
    // Verify device belongs to user
    const {rows: deviceRows} = await pool.query("select id from devices where id=$1 and user_id=$2",
      [deviceId, req.user]
    );
    if (deviceRows.length != 1) return res.sendStatus(404);
    
    // Check if device has an active socket connection
    const {rows} = await pool.query("select connected from device_tokens where device_id=$1 and connected=TRUE",
      [deviceId]
    );
    res.status(200).json({connected: rows.length > 0});
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

app.post('/rename_group', authenticateToken, async (req, res) => {
  console.log("Rename group");
  try {
    const {groupId, newName} = req.body;
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
    const {rows: groupRows} = await pool.query(
      'SELECT id FROM groups WHERE id=$1 AND user_id=$2',
      [groupId, req.user]
    );

    if (groupRows.length === 0) {
      return res.status(404).json({ error: 'Group not found' });
    }

    // Verify all peripherals belong to user
    const {rows: periphRows} = await pool.query(
      'SELECT id FROM peripherals WHERE id = ANY($1::int[]) AND user_id=$2',
      [peripheral_ids, req.user]
    );

    if (periphRows.length !== peripheral_ids.length) {
      return res.status(403).json({ error: 'One or more peripherals do not belong to you' });
    }

    // Check if this exact peripheral set already exists in another group
    const {rows: duplicateRows} = await pool.query(
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
    const {deviceId} = req.body;
    const {rows} = await pool.query("delete from devices where user_id=$1 and id=$2 returning id",
      [req.user, deviceId]
    );
    if (rows.length != 1) {
      return res.status(404).json({ error: 'Device not found' });
    }

    // Get socket ID before deleting tokens
    const {rows: tokenRows} = await pool.query(
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

// Helper function to create cron expression from time and days
function createCronExpression(time, daysOfWeek) {
  const cronDays = daysOfWeek.join(',');
  return `${time.minute} ${time.hour} * * ${cronDays}`;
}

// Helper function to find and verify a schedule job belongs to the user
async function findUserScheduleJob(jobId, userId) {
  const jobs = await agenda.jobs({
    _id: new ObjectId(jobId),
    'data.userID': userId
  });
  return jobs.length === 1 ? jobs[0] : null;
}

app.post('/add_schedule', authenticateToken, async (req, res) => {
  console.log("add schedule");
  try {
    const {periphId, periphNum, deviceId, newPos, time, daysOfWeek} = req.body;
    
    // Validate required fields
    if (!periphId || periphNum === undefined || !deviceId || newPos === undefined || !time || !daysOfWeek || daysOfWeek.length === 0) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    // Create cron expression
    const cronExpression = createCronExpression(time, daysOfWeek);
    
    // Check for duplicate schedule
    const existingJobs = await agenda.jobs({
      'name': 'posChangeScheduled',
      'data.changedPosList.0.periphID': periphId,
      'data.userID': req.user
    });
    
    // Check if any existing job has the same cron expression (time + days)
    const duplicate = existingJobs.find(job => {
      const jobCron = job.attrs.repeatInterval;
      return jobCron === cronExpression;
    });
    
    if (duplicate) {
      console.log("Duplicate schedule detected");
      return res.status(409).json({
        success: false,
        message: 'A schedule with the same time and days already exists for this blind',
        duplicate: true
      });
    }
    
    const changedPosList = [{periphNum: periphNum, periphID: periphId, pos: newPos}];
    
    // Schedule the recurring job
    const job = await agenda.create('posChangeScheduled', {
      deviceID: deviceId,
      changedPosList: changedPosList,
      userID: req.user
    });
    
    job.repeatEvery(cronExpression, {
      skipImmediate: true
    });
    
    await job.save();

    res.status(201).json({
      success: true,
      message: 'Schedule created successfully',
      jobId: job.attrs._id
    });
  } catch (error) {
    console.error('Error creating schedule:', error);
    res.status(500).json({ success: false, message: 'Failed to create schedule', error: error.message });
  }
});

app.post('/delete_schedule', authenticateToken, async (req, res) => {
  console.log("delete schedule");
  try {
    const {jobId} = req.body;
    
    if (!jobId) {
      return res.status(400).json({ error: 'Missing jobId' });
    }

    // Find and verify the existing job belongs to the user
    const existingJob = await findUserScheduleJob(jobId, req.user);
    
    if (!existingJob) {
      return res.status(404).json({ error: 'Schedule not found' });
    }

    // Remove the job
    await existingJob.remove();
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
    const {jobId, periphId, periphNum, deviceId, newPos, time, daysOfWeek} = req.body;
    
    console.log("jobId type:", typeof jobId, "value:", jobId);
    
    // Validate required fields
    if (!jobId || !periphId || periphNum === undefined || !deviceId || newPos === undefined || !time || !daysOfWeek || daysOfWeek.length === 0) {
      console.log("Missing required fields");
      return res.status(400).json({ error: 'Missing required fields' });
    }

    // Find and verify the existing job belongs to the user
    console.log("Searching for job with _id:", jobId, "and userID:", req.user);
    const existingJob = await findUserScheduleJob(jobId, req.user);
    
    console.log("Found job:", existingJob ? 'yes' : 'no');
    if (!existingJob) {
      console.log("Schedule not found");
      return res.status(404).json({ error: 'Schedule not found' });
    }

    console.log("Removing old job...");
    // Cancel the old job
    await existingJob.remove();

    // Create cron expression
    const cronExpression = createCronExpression(time, daysOfWeek);
    const changedPosList = [{periphNum: periphNum, periphID: periphId, pos: newPos}];
    
    console.log("Creating new job with cron:", cronExpression);
    // Create new job with updated schedule
    const job = await agenda.create('posChangeScheduled', {
      deviceID: deviceId,
      changedPosList: changedPosList,
      userID: req.user
    });
    
    job.repeatEvery(cronExpression, {
      skipImmediate: true
    });
    
    await job.save();
    console.log("Job saved successfully with ID:", job.attrs._id);

    res.status(200).json({
      success: true,
      message: 'Schedule updated successfully',
      jobId: job.attrs._id
    });
  } catch (error) {
    console.error('Error updating schedule:', error);
    res.status(500).json({ success: false, message: 'Failed to update schedule', error: error.message });
  }
});

server.listen(port, '127.0.0.1', () => {
  console.log(`Example app listening on 127.0.0.1:${port}`);
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

app.get('/group_list', authenticateToken, async (req, res) => {
  console.log("group_list request for user:", req.user);
  try {
    // Get all groups for the user - no joins needed for menu view
    const {rows} = await pool.query(
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

    if (!peripheral_ids || !Array.isArray(peripheral_ids) || peripheral_ids.length < 2) {
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
      agenda.now('posChange', { deviceID: deviceId, changedPosList, userID: req.user })
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
    const {rows: groupRows} = await pool.query(
      'SELECT id FROM groups WHERE id=$1 AND user_id=$2',
      [groupId, req.user]
    );

    if (groupRows.length === 0) {
      return res.status(404).json({ error: 'Group not found' });
    }

    // Check for duplicate schedule
    const cronExpression = createCronExpression(time, daysOfWeek);
    const existingJobs = await agenda.jobs({
      name: 'groupPosChangeScheduled',
      'data.groupID': groupId,
      'data.userID': req.user
    });

    for (const existingJob of existingJobs) {
      if (existingJob.attrs.repeatInterval === cronExpression && existingJob.attrs.data.newPos === newPos) {
        return res.status(409).json({ error: 'Duplicate schedule already exists' });
      }
    }

    // Create the job
    const job = await agenda.create('groupPosChangeScheduled', {
      groupID: groupId,
      newPos,
      userID: req.user
    });

    job.repeatEvery(cronExpression, {
      timezone: 'America/New_York',
      skipImmediate: true
    });

    await job.save();

    res.status(201).json({
      success: true,
      message: 'Group schedule created successfully',
      jobId: job.attrs._id
    });
  } catch (error) {
    console.error('Error creating group schedule:', error);
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

    const job = await findUserScheduleJob(jobId, req.user);
    if (!job) {
      return res.status(404).json({ error: 'Schedule not found' });
    }

    await job.remove();
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

    const job = await findUserScheduleJob(jobId, req.user);
    if (!job) {
      return res.status(404).json({ error: 'Schedule not found' });
    }

    const groupId = job.attrs.data.groupID;
    const cronExpression = createCronExpression(time, daysOfWeek);

    // Check for duplicates excluding current job
    const existingJobs = await agenda.jobs({
      name: 'groupPosChangeScheduled',
      'data.groupID': groupId,
      'data.userID': req.user,
      _id: { $ne: new ObjectId(jobId) }
    });

    for (const existingJob of existingJobs) {
      if (existingJob.attrs.repeatInterval === cronExpression && existingJob.attrs.data.newPos === newPos) {
        return res.status(409).json({ error: 'Duplicate schedule already exists' });
      }
    }

    // Update job
    job.attrs.data.newPos = newPos;
    job.repeatEvery(cronExpression, {
      timezone: 'America/New_York',
      skipImmediate: true
    });

    await job.save();

    res.status(200).json({
      success: true,
      message: 'Group schedule updated successfully'
    });
  } catch (error) {
    console.error('Error updating group schedule:', error);
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
    const {rows: groupRows} = await pool.query(
      'SELECT id FROM groups WHERE id=$1 AND user_id=$2',
      [groupId, req.user]
    );

    if (groupRows.length === 0) {
      return res.status(404).json({ error: 'Group not found' });
    }

    const jobs = await agenda.jobs({
      name: 'groupPosChangeScheduled',
      'data.groupID': groupId,
      'data.userID': req.user
    });

    const scheduledUpdates = jobs.map(job => {
      const interval = cronParser.parseExpression(job.attrs.repeatInterval);
      const schedule = {
        minutes: interval.fields.minute,
        hours: interval.fields.hour,
        daysOfWeek: interval.fields.dayOfWeek
      };

      return {
        id: job.attrs._id,
        pos: job.attrs.data.newPos,
        schedule
      };
    });

    res.status(200).json({ scheduledUpdates });
  } catch (error) {
    console.error('Error fetching group schedules:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

