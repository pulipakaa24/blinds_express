const { Pool } = require('pg');
require('dotenv').config();

const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT,
});

(async () => {
  try {
    const res = await pool.query("SELECT now(), current_setting('TIMEZONE');");
    console.log("DB Time:", res.rows[0]);
    console.log("System Time:", new Date().toString());
    process.exit(0);
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
})();
