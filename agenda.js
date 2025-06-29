// agenda.js (modified to receive the already created pool)
const Agenda = require('agenda');
const socketIo = require('socket.io');

let agenda;
let wssInstance;
let sharedPgPool; // This will hold the pool instance passed from server.js

const initializeAgenda = async (mongoUri, pool, wss) => { // Now accepts pgPool
  wssInstance = wss;
  sharedPgPool = pool; // Store the passed pool

  agenda = new Agenda({
    db: {
      address: mongoUri || 'mongodb://localhost:27017/myScheduledApp',
      collection: 'agendaJobs',
    }
  });

  agenda.define('manual update position', async (job) => {
    const { recordId, dataToUpdate, conditionCheck } = job.attrs.data;
    console.log(`Processing job for recordId: ${recordId} at ${new Date()}`);
    try {


      const updatedRecord = result.rows[0];


    } catch (error) {
      console.error(`Error processing job for recordId ${recordId}:`, error);
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
};

module.exports = {
  agenda,
  initializeAgenda
};