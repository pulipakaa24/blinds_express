// db.js
const mongoose = require('mongoose');

const connectDB = async () => {
    try {
        const mongoUri = 'mongodb://localhost:27017/myScheduledApp';
        await mongoose.connect(mongoUri);
        console.log('MongoDB connected successfully for Mongoose!');
    } catch (err) {
        console.error('MongoDB connection error (Mongoose):', err);
        process.exit(1); // Exit process with failure
    }
};

module.exports = connectDB;