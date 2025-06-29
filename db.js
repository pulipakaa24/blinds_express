// db.js
const mongoose = require('mongoose');

const connectDB = async () => {
    try {
        const mongoUri = process.env.MONGO_URI || 'mongodb://localhost:27017/myScheduledApp';
        await mongoose.connect(mongoUri, {
            // These options are often recommended for newer Mongoose versions, though some might be deprecated in future
            // useNewUrlParser: true,
            // useUnifiedTopology: true,
        });
        console.log('MongoDB connected successfully for Mongoose!');
    } catch (err) {
        console.error('MongoDB connection error (Mongoose):', err);
        process.exit(1); // Exit process with failure
    }
};

module.exports = connectDB;