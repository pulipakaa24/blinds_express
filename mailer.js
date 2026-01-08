const nodemailer = require('nodemailer');
const { SESClient, SendRawEmailCommand } = require('@aws-sdk/client-sesv2');

const ses = new SESClient({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

// Create the transporter
const transporter = nodemailer.createTransport({
  SES: { ses, aws: { SendRawEmailCommand } },
});

// Helper function to send email
async function sendVerificationEmail(toEmail, token, name) {
  const verificationLink = `https://wahwa.com/verify-email?token=${token}`;

  try {
    const info = await transporter.sendMail({
      from: `"BlindMaster" <${process.env.EMAIL_FROM}>`, // Sender address
      to: toEmail,
      subject: "Verify your BlindMaster account",
      html: `
        <div style="font-family: sans-serif; padding: 20px;">
          <h2>Welcome${name && name.trim() ? `, ${name.trim()}` : ''}!</h2>
          <p>Please verify your email address to complete your registration.</p>
          <a href="${verificationLink}" style="padding: 10px 20px; background-color: #007bff; color: white; text-decoration: none; border-radius: 5px;">Verify Email</a>
          <p style="margin-top: 20px; font-size: 12px; color: #888;">Link expires in 24 hours.</p>
        </div>
      `,
    });
    console.log("Email sent successfully:", info.messageId);
    return true;
  } catch (error) {
    console.error("Error sending email:", error);
    return false;
  }
}

module.exports = { sendVerificationEmail };