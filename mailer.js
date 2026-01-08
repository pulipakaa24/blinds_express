const nodemailer = require('nodemailer');
const { SESv2Client, SendEmailCommand } = require('@aws-sdk/client-sesv2');

const sesClient = new SESv2Client({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

// Create the transporter
const transporter = nodemailer.createTransport({
  SES: { sesClient, SendEmailCommand },
});

// Helper function to get color based on time of day
// hour parameter should be the local hour (0-23) from the client
function getColorForTime(hour) {
  if (hour >= 5 && hour < 10) {
    // Morning - orange
    return '#FF9800';
  } else if (hour >= 10 && hour < 18) {
    // Afternoon - blue
    return '#2196F3';
  } else {
    // Evening/Night - purple
    return '#471189';
  }
}

// Helper function to send email
async function sendVerificationEmail(toEmail, token, name, localHour = new Date().getHours()) {
  const primaryColor = getColorForTime(localHour);
  const verificationLink = `https://wahwa.com/verify-email?token=${token}`;

  try {
    const info = await transporter.sendMail({
      from: `"BlindMaster" <${process.env.EMAIL_FROM}>`, // Sender address
      to: toEmail,
      subject: "Verify your BlindMaster account",
      html: `
        <!DOCTYPE html>
        <html>
        <head>
          <meta charset="UTF-8">
          <meta name="viewport" content="width=device-width, initial-scale=1.0">
          <link href="https://fonts.googleapis.com/css2?family=ABeeZee:ital@0;1&display=swap" rel="stylesheet">
        </head>
        <body style="margin: 0; padding: 0; background-color: #f5f5f5; font-family: 'ABeeZee', 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;">
          <table width="100%" cellpadding="0" cellspacing="0" style="background-color: #f5f5f5; padding: 40px 0;">
            <tr>
              <td align="center">
                <table width="600" cellpadding="0" cellspacing="0" style="background-color: #ffffff; border-radius: 12px; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1); overflow: hidden; max-width: 600px;">
                  
                  <!-- Header with brand color -->
                  <tr>
                    <td align="center" style="background: ${colors.gradient}; padding: 40px 20px;">
                      <h1 style="margin: 0; color: #ffffff; font-size: 32px; font-weight: bold; letter-spacing: 0.5px;">BlindMaster</h1>
                      <p style="margin: 10px 0 0 0; color: #ffffff; font-size: 14px; opacity: 0.95;">Smart Home Automation</p>
                    </td>
                  </tr>
                  
                  <!-- Welcome message -->
                  <tr>
                    <td style="padding: 50px 40px 30px 40px; text-align: center;">
                      <h2 style="margin: 0 0 20px 0; color: #333333; font-size: 28px; font-weight: normal;">
                        Welcome${name && name.trim() ? `, <span style="color: ${primaryColor};">${name.trim()}</span>` : ''}!
                      </h2>
                      <p style="margin: 0 0 30px 0; color: #666666; font-size: 16px; line-height: 1.6;">
                        Thank you for joining BlindMaster! To electrify your blinds, please verify your email address 🥹
                      </p>
                    </td>
                  </tr>
                  
                  <!-- CTA Button -->
                  <tr>
                    <td align="center" style="padding: 0 40px 40px 40px;">
                      <a href="${verificationLink}" 
                         style="display: inline-block; padding: 16px 48px; background-color: ${primaryColor}; color: #ffffff; text-decoration: none; border-radius: 8px; font-size: 16px; font-weight: bold; box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15); transition: all 0.3s ease;">
                        Verify Email Address
                      </a>
                    </td>
                  </tr>
                  
                  <!-- Divider -->
                  <tr>
                    <td style="padding: 0 40px;">
                      <div style="border-top: 1px solid #e0e0e0;"></div>
                    </td>
                  </tr>
                  
                  <!-- Footer info -->
                  <tr>
                    <td style="padding: 30px 40px; text-align: center;">
                      <p style="margin: 0 0 10px 0; color: #999999; font-size: 13px; line-height: 1.5;">
                        This verification link will expire in <strong style="color: #666666;">24 hours</strong>.
                      </p>
                      <p style="margin: 0; color: #999999; font-size: 13px; line-height: 1.5;">
                        If you didn't create a BlindMaster account, please ignore this email!!!
                      </p>
                    </td>
                  </tr>
                  
                  <!-- Footer bar -->
                  <tr>
                    <td align="center" style="background-color: #f9f9f9; padding: 25px 40px;">
                      <p style="margin: 0; color: #999999; font-size: 12px;">
                        © 2026 BlindMaster. All rights reserved.
                      </p>
                    </td>
                  </tr>
                  
                </table>
              </td>
            </tr>
          </table>
        </body>
        </html>
      `,
    });
    console.log("Email sent successfully:", info.messageId);
    return true;
  } catch (error) {
    console.error("Error sending email:", error);
    return false;
  }
}

// Helper function to send password reset email
async function sendPasswordResetEmail(toEmail, code, name, localHour = new Date().getHours()) {
  const primaryColor = getColorForTime(localHour);
  
  try {
    const info = await transporter.sendMail({
      from: `"BlindMaster" <${process.env.EMAIL_FROM}>`,
      to: toEmail,
      subject: "Reset your BlindMaster password",
      html: `
        <!DOCTYPE html>
        <html>
        <head>
          <meta charset="UTF-8">
          <meta name="viewport" content="width=device-width, initial-scale=1.0">
          <link href="https://fonts.googleapis.com/css2?family=ABeeZee:ital@0;1&display=swap" rel="stylesheet">
        </head>
        <body style="margin: 0; padding: 0; background-color: #f5f5f5; font-family: 'ABeeZee', 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;">
          <table width="100%" cellpadding="0" cellspacing="0" style="background-color: #f5f5f5; padding: 40px 0;">
            <tr>
              <td align="center">
                <table width="600" cellpadding="0" cellspacing="0" style="background-color: #ffffff; border-radius: 12px; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1); overflow: hidden; max-width: 600px;">
                  
                  <!-- Header with brand color -->
                  <tr>
                    <td align="center" style="background: ${colors.gradient}; padding: 40px 20px;">
                      <h1 style="margin: 0; color: #ffffff; font-size: 32px; font-weight: bold; letter-spacing: 0.5px;">BlindMaster</h1>
                      <p style="margin: 10px 0 0 0; color: #ffffff; font-size: 14px; opacity: 0.95;">Smart Home Automation</p>
                    </td>
                  </tr>
                  
                  <!-- Message -->
                  <tr>
                    <td style="padding: 50px 40px 30px 40px; text-align: center;">
                      <h2 style="margin: 0 0 20px 0; color: #333333; font-size: 28px; font-weight: normal;">
                        Password Reset Request
                      </h2>
                      <p style="margin: 0 0 30px 0; color: #666666; font-size: 16px; line-height: 1.6;">
                        ${name && name.trim() ? `Hi ${name.trim()}, we` : 'We'} received a request to reset your password. Use the code below to continue:
                      </p>
                    </td>
                  </tr>
                  
                  <!-- Code Display -->
                  <tr>
                    <td align="center" style="padding: 0 40px 40px 40px;">
                      <div style="display: inline-block; padding: 20px 40px; background-color: #f9f9f9; border-radius: 8px; border: 2px solid ${primaryColor};">
                        <p style="margin: 0; color: ${primaryColor}; font-size: 36px; font-weight: bold; letter-spacing: 8px; font-family: 'Courier New', monospace;">
                          ${code}
                        </p>
                      </div>
                    </td>
                  </tr>
                  
                  <!-- Divider -->
                  <tr>
                    <td style="padding: 0 40px;">
                      <div style="border-top: 1px solid #e0e0e0;"></div>
                    </td>
                  </tr>
                  
                  <!-- Footer info -->
                  <tr>
                    <td style="padding: 30px 40px; text-align: center;">
                      <p style="margin: 0 0 10px 0; color: #999999; font-size: 13px; line-height: 1.5;">
                        This code will expire in <strong style="color: #666666;">15 minutes</strong>.
                      </p>
                      <p style="margin: 0; color: #999999; font-size: 13px; line-height: 1.5;">
                        If you didn't request a password reset, please ignore this email.
                      </p>
                    </td>
                  </tr>
                  
                  <!-- Footer bar -->
                  <tr>
                    <td align="center" style="background-color: #f9f9f9; padding: 25px 40px;">
                      <p style="margin: 0; color: #999999; font-size: 12px;">
                        © 2026 BlindMaster. All rights reserved.
                      </p>
                    </td>
                  </tr>
                  
                </table>
              </td>
            </tr>
          </table>
        </body>
        </html>
      `,
    });
    console.log("Password reset email sent successfully:", info.messageId);
    return true;
  } catch (error) {
    console.error("Error sending password reset email:", error);
    return false;
  }
}

module.exports = { sendVerificationEmail, sendPasswordResetEmail };