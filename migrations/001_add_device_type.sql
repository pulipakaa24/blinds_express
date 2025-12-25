-- Migration: Add max_ports column to devices table to support different device types
-- BlindMaster-C6: max_ports = 1 (single servo)
-- BlindMaster Device: max_ports = 4 (multi-port)

-- Add max_ports column with default value of 4 for existing devices
ALTER TABLE devices 
ADD COLUMN max_ports INTEGER NOT NULL DEFAULT 4;

-- Add a check constraint to ensure max_ports is between 1 and 4
ALTER TABLE devices 
ADD CONSTRAINT devices_max_ports_check CHECK (max_ports >= 1 AND max_ports <= 4);

-- For existing devices, you may want to set max_ports based on the number of peripherals
-- Uncomment the following line if you want to auto-detect based on existing peripherals:
-- UPDATE devices d SET max_ports = GREATEST(1, (SELECT COUNT(*) FROM peripherals p WHERE p.device_id = d.id));

-- Create an index for faster queries
CREATE INDEX idx_devices_max_ports ON devices(max_ports);

COMMENT ON COLUMN devices.max_ports IS 'Maximum number of ports/peripherals this device supports (1 for C6, 4 for multi-port)';
