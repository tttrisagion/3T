-- Drop tables if they exist to ensure a clean slate on each startup.
DROP TABLE IF EXISTS `positions`, `balance_history`, `products`, `instruments`, `exchanges`;

-- Create the exchanges table
CREATE TABLE `exchanges` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `name` VARCHAR(255) NOT NULL UNIQUE,
  `api_url` VARCHAR(255),
  `websocket_url` VARCHAR(255),
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- Create the instruments table
CREATE TABLE `instruments` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `name` VARCHAR(255) NOT NULL UNIQUE COMMENT 'e.g., Bitcoin, Ethereum',
  `description` TEXT,
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- Create the products table to link instruments to exchanges
CREATE TABLE `products` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `instrument_id` INT NOT NULL,
  `exchange_id` INT NOT NULL,
  `symbol` VARCHAR(255) NOT NULL COMMENT 'Exchange-specific symbol, e.g., BTC/USDC:USDC',
  `product_type` VARCHAR(50) NOT NULL COMMENT 'e.g., PERP, SPOT, OPTION',
  `max_leverage` DECIMAL(10, 2),
  `metadata` JSON COMMENT 'For storing other details like option expiration, etc.',
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (`instrument_id`) REFERENCES `instruments`(`id`),
  FOREIGN KEY (`exchange_id`) REFERENCES `exchanges`(`id`),
  UNIQUE (`exchange_id`, `symbol`)
) ENGINE=InnoDB;

-- Create the positions table
CREATE TABLE `positions` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `product_id` INT NOT NULL,
  `position_size` DECIMAL(20, 10) NOT NULL,
  `position_value` DECIMAL(20, 10) NOT NULL,
  `unrealized_pnl` DECIMAL(20, 10) NOT NULL,
  `timestamp` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (`product_id`) REFERENCES `products`(`id`)
) ENGINE=InnoDB;

-- Create the balance history table
CREATE TABLE `balance_history` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `exchange_id` INT NOT NULL,
  `account_value` DECIMAL(20, 10) NOT NULL,
  `cross_maintenance_margin_used` DECIMAL(20, 10) NOT NULL,
  `timestamp` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (`exchange_id`) REFERENCES `exchanges`(`id`)
) ENGINE=InnoDB;

-- Insert initial data
INSERT INTO `exchanges` (`name`, `api_url`) VALUES ('HyperLiquid', 'https://api.hyperliquid.xyz');
INSERT INTO `instruments` (`name`) VALUES ('Bitcoin'), ('Ethereum'), ('Solana');
INSERT INTO `products` (`instrument_id`, `exchange_id`, `symbol`, `product_type`, `max_leverage`) VALUES
(1, 1, 'BTC/USDC:USDC', 'PERP', 50.00),
(2, 1, 'ETH/USDC:USDC', 'PERP', 50.00),
(3, 1, 'SOL/USDC:USDC', 'PERP', 50.00);
