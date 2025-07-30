-- Drop tables if they exist to ensure a clean slate on each startup.
DROP TABLE IF EXISTS `order_execution_log`, `positions`, `balance_history`, `products`, `instruments`, `exchanges`, `trading_range`;

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

CREATE TABLE `market_data` (
  `timestamp` bigint(20) NOT NULL,
  `symbol` varchar(20) NOT NULL,
  `timeframe` varchar(10) NOT NULL,
  `open` decimal(18,8) DEFAULT NULL,
  `high` decimal(18,8) DEFAULT NULL,
  `low` decimal(18,8) DEFAULT NULL,
  `close` decimal(18,8) DEFAULT NULL,
  `volume` decimal(20,5) DEFAULT NULL,
  PRIMARY KEY (`timestamp`,`symbol`,`timeframe`),
  KEY `idx_symbol` (`symbol`),
  KEY `idx_timeframe` (`timeframe`),
  KEY `idx_timestamp_symbol_timeframe` (`timestamp`,`symbol`,`timeframe`)
) ENGINE=MEMORY DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_.ci;

-- Create the trading_range table
CREATE TABLE `trading_range` (
    `symbol` VARCHAR(20) NOT NULL,
    `high_threshold` DECIMAL(18, 8),
    `low_threshold` DECIMAL(18, 8),
    `last_updated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`symbol`)
) ENGINE=InnoDB;

-- Create the order_execution_log table for tracking order submissions
CREATE TABLE `order_execution_log` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `client_order_id` VARCHAR(34) NOT NULL UNIQUE COMMENT 'HyperLiquid 128-bit hex string (0x + 32 hex chars)',
  `request_hash` VARCHAR(64) NOT NULL COMMENT 'SHA256 hash of request payload for deduplication',
  `product_id` VARCHAR(50) NOT NULL,
  `side` ENUM('buy', 'sell') NOT NULL,
  `size` DECIMAL(20, 10) NOT NULL,
  `order_type` ENUM('market', 'limit') NOT NULL,
  `price` DECIMAL(20, 10) NULL COMMENT 'For limit orders',
  `status` ENUM('PENDING', 'CONFIRMED', 'FAILED') NOT NULL DEFAULT 'PENDING',
  `exchange_order_id` VARCHAR(255) NULL COMMENT 'Order ID returned by exchange',
  `error_message` TEXT NULL,
  `retry_count` INT DEFAULT 0,
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX `idx_client_order_id` (`client_order_id`),
  INDEX `idx_request_hash` (`request_hash`),
  INDEX `idx_status` (`status`),
  INDEX `idx_created_at` (`created_at`)
) ENGINE=InnoDB;

-- Insert initial data
INSERT INTO `exchanges` (`id`, `name`, `api_url`) VALUES (1, 'HyperLiquid', 'https://api.hyperliquid.xyz');

INSERT INTO `instruments` (`name`) VALUES
('BTC'), ('ETH'), ('SOL'), ('XRP'), ('MKR'), ('TRUMP'), ('HYPE'), ('SUI'),
('FARTCOIN'), ('DOGE'), ('KPEPE'), ('ENA'), ('ADA'), ('AVAX'), ('CRV'),
('BERA'), ('GRASS'), ('TAO'), ('RENDER'), ('WLD'), ('AI16Z'), ('AIXBT'), ('PAXG'),
('BCH'), ('PENGU');

INSERT INTO `products` (`instrument_id`, `exchange_id`, `symbol`, `product_type`, `max_leverage`) VALUES
((SELECT id from `instruments` where name = 'BTC'), 1, 'BTC/USDC:USDC', 'PERP', 40),
((SELECT id from `instruments` where name = 'ETH'), 1, 'ETH/USDC:USDC', 'PERP', 25),
((SELECT id from `instruments` where name = 'SOL'), 1, 'SOL/USDC:USDC', 'PERP', 20),
((SELECT id from `instruments` where name = 'XRP'), 1, 'XRP/USDC:USDC', 'PERP', 20),
((SELECT id from `instruments` where name = 'MKR'), 1, 'MKR/USDC:USDC', 'PERP', 10),
((SELECT id from `instruments` where name = 'TRUMP'), 1, 'TRUMP/USDC:USDC', 'PERP', 10),
((SELECT id from `instruments` where name = 'HYPE'), 1, 'HYPE/USDC:USDC', 'PERP', 10),
((SELECT id from `instruments` where name = 'SUI'), 1, 'SUI/USDC:USDC', 'PERP', 10),
((SELECT id from `instruments` where name = 'FARTCOIN'), 1, 'FARTCOIN/USDC:USDC', 'PERP', 3),
((SELECT id from `instruments` where name = 'DOGE'), 1, 'DOGE/USDC:USDC', 'PERP', 10),
((SELECT id from `instruments` where name = 'KPEPE'), 1, 'KPEPE/USDC:USDC', 'PERP', 10),
((SELECT id from `instruments` where name = 'ENA'), 1, 'ENA/USDC:USDC', 'PERP', 10),
((SELECT id from `instruments` where name = 'ADA'), 1, 'ADA/USDC:USDC', 'PERP', 10),
((SELECT id from `instruments` where name = 'AVAX'), 1, 'AVAX/USDC:USDC', 'PERP', 10),
((SELECT id from `instruments` where name = 'CRV'), 1, 'CRV/USDC:USDC', 'PERP', 10),
((SELECT id from `instruments` where name = 'BERA'), 1, 'BERA/USDC:USDC', 'PERP', 5),
((SELECT id from `instruments` where name = 'GRASS'), 1, 'GRASS/USDC:USDC', 'PERP', 5),
((SELECT id from `instruments` where name = 'TAO'), 1, 'TAO/USDC:USDC', 'PERP', 5),
((SELECT id from `instruments` where name = 'RENDER'), 1, 'RENDER/USDC:USDC', 'PERP', 5),
((SELECT id from `instruments` where name = 'WLD'), 1, 'WLD/USDC:USDC', 'PERP', 10),
((SELECT id from `instruments` where name = 'AI16Z'), 1, 'AI16Z/USDC:USDC', 'PERP', 5),
((SELECT id from `instruments` where name = 'AIXBT'), 1, 'AIXBT/USDC:USDC', 'PERP', 5),
((SELECT id from `instruments` where name = 'PAXG'), 1, 'PAXG/USDC:USDC', 'PERP', 5),
((SELECT id from `instruments` where name = 'BCH'), 1, 'BCH/USDC:USDC', 'PERP', 10),
((SELECT id from `instruments` where name = 'PENGU'), 1, 'PENGU/USDC:USDC', 'PERP', 3);
