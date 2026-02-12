-- phpMyAdmin SQL Dump
-- version 5.2.3
-- https://www.phpmyadmin.net/
--
-- Host: db:3306
-- Generation Time: Feb 07, 2026 at 01:00 PM
-- Server version: 8.0.44
-- PHP Version: 8.3.26

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `po_data_app`
--

-- --------------------------------------------------------

--
-- Table structure for table `fund_request_items`
--

CREATE TABLE `fund_request_items` (
  `id` int NOT NULL,
  `request_id` int NOT NULL,
  `target_pm_id` int NOT NULL,
  `requested_amount` float NOT NULL,
  `approved_amount` float DEFAULT NULL,
  `remarque` varchar(255) DEFAULT NULL,
  `admin_note` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Dumping data for table `fund_request_items`
--

INSERT INTO `fund_request_items` (`id`, `request_id`, `target_pm_id`, `requested_amount`, `approved_amount`, `remarque`, `admin_note`) VALUES
(32, 20, 3, 15190, 15190, 'salaire jad , ouiam , femme de menage , choukri + 3000 divers', 'back office ');

--
-- Indexes for dumped tables
--

--
-- Indexes for table `fund_request_items`
--
ALTER TABLE `fund_request_items`
  ADD PRIMARY KEY (`id`),
  ADD KEY `request_id` (`request_id`),
  ADD KEY `target_pm_id` (`target_pm_id`),
  ADD KEY `ix_fund_request_items_id` (`id`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `fund_request_items`
--
ALTER TABLE `fund_request_items`
  MODIFY `id` int NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=37;

--
-- Constraints for dumped tables
--

--
-- Constraints for table `fund_request_items`
--
ALTER TABLE `fund_request_items`
  ADD CONSTRAINT `fund_request_items_ibfk_1` FOREIGN KEY (`request_id`) REFERENCES `fund_requests` (`id`),
  ADD CONSTRAINT `fund_request_items_ibfk_2` FOREIGN KEY (`target_pm_id`) REFERENCES `users` (`id`);
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
