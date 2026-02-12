-- phpMyAdmin SQL Dump
-- version 5.2.1
-- https://www.phpmyadmin.net/
--
-- Hôte : localhost
-- Généré le : mar. 10 fév. 2026 à 19:05
-- Version du serveur : 10.4.32-MariaDB
-- Version de PHP : 8.2.12

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Base de données : `po_data_app`
--

-- --------------------------------------------------------

--
-- Structure de la table `fund_requests`
--

CREATE TABLE `fund_requests` (
  `id` int(11) NOT NULL,
  `request_number` varchar(50) DEFAULT NULL,
  `requester_id` int(11) DEFAULT NULL,
  `approver_id` int(11) DEFAULT NULL,
  `status` varchar(50) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `approved_at` datetime DEFAULT NULL,
  `completed_at` datetime DEFAULT NULL,
  `paid_amount` float DEFAULT NULL,
  `admin_comment` text DEFAULT NULL,
  `reception_attachment` varchar(500) DEFAULT NULL,
  `confirmed_reception_amount` float DEFAULT NULL,
  `variance_note` text DEFAULT NULL,
  `variance_acknowledged` tinyint(1) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Déchargement des données de la table `fund_requests`
--

INSERT INTO `fund_requests` (`id`, `request_number`, `requester_id`, `approver_id`, `status`, `created_at`, `approved_at`, `completed_at`, `paid_amount`, `admin_comment`, `reception_attachment`, `confirmed_reception_amount`, `variance_note`, `variance_acknowledged`) VALUES
(22, 'REQ-2026-02-001', 3, 1, 'PARTIALLY_PAID', '2026-02-10 19:01:13', '2026-02-10 19:01:33', NULL, 2500, NULL, 'CONFIRM_REQ_22_BC2026010203 (2).pdf', 2500, NULL, 0);

--
-- Index pour les tables déchargées
--

--
-- Index pour la table `fund_requests`
--
ALTER TABLE `fund_requests`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `ix_fund_requests_request_number` (`request_number`),
  ADD KEY `approver_id` (`approver_id`),
  ADD KEY `requester_id` (`requester_id`),
  ADD KEY `ix_fund_requests_id` (`id`);

--
-- AUTO_INCREMENT pour les tables déchargées
--

--
-- AUTO_INCREMENT pour la table `fund_requests`
--
ALTER TABLE `fund_requests`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=23;

--
-- Contraintes pour les tables déchargées
--

--
-- Contraintes pour la table `fund_requests`
--
ALTER TABLE `fund_requests`
  ADD CONSTRAINT `fund_requests_ibfk_1` FOREIGN KEY (`approver_id`) REFERENCES `users` (`id`),
  ADD CONSTRAINT `fund_requests_ibfk_2` FOREIGN KEY (`requester_id`) REFERENCES `users` (`id`);
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
