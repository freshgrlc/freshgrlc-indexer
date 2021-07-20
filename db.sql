-- MySQL dump 10.16  Distrib 10.1.35-MariaDB, for Linux (x86_64)
--
-- Host: localhost    Database: blockchain
-- ------------------------------------------------------
-- Server version	10.1.34-MariaDB

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `address`
--

DROP TABLE IF EXISTS `address`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `address` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `type` tinyint(4) NOT NULL,
  `address` varchar(64) DEFAULT NULL,
  `raw` varchar(256) DEFAULT NULL,
  `balance` decimal(16,8) DEFAULT NULL,
  `balance_dirty` tinyint(4) NOT NULL DEFAULT '1',
  PRIMARY KEY (`id`),
  UNIQUE KEY `address` (`address`),
  KEY `addresstype` (`type`,`address`),
  KEY `balance` (`balance`),
  KEY `balance_dirty` (`balance_dirty`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `block`
--

DROP TABLE IF EXISTS `block`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `block` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `hash` binary(32) NOT NULL,
  `height` int(11) DEFAULT NULL,
  `size` int(11) NOT NULL,
  `totalfee` decimal(16,8) NOT NULL,
  `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `difficulty` decimal(8,3) NOT NULL,
  `firstseen` timestamp NULL DEFAULT NULL,
  `relayedby` varchar(48) DEFAULT NULL,
  `miner` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `hash` (`hash`),
  UNIQUE KEY `height` (`height`),
  KEY `miner` (`miner`),
  KEY `timestamp` (`timestamp`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `blocktx`
--

DROP TABLE IF EXISTS `blocktx`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `blocktx` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `transaction` bigint(20) NOT NULL,
  `block` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `blocktx` (`block`,`transaction`),
  KEY `fk_blocktx_block_idx` (`block`),
  KEY `fk_blocktx_transaction_idx` (`transaction`),
  CONSTRAINT `fk_blocktx_block` FOREIGN KEY (`block`) REFERENCES `block` (`id`) ON DELETE CASCADE,
  CONSTRAINT `fk_blocktx_transaction` FOREIGN KEY (`transaction`) REFERENCES `transaction` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `cache`
--

DROP TABLE IF EXISTS `cache`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cache` (
  `id` int(11) NOT NULL,
  `valid` tinyint(4) NOT NULL,
  `value` decimal(16,8) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `cache`
--

LOCK TABLES `cache` WRITE;
/*!40000 ALTER TABLE `cache` DISABLE KEYS */;
INSERT INTO `cache` VALUES (0,0,0.00000000),(1,0,0.00000000),(2,0,0.00000000),(3,0,0.00000000);
/*!40000 ALTER TABLE `cache` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `coinbase`
--

DROP TABLE IF EXISTS `coinbase`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `coinbase` (
  `block` int(11) NOT NULL,
  `transaction` bigint(20) NOT NULL,
  `newcoins` decimal(16,8) NOT NULL,
  `raw` varbinary(256) NOT NULL,
  `signature` varchar(32) DEFAULT NULL,
  `mainoutput` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`block`),
  UNIQUE KEY `transaction` (`transaction`),
  KEY `signature` (`signature`),
  KEY `fk_coinbase_mainoutput_idx` (`mainoutput`),
  CONSTRAINT `fk_coinbase_block` FOREIGN KEY (`block`) REFERENCES `block` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `fk_coinbase_mainoutput` FOREIGN KEY (`mainoutput`) REFERENCES `txout` (`id`) ON DELETE CASCADE,
  CONSTRAINT `fk_coinbase_transaction` FOREIGN KEY (`transaction`) REFERENCES `transaction` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `coindaysdestroyed`
--

DROP TABLE IF EXISTS `coindaysdestroyed`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `coindaysdestroyed` (
  `transaction` bigint(20) NOT NULL,
  `coindays` decimal(20,8) NOT NULL,
  `timestamp` timestamp NOT NULL,
  PRIMARY KEY (`transaction`),
  KEY `timestamp` (`timestamp`),
  CONSTRAINT `fk_coindaysdestroyed_transaction` FOREIGN KEY (`transaction`) REFERENCES `transaction` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `mutation`
--

DROP TABLE IF EXISTS `mutation`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `mutation` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `transaction` bigint(20) NOT NULL,
  `address` int(11) NOT NULL,
  `amount` decimal(16,8) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_mutations_transaction_idx` (`transaction`),
  KEY `fk_mutations_address_idx` (`address`),
  CONSTRAINT `fk_mutations_address` FOREIGN KEY (`address`) REFERENCES `address` (`id`),
  CONSTRAINT `fk_mutations_transaction` FOREIGN KEY (`transaction`) REFERENCES `transaction` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `pool`
--

DROP TABLE IF EXISTS `pool`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `pool` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `group` int(11) DEFAULT NULL,
  `name` varchar(64) NOT NULL,
  `solo` tinyint(4) NOT NULL DEFAULT '0',
  `website` varchar(64) DEFAULT NULL,
  `graphcolor` char(6) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name_UNIQUE` (`name`),
  KEY `fk_pool_group_idx` (`group`),
  CONSTRAINT `fk_pool_group` FOREIGN KEY (`group`) REFERENCES `poolgroup` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `pooladdress`
--

DROP TABLE IF EXISTS `pooladdress`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `pooladdress` (
  `address` int(11) NOT NULL,
  `pool` int(11) NOT NULL,
  PRIMARY KEY (`address`),
  KEY `fk_pooladdress_pool_idx` (`pool`),
  CONSTRAINT `fk_pooladdress_address` FOREIGN KEY (`address`) REFERENCES `address` (`id`) ON DELETE CASCADE,
  CONSTRAINT `fk_pooladdress_pool` FOREIGN KEY (`pool`) REFERENCES `pool` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `poolgroup`
--

DROP TABLE IF EXISTS `poolgroup`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `poolgroup` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(64) NOT NULL,
  `website` varchar(64) DEFAULT NULL,
  `graphcolor` char(6) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name_UNIQUE` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `poolsignature`
--

DROP TABLE IF EXISTS `poolsignature`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `poolsignature` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `signature` varchar(32) NOT NULL,
  `pool` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `signature_UNIQUE` (`signature`),
  KEY `fk_poolsignature_pool_idx` (`pool`),
  CONSTRAINT `fk_poolsignature_pool` FOREIGN KEY (`pool`) REFERENCES `pool` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `transaction`
--

DROP TABLE IF EXISTS `transaction`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `transaction` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `txid` binary(32) NOT NULL,
  `size` int(11) NOT NULL,
  `fee` decimal(16,8) NOT NULL,
  `totalvalue` decimal(16,8) NOT NULL,
  `firstseen` datetime DEFAULT NULL,
  `relayedby` varchar(48) DEFAULT NULL,
  `confirmation` bigint(20) DEFAULT NULL,
  `doublespends` bigint(20) DEFAULT NULL,
  `mempool` tinyint(1) AS (IF(ISNULL(`confirmation`) AND ISNULL(`doublespends`), '1', '0')),
  PRIMARY KEY (`id`),
  UNIQUE KEY `txid` (`txid`),
  UNIQUE KEY `confirmation` (`confirmation`),
  KEY `mempool` (`mempool`),
  CONSTRAINT `fk_transaction_confirmation` FOREIGN KEY (`confirmation`) REFERENCES `blocktx` (`id`) ON DELETE SET NULL ON UPDATE NO ACTION,
  CONSTRAINT `fk_transaction_doublespends` FOREIGN KEY (`doublespends`) REFERENCES `transaction` (`id`) ON DELETE SET NULL ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `txin`
--

DROP TABLE IF EXISTS `txin`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `txin` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `transaction` bigint(20) NOT NULL,
  `index` int(11) NOT NULL,
  `input` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `txin` (`transaction`,`index`),
  UNIQUE KEY `txspend` (`transaction`,`input`),
  KEY `fk_transaction_idx` (`transaction`),
  KEY `fk_txin_input_idx` (`input`),
  CONSTRAINT `fk_txin_input` FOREIGN KEY (`input`) REFERENCES `txout` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `fk_txin_transaction` FOREIGN KEY (`transaction`) REFERENCES `transaction` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `txout`
--

DROP TABLE IF EXISTS `txout`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `txout` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `transaction` bigint(20) NOT NULL,
  `index` int(11) NOT NULL,
  `type` tinyint(4) NOT NULL,
  `address` int(11) NOT NULL,
  `amount` decimal(16,8) NOT NULL,
  `spentby` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `txout` (`transaction`,`index`),
  UNIQUE KEY `spentby` (`spentby`),
  KEY `address` (`address`),
  KEY `address_utxo` (`address`,`spentby`),
  CONSTRAINT `fk_txout_address` FOREIGN KEY (`address`) REFERENCES `address` (`id`),
  CONSTRAINT `fk_txout_spentby` FOREIGN KEY (`spentby`) REFERENCES `txin` (`id`) ON DELETE SET NULL,
  CONSTRAINT `fk_txout_transaction` FOREIGN KEY (`transaction`) REFERENCES `transaction` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2018-11-24 23:19:21
