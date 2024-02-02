-- --------------------------------------------------------
-- 主机:                           127.0.0.1
-- 服务器版本:                        10.4.28-MariaDB - mariadb.org binary distribution
-- 服务器操作系统:                      Win64
-- HeidiSQL 版本:                  12.3.0.6589
-- --------------------------------------------------------

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET NAMES utf8 */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;


-- 导出 tdx 的数据库结构
CREATE DATABASE IF NOT EXISTS `tdx` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci */;
USE `tdx`;

-- 导出  表 tdx.lday 结构
CREATE TABLE IF NOT EXISTS `lday` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `code` char(12) NOT NULL DEFAULT '',
  `date` date NOT NULL,
  `open` int(11) NOT NULL,
  `hight` int(11) NOT NULL,
  `low` int(11) NOT NULL,
  `close` int(11) NOT NULL,
  `amount` decimal(20,6) NOT NULL,
  `vol` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `code` (`code`)
) ENGINE=InnoDB AUTO_INCREMENT=53003 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='日线数据';

-- 数据导出被取消选择。

/*!40103 SET TIME_ZONE=IFNULL(@OLD_TIME_ZONE, 'system') */;
/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
/*!40014 SET FOREIGN_KEY_CHECKS=IFNULL(@OLD_FOREIGN_KEY_CHECKS, 1) */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40111 SET SQL_NOTES=IFNULL(@OLD_SQL_NOTES, 1) */;
