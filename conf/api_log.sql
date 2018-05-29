CREATE TABLE `api_log` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_name` varchar(100) NOT NULL,
  `api_url` varchar(100) DEFAULT '',
  `otype_name` varchar(45) DEFAULT NULL,
  `ip_info` varchar(20) DEFAULT NULL,
  `start_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `end_time` timestamp NULL DEFAULT '2002-11-13 08:37:23',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
