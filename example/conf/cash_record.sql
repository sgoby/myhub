CREATE TABLE `cash_record` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `order_sn` varchar(32) NOT NULL DEFAULT '',
  `user_name` varchar(30) NOT NULL,
  `add_time` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `pkey` varchar(50) DEFAULT '',
  PRIMARY KEY (`id`),
  KEY `user_name` (`user_name`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8 COMMENT=''
