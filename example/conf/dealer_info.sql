CREATE TABLE `dealer_info` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `dealer` varchar(20) NOT NULL DEFAULT '',
  `dealer_name` varchar(30) NOT NULL DEFAULT '',
  `dealer_img` varchar(255) DEFAULT '',
  `last_update` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8
