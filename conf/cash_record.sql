CREATE TABLE `cash_record` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `order_sn` varchar(32) NOT NULL DEFAULT '' COMMENT '流水号',
  `user_name` varchar(30) NOT NULL COMMENT '玩家名称',
  `add_time` datetime NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '添加时间',
  `pkey` varchar(50) DEFAULT '' COMMENT '用于校验记录是否被修改,MD5(uid+cash_no+amount)',
  PRIMARY KEY (`id`),
  KEY `user_name` (`user_name`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8 COMMENT='现金记录表(会用nosql替换)'
