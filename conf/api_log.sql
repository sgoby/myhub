CREATE TABLE `api_log` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_name` varchar(100) NOT NULL COMMENT '代理商名称',
  `api_url` varchar(100) DEFAULT '' COMMENT 'APIurl',
  `otype_name` varchar(45) DEFAULT NULL COMMENT '接口名称或备注',
  `ip_info` varchar(20) DEFAULT NULL,
  `start_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT '开始调用时间',
  `end_time` timestamp NULL DEFAULT '2002-11-13 08:37:23' COMMENT '调用结束时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='api调用日志（使用nosql代替）'
