CREATE TABLE `dealer_info` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `dealer` varchar(20) NOT NULL DEFAULT '' COMMENT '荷官ID(荷官编码)',
  `dealer_name` varchar(30) NOT NULL DEFAULT '' COMMENT '荷官名称',
  `dealer_img` varchar(255) DEFAULT '' COMMENT '荷官图片',
  `last_update` datetime NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '最后更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8 COMMENT='荷官信息表'
