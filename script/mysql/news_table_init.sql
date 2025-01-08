CREATE TABLE `tb_resource` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `status` smallint NOT NULL DEFAULT '0',
  `table_id` bigint NOT NULL,
  `house_id` bigint DEFAULT NULL,
  `create_time` datetime DEFAULT NULL,
  `creator` varchar(200) DEFAULT NULL,
  `updater` varchar(200) DEFAULT NULL,
  `update_time` datetime DEFAULT NULL,
  `deleted` smallint DEFAULT '0',
  `fn_title` varchar(255) DEFAULT NULL COMMENT '标题',
  `fn_author` varchar(255) DEFAULT NULL COMMENT '作者',
  `delete_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '删除时间',
  `repeated` smallint DEFAULT '0' COMMENT '是否重复提醒',
  `fn_type` varchar(255) DEFAULT NULL COMMENT '类型',
  `fn_source` varchar(255) DEFAULT NULL COMMENT '来源',
  `fn_editor` varchar(255) DEFAULT NULL COMMENT '编辑',
  `fn_pubtime` datetime DEFAULT NULL COMMENT '发布时间',
  `fn_content` text COMMENT '内容',
  `classify_check` smallint DEFAULT '0' COMMENT '分类审核状态',
  `str_id` varchar(100) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `str_id_unique` (`str_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='资源表'

