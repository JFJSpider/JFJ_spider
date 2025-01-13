create table public.rs_test (
  id integer primary key not null, -- id
  title character varying(1000) not null, -- 标题
  all_title character varying(2000), -- 所有题名
  series_titles character varying(1000), -- 丛编题名
  author character varying(1000), -- 作者
  author_intro text, -- 作者简介
  editor character varying(1000), -- 主编
  publish character varying(1000), -- 出版社
  publish_time timestamp, -- 出版时间
  publish_place character varying(1000), -- 出版发行地
  catalogue text, -- 目录
  preface text, -- 前言序言
  content_intro text, -- 内容简介
  abstracts text, -- 摘要
  "ISBN" character varying(100), -- ISBN号
  language character varying(50), -- 语种
  keyword character varying(500), -- 关键词
  document_type character varying(100), -- 文献类型
  subcategory character varying(100), -- 所属分类
  product_features character varying(1000), -- 产品特色
  price numeric(8,2), -- 价格
  evaluation_number integer, -- 评价数
  "People_buy" integer, -- 购买人数
  sellers_number integer, -- 在售商家数
  packages character varying(100), -- 包装
  brand character varying(50), -- 品牌
  product_code character varying(50), -- 商品编码
  is_set character varying(10), -- 是否套装
  format character varying(50), -- 开本
  paper character varying(50), -- 纸张
  collection_information character varying(500), -- 馆藏信息
  editor_recommendations text, -- 编辑推荐
  pages integer, -- 页数
  words integer, -- 字数
  edition integer, -- 版次
  version character varying(100), -- 版本说明
  additional_version character varying(200), -- 附加版本说明
  "Illustration_url" text, -- 内页插图
  image_url text, -- 商品图
  page_url text, -- 页面网址
  data_source character varying(50), -- 来源数据库
  data_type character varying(10), -- 数据类型
  data_status varchar default 0, -- 数据状态（0，未提交，1，已提交）
  creator character varying(100), -- 创建人
  create_time timestamp, -- 创建时间
  updater character varying(100), -- 修改人
  update_time timestamp, -- 修改时间
  deleted smallint default 0,
  tenant_id bigint,
  may bigint, -- 码洋
  book_type character varying(50), -- 书类型(1:图书2:电子书3:音像)
  base64_url text, -- 封面
  str_id character varying(100) not null, -- 自定义数据id，带有前缀
  distribution_scope character varying(50), -- 发行范围
  weight integer, -- 重量
  "CIP" character varying(100), -- CIP核字
  height integer, -- 高度
  "CLCI" character varying(100),
  printing_date timestamp, -- 印刷时间
  out_edition integer,
  first_publish_time timestamp, -- 首版时间
  printing_sheet character varying(50), -- 印张
  printing_edition integer, -- 印次
  length integer, -- 长
  width integer, -- 宽
  target_audience character varying(100), -- 读者对象（适合人群）
  book_medium character varying(100), -- 媒质/介质
  use_paper character varying(50), -- 用纸
  is_phonetic character varying(50), -- 是否注音
  photocopy_version character varying(50), -- 影印版本
  publisher_country character varying(50), -- 出版商国别
  translator character varying(100), -- 译者
  printing_num integer, -- 印数
  freebie_quantity character varying(100), -- 附赠及数量
  compiler character varying(100), -- 整理者（整理）
  drawer character varying(100), -- 绘者
  annotation character varying(100), -- 校注
  binding character varying(50), -- 装帧
  oral_narration character varying(50), -- 口述人
  planner character varying(50), -- 策划人
  copyright_provider character varying(100), -- 版权提供者
  e_book_release_time timestamp, -- 电子书上线时间
  is_have_e_book character varying(50), -- 是否有电子书
  copyright_number character varying(100),
  photographer character varying(100) -- 摄影师(书中图片摄影师)
);


create unique index rs_correct_resources_strid_unique on rs_correct_resources using btree (str_id);
comment on table public.rs_correct_resources is '正确资源列表';
comment on column public.rs_correct_resources.id is 'id';
comment on column public.rs_correct_resources.title is '标题';
comment on column public.rs_correct_resources.all_title is '所有题名';
comment on column public.rs_correct_resources.series_titles is '丛编题名';
comment on column public.rs_correct_resources.author is '作者';
comment on column public.rs_correct_resources.author_intro is '作者简介';
comment on column public.rs_correct_resources.editor is '主编';
comment on column public.rs_correct_resources.publish is '出版社';
comment on column public.rs_correct_resources.publish_time is '出版时间';
comment on column public.rs_correct_resources.publish_place is '出版发行地';
comment on column public.rs_correct_resources.catalogue is '目录';
comment on column public.rs_correct_resources.preface is '前言序言';
comment on column public.rs_correct_resources.content_intro is '内容简介';
comment on column public.rs_correct_resources.abstracts is '摘要';
comment on column public.rs_correct_resources."ISBN" is 'ISBN号';
comment on column public.rs_correct_resources.language is '语种';
comment on column public.rs_correct_resources.keyword is '关键词';
comment on column public.rs_correct_resources.document_type is '文献类型';
comment on column public.rs_correct_resources.subcategory is '所属分类';
comment on column public.rs_correct_resources.product_features is '产品特色';
comment on column public.rs_correct_resources.price is '价格';
comment on column public.rs_correct_resources.evaluation_number is '评价数';
comment on column public.rs_correct_resources."People_buy" is '购买人数';
comment on column public.rs_correct_resources.sellers_number is '在售商家数';
comment on column public.rs_correct_resources.packages is '包装';
comment on column public.rs_correct_resources.brand is '品牌';
comment on column public.rs_correct_resources.product_code is '商品编码';
comment on column public.rs_correct_resources.is_set is '是否套装';
comment on column public.rs_correct_resources.format is '开本';
comment on column public.rs_correct_resources.paper is '纸张';
comment on column public.rs_correct_resources.collection_information is '馆藏信息';
comment on column public.rs_correct_resources.editor_recommendations is '编辑推荐';
comment on column public.rs_correct_resources.pages is '页数';
comment on column public.rs_correct_resources.words is '字数';
comment on column public.rs_correct_resources.edition is '版次';
comment on column public.rs_correct_resources.version is '版本说明';
comment on column public.rs_correct_resources.additional_version is '附加版本说明';
comment on column public.rs_correct_resources."Illustration_url" is '内页插图';
comment on column public.rs_correct_resources.image_url is '商品图';
comment on column public.rs_correct_resources.page_url is '页面网址';
comment on column public.rs_correct_resources.data_source is '来源数据库';
comment on column public.rs_correct_resources.data_type is '数据类型';
comment on column public.rs_correct_resources.data_status is '数据状态（0，未提交，1，已提交）';
comment on column public.rs_correct_resources.creator is '创建人';
comment on column public.rs_correct_resources.create_time is '创建时间';
comment on column public.rs_correct_resources.updater is '修改人';
comment on column public.rs_correct_resources.update_time is '修改时间';
comment on column public.rs_correct_resources.may is '码洋';
comment on column public.rs_correct_resources.book_type is '书类型(1:图书2:电子书3:音像)';
comment on column public.rs_correct_resources.base64_url is '封面';
comment on column public.rs_correct_resources.str_id is '自定义数据id，带有前缀';
comment on column public.rs_correct_resources.distribution_scope is '发行范围';
comment on column public.rs_correct_resources.weight is '重量';
comment on column public.rs_correct_resources."CIP" is 'CIP核字';
comment on column public.rs_correct_resources.height is '高度';
comment on column public.rs_correct_resources.printing_date is '印刷时间';
comment on column public.rs_correct_resources.first_publish_time is '首版时间';
comment on column public.rs_correct_resources.printing_sheet is '印张';
comment on column public.rs_correct_resources.printing_edition is '印次';
comment on column public.rs_correct_resources.length is '长';
comment on column public.rs_correct_resources.width is '宽';
comment on column public.rs_correct_resources.target_audience is '读者对象（适合人群）';
comment on column public.rs_correct_resources.book_medium is '媒质/介质';
comment on column public.rs_correct_resources.use_paper is '用纸';
comment on column public.rs_correct_resources.is_phonetic is '是否注音';
comment on column public.rs_correct_resources.photocopy_version is '影印版本';
comment on column public.rs_correct_resources.publisher_country is '出版商国别';
comment on column public.rs_correct_resources.translator is '译者';
comment on column public.rs_correct_resources.printing_num is '印数';
comment on column public.rs_correct_resources.freebie_quantity is '附赠及数量';
comment on column public.rs_correct_resources.compiler is '整理者（整理）';
comment on column public.rs_correct_resources.drawer is '绘者';
comment on column public.rs_correct_resources.annotation is '校注';
comment on column public.rs_correct_resources.binding is '装帧';
comment on column public.rs_correct_resources.oral_narration is '口述人';
comment on column public.rs_correct_resources.planner is '策划人';
comment on column public.rs_correct_resources.copyright_provider is '版权提供者';
comment on column public.rs_correct_resources.e_book_release_time is '电子书上线时间';
comment on column public.rs_correct_resources.is_have_e_book is '是否有电子书';
comment on column public.rs_correct_resources.photographer is '摄影师(书中图片摄影师)';

