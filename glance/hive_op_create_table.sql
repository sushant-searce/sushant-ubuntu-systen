CREATE TABLE output_orc_table(
  glanceid string,
  userid string,
  glancestarttime bigint,
  notificationcount bigint,
  networktype string,
  sdkversion bigint,
  sessionid string,
  sessionmode string,
  impressionid string,
  gpid bigint,
  cityid bigint,
  region string,
  isfeaturebank boolean,
  livestoriescount bigint,
  glanceposition string,
  bubbleimpressionid string,
  impressiontype string,
  glancestartsource string,
  glanceendsource string,
  glanceduration bigint,
  glanceholdduration bigint,
  glanceendtime bigint,
  glance_count bigint,
  ctastarttime bigint,
  ctaendtime bigint,
  ctaduration bigint,
  ctaloadduration bigint,
  ctafailed bigint,
  cta_count bigint,
  peekstarttime bigint,
  peeksource string,
  peekendtime bigint,
  peekduration bigint,
  peeks_count bigint,
  videostarttime bigint,
  videoendtime bigint,
  videoduration bigint,
  playcallduration bigint,
  playstartduration bigint,
  videoloadduration bigint,
  video_count bigint,
  liketime bigint,
  like_count bigint,
  sharetime bigint,
  share_count bigint,
  cta_count_started bigint)
PARTITIONED BY (
  apikey string,
  process_date date)
CLUSTERED BY (
  userid)
SORTED BY (
  userid ASC)
INTO 100 BUCKETS
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'gs://glanceaztogcspoc/analytics/searce-orc-hourly/output-data/glance_op'
TBLPROPERTIES (
  'orc.bloom.filter.columns'='sdkversion,sessionmode,networktype',
  'orc.compress'='ZLIB');



create external table glance_started(`apikey` string,`bubbleimpressionid` string,`cityid` bigint,`clienttime` bigint,`eventname` string,`glanceid` string,`glanceposition` string,`impressionid` string,`impressiontype` string,`isfeaturebank` boolean,`livestoriescount` bigint,`locale` string,`networktype` string,`notificationcount` bigint,`partnerid` string,`region` string,`requesttime` bigint,`sdkversion` bigint,`sessionid` string,`sessionmode` string,`source` string,`stateid` bigint,`time` bigint,`userid` string,`year` integer,`month` integer,`day` integer,`hour` integer)partitioned by(`process_date` date) stored as orc location 'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/testorcdata/glance_started';

create external table glance_ended(`apikey` string,`cityid` integer,`clienttime` bigint,`day` integer,`duration` bigint,`eventname` string,`glanceid` string,`holdduration` bigint,`hour` integer,`impressionid` string,`impressiontype` string,`locale` string,`month` integer,`partnerid` string,`region` string,`requesttime` bigint,`sdkversion` integer,`sessionid` string,`sessionmode` string,`source` string,`stateid` integer,`time` bigint,`userid` string,`year` integer)partitioned by(`process_date` date) stored as orc location 'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/testorcdata/glance_ended';
