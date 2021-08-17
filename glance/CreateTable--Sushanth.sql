
create table glance_shared(
    `cityid` bigint,
    `clienttime` bigint,
    `eventname` string,
    `glanceid` string,
    `impressionid` string,
    `impressiontype` string,
    `locale` string,
    `partnerid` string,
    `region` string,
    `requesttime` bigint,
    `sdkversion` bigint,
    `sessionid` string,
    `sessionmode` string,
    `source` string,
    `stateid` bigint,
    `time` bigint,
    `userid` string,
    `year` integer,
    `month` integer,
    `day` integer
    )
    partitioned by(`apikey` string, `process_date` date, `hour` integer) 
    stored as orc
    location 'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/testorcdata/glance_shared';


CREATE EXTERNAL TABLE `hour_glance_shared`(
    `cityid` bigint,
    `clienttime` bigint,
    `eventname` string,
    `glanceid` string,
    `impressionid` string,
    `impressiontype` string,
    `locale` string,
    `partnerid` string,
    `region` string,
    `requesttime` bigint,
    `sdkversion` bigint,
    `sessionid` string,
    `sessionmode` string,
    `source` string,
    `stateid` bigint,
    `time` bigint,
    `userid` string,
    `year` integer,
    `month` integer,
    `day` integer)
PARTITIONED BY ( 
  `apikey` string, 
  `process_date` date, 
  `hour` int)
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
  'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/testorcdata/hour_glance_sharedv1'
TBLPROPERTIES (
  'orc.bloom.filter.columns'='userid,glanceid', 
  'orc.compress'='ZLIB', 
  'transient_lastDdlTime'='1589172120')

insert into table hour_glance_shared select * from glance_shared;


#####################################################################################################################


create external table glance_like(
    `cityid` bigint,
    `clienttime` bigint,
    `eventname` string,
    `glanceid` string,
    `impressionid` string,
    `impressiontype` string,
    `isliked` boolean,
    `locale` string,
    `partnerid` string,
    `region` string,
    `requesttime` bigint,
    `sdkversion` bigint,
    `sessionid` string,
    `sessionmode` string,
    `source` string,
    `stateid` bigint,
    `time` bigint,
    `userid` string,
    `year` integer,
    `month` integer,
    `day` integer
    )
    partitioned by(`apikey` string, `process_date` date, `hour` integer) 
    stored as orc 
    location 'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/testorcdata/glance_like';

CREATE EXTERNAL TABLE `hour_glance_like`(
    `cityid` bigint,
    `clienttime` bigint,
    `eventname` string,
    `glanceid` string,
    `impressionid` string,
    `impressiontype` string,
    `isliked` boolean,
    `locale` string,
    `partnerid` string,
    `region` string,
    `requesttime` bigint,
    `sdkversion` bigint,
    `sessionid` string,
    `sessionmode` string,
    `source` string,
    `stateid` bigint,
    `time` bigint,
    `userid` string,
    `year` integer,
    `month` integer,
    `day` integer)
PARTITIONED BY ( 
  `apikey` string, 
  `process_date` date, 
  `hour` int)
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
  'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/testorcdata/hour_glance_like'
TBLPROPERTIES (
  'orc.bloom.filter.columns'='userid,glanceid', 
  'orc.compress'='ZLIB', 
  'transient_lastDdlTime'='1587560678')

insert into table hour_glance_like select * from glance_like;

###################################################################################################################


create external table cta_started(
    `cityid` bigint,
    `clienttime` bigint,
    `eventname` string,
    `glanceid` string,
    `impressionid` string,
    `impressiontype` string,
    `isofflinepeek` boolean,
    `locale` string,
    `partnerid` string,
    `region` string,
    `requesttime` bigint,
    `sdkversion` bigint,
    `sessionid` string,
    `sessionmode` string,
    `stateid` bigint,
    `time` bigint,
    `userid` string,
    `year` integer,
    `month` integer,
    `day` integer
    )
    partitioned by(`apikey` string, `process_date` date, `hour` integer) 
    stored as orc 
    location 'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/testorcdata/cta_started';


CREATE EXTERNAL TABLE `hour_cta_started`(
    `cityid` bigint,
    `clienttime` bigint,
    `eventname` string,
    `glanceid` string,
    `impressionid` string,
    `impressiontype` string,
    `isofflinepeek` boolean,
    `locale` string,
    `partnerid` string,
    `region` string,
    `requesttime` bigint,
    `sdkversion` bigint,
    `sessionid` string,
    `sessionmode` string,
    `stateid` bigint,
    `time` bigint,
    `userid` string,
    `year` integer,
    `month` integer,
    `day` integer)
PARTITIONED BY ( 
  `apikey` string, 
  `process_date` date, 
  `hour` int)
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
  'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/testorcdata/hour_cta_started'
TBLPROPERTIES (
  'orc.bloom.filter.columns'='userid,glanceid', 
  'orc.compress'='ZLIB', 
  'transient_lastDdlTime'='1595915117')


###########################################################################################################################


create external table cta_ended(
    `cityid` bigint,
    `clienttime` bigint,
    `duration` bigint,
    `eventname` string,
    `failed` boolean,
    `glanceid` string,
    `impressionid` string,
    `impressiontype` string,
    `isofflinepeek` boolean,
    `loadduration` bigint,
    `locale` string,
    `partnerid` string,
    `region` string,
    `requesttime` bigint,
    `sdkversion` bigint,
    `sessionid` string,
    `sessionmode` string,
    `stateid` bigint,
    `time` bigint,
    `userid` string,
    `year` integer,
    `month` integer,
    `day` integer
    )
    partitioned by(`apikey` string, `process_date` date, `hour` integer) 
    stored as orc 
    location 'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/testorcdata/cta_ended';


CREATE EXTERNAL TABLE `hour_cta_ended`(
    `cityid` bigint,
    `clienttime` bigint,
    `duration` bigint,
    `eventname` string,
    `failed` boolean,
    `glanceid` string,
    `impressionid` string,
    `impressiontype` string,
    `isofflinepeek` boolean,
    `loadduration` bigint,
    `locale` string,
    `partnerid` string,
    `region` string,
    `requesttime` bigint,
    `sdkversion` bigint,
    `sessionid` string,
    `sessionmode` string,
    `stateid` bigint,
    `time` bigint,
    `userid` string,
    `year` integer,
    `month` integer,
    `day` integer)
PARTITIONED BY ( 
  `apikey` string, 
  `process_date` date, 
  `hour` int)
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
  'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/testorcdata/hour_cta_ended'
TBLPROPERTIES (
  'orc.bloom.filter.columns'='userid,glanceid', 
  'orc.compress'='ZLIB', 
  'transient_lastDdlTime'='1595915094')


#######################################################################################################################


create external table peek_started(
    `cityid` bigint,
    `clienttime` bigint,
    `eventname` string,
    `glanceid` string,
    `impressionid` string,
    `impressiontype` string,
    `isofflinepeek` boolean,
    `locale` string,
    `partnerid` string,
    `peeksource` string,
    `region` string,
    `requesttime` bigint,
    `sdkversion` bigint,
    `sessionid` string,
    `sessionmode` string,
    `stateid` bigint,
    `time` bigint,
    `userid` string,
    `year` integer,
    `month` integer,
    `day` integer
    )
    partitioned by(`apikey` string, `process_date` date, `hour` integer) 
    stored as orc 
    location 'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/testorcdata/peek_started';


CREATE EXTERNAL TABLE `hour_peek_started`(
    `cityid` bigint,
    `clienttime` bigint,
    `eventname` string,
    `glanceid` string,
    `impressionid` string,
    `impressiontype` string,
    `isofflinepeek` boolean,
    `locale` string,
    `partnerid` string,
    `peeksource` string,
    `region` string,
    `requesttime` bigint,
    `sdkversion` bigint,
    `sessionid` string,
    `sessionmode` string,
    `stateid` bigint,
    `time` bigint,
    `userid` string,
    `year` integer,
    `month` integer,
    `day` integer)
PARTITIONED BY ( 
  `apikey` string, 
  `process_date` date, 
  `hour` int)
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
  'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/testorcdata/hour_peek_started'
TBLPROPERTIES (
  'orc.bloom.filter.columns'='userid,glanceid', 
  'orc.compress'='ZLIB', 
  'transient_lastDdlTime'='1595915118')


create external table peek_ended(
    `cityid` bigint,
    `clienttime` bigint,
    `duration` bigint,
    `eventname` string,
    `glanceid` string,
    `impressionid` string,
    `impressiontype` string,
    `isofflinepeek` boolean,
    `locale` string,
    `partnerid` string,
    `region` string,
    `requesttime` bigint,
    `sdkversion` bigint,
    `sessionid` string,
    `sessionmode` string,
    `stateid` bigint,
    `time` bigint,
    `userid` string,
    `year` integer,
    `month` integer,
    `day` integer
    )
    partitioned by(`apikey` string, `process_date` date, `hour` integer) 
    stored as orc 
    location 'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/testorcdata/peek_ended';



CREATE EXTERNAL TABLE `hour_peek_ended`(
    `cityid` bigint,
    `clienttime` bigint,
    `duration` bigint,
    `eventname` string,
    `glanceid` string,
    `impressionid` string,
    `impressiontype` string,
    `isofflinepeek` boolean,
    `locale` string,
    `partnerid` string,
    `region` string,
    `requesttime` bigint,
    `sdkversion` bigint,
    `sessionid` string,
    `sessionmode` string,
    `stateid` bigint,
    `time` bigint,
    `userid` string,
    `year` integer,
    `month` integer,
    `day` integer)
PARTITIONED BY ( 
  `apikey` string, 
  `process_date` date, 
  `hour` int)
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
  'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/testorcdata/hour_peek_ended'
TBLPROPERTIES (
  'orc.bloom.filter.columns'='userid,glanceid', 
  'orc.compress'='ZLIB', 
  'transient_lastDdlTime'='1595915116');


#########################################################################################################################

create external table video_started(
    `cityid` bigint,
    `clienttime` bigint,
    `eventname` string,
    `glanceid` string,
    `impressionid` string,
    `impressiontype` string,
    `locale` string,
    `partnerid` string,
    `region` string,
    `requesttime` bigint,
    `sdkversion` bigint,
    `sessionid` string,
    `sessionmode` string,
    `stateid` bigint,
    `time` bigint,
    `userid` string,
    `year` integer,
    `month` integer,
    `day` integer
    )
    partitioned by(`apikey` string, `process_date` date, `hour` integer)
    stored as orc 
    location 'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/testorcdata/video_started';


CREATE EXTERNAL TABLE `hour_video_started`(
    `cityid` bigint,
    `clienttime` bigint,
    `eventname` string,
    `glanceid` string,
    `impressionid` string,
    `impressiontype` string,
    `locale` string,
    `partnerid` string,
    `region` string,
    `requesttime` bigint,
    `sdkversion` bigint,
    `sessionid` string,
    `sessionmode` string,
    `stateid` bigint,
    `time` bigint,
    `userid` string,
    `year` integer,
    `month` integer,
    `day` integer)
PARTITIONED BY ( 
  `apikey` string, 
  `process_date` date, 
  `hour` int)
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
  'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/testorcdata/hour_video_started'
TBLPROPERTIES (
  'orc.bloom.filter.columns'='userid,glanceid', 
  'orc.compress'='ZLIB', 
  'transient_lastDdlTime'='1595915119')

#########################################################################################################################

create external table video_ended(
    `cityid` bigint,
    `clienttime` bigint,
    `duration` bigint,
    `eventname` string,
    `failed` boolean,
    `glanceid` string,
    `impressionid` string,
    `impressiontype` string,
    `loadduration` bigint,
    `locale` string,
    `partnerid` string,
    `playcallduration` bigint,
    `playstartduration` bigint,
    `region` string,
    `requesttime` bigint,
    `sdkversion` bigint,
    `sessionid` string,
    `sessionmode` string,
    `stateid` bigint,
    `time` bigint,
    `userid` string,
    `year` integer,
    `month` integer,
    `day` integer
    )
    partitioned by(`apikey` string, `process_date` date, `hour` integer) 
    stored as orc 
    location 'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/testorcdata/video_ended';

CREATE EXTERNAL TABLE `hour_video_ended`(
    `cityid` bigint,
    `clienttime` bigint,
    `duration` bigint,
    `eventname` string,
    `failed` boolean,
    `glanceid` string,
    `impressionid` string,
    `impressiontype` string,
    `loadduration` bigint,
    `locale` string,
    `partnerid` string,
    `playcallduration` bigint,
    `playstartduration` bigint,
    `region` string,
    `requesttime` bigint,
    `sdkversion` bigint,
    `sessionid` string,
    `sessionmode` string,
    `stateid` bigint,
    `time` bigint,
    `userid` string,
    `year` integer,
    `month` integer,
    `day` integer)
PARTITIONED BY ( 
  `apikey` string, 
  `process_date` date, 
  `hour` int)
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
  'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/testorcdata/hour_video_ended'
TBLPROPERTIES (
  'orc.bloom.filter.columns'='userid,glanceid', 
  'orc.compress'='ZLIB', 
  'transient_lastDdlTime'='1595915117')


#########################################################################################################################


create external table glance_started(
    `bubbleimpressionid` string,
    `cityid` bigint,
    `clienttime` bigint,
    `eventname` string,
    `glanceid` string,
    `glanceposition` string,
    `impressionid` string,
    `impressiontype` string,
    `isfeaturebank` boolean,
    `livestoriescount` bigint,
    `locale` string,
    `networktype` string,
    `notificationcount` bigint,
    `partnerid` string,
    `region` string,
    `requesttime` bigint,
    `sdkversion` bigint,
    `sessionid` string,
    `sessionmode` string,
    `source` string,
    `stateid` bigint,
    `time` bigint,
    `userid` string,
    `year` integer,
    `month` integer,
    `day` integer
    )
    partitioned by(`apikey` string, `process_date` date, `hour` integer) 
    stored as orc 
    location 'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/testorcdata/glance_startedv3';



CREATE EXTERNAL TABLE `hour_glance_started`(
    `bubbleimpressionid` string,
    `cityid` bigint,
    `clienttime` bigint,
    `eventname` string,
    `glanceid` string,
    `glanceposition` string,
    `impressionid` string,
    `impressiontype` string,
    `isfeaturebank` boolean,
    `livestoriescount` bigint,
    `locale` string,
    `networktype` string,
    `notificationcount` bigint,
    `partnerid` string,
    `region` string,
    `requesttime` bigint,
    `sdkversion` bigint,
    `sessionid` string,
    `sessionmode` string,
    `source` string,
    `stateid` bigint,
    `time` bigint,
    `userid` string,
    `year` integer,
    `month` integer,
    `day` integer)
PARTITIONED BY ( 
  `apikey` string, 
  `process_date` date, 
  `hour` int)
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
  'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/testorcdata/hour_glance_started'
TBLPROPERTIES (
  'orc.bloom.filter.columns'='userid,glanceid', 
  'orc.compress'='ZLIB', 
  'transient_lastDdlTime'='1595917926')


insert into table hour_glance_started select * from glance_started_part1;