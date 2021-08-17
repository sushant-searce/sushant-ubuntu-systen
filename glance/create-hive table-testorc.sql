

create external table glance_ended(
    `cityid` integer,
    `clienttime` bigint,
    `day` integer,
    `duration` bigint,
    `eventname` string,
    `glanceid` string,
    `holdduration` bigint,
    `impressionid` string,
    `impressiontype` string,
    `locale` string,
    `month` integer,
    `userid` string,
    `partnerid` string,
    `region` string,
    `requesttime` bigint,
    `sdkversion` integer,
    `sessionid` string,
    `sessionmode` string,
    `source` string,
    `stateid` integer,
    `time` bigint,
    `year` integer
    )
    partitioned by(`apikey` string, `process_date` date, `hour` integer) 
    
    stored as orc 
    location 'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/testorcdata/glance_endedv3';


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


create external table glance_shared(
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



msck repair table cta_ended;
msck repair table cta_started;
msck repair table glance_ended;
msck repair table glance_like;
msck repair table glance_started;
msck repair table glance_shared;
msck repair table peek_ended;
msck repair table peek_started;
msck repair table video_ended;
msck repair table video_started;



alter table glance_ended add columns(gpid int);
alter table glance_started add columns(gpid int);

