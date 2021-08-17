create external table glance_started_part1(
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
    location 'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/testorcdata/hour_glance_started_###/part1';
	
	
	
	
create external table glance_started_part2(
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
    location 'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/testorcdata/hour_glance_started_###/part2';
	
	
create external table glance_started_part3(
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
    location 'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/testorcdata/hour_glance_started_###/part3';
	
create external table glance_started_part4(
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
    location 'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/testorcdata/hour_glance_started_###/part4';
	
create external table glance_started_part5(
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
    location 'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/testorcdata/hour_glance_started_###/part5';
	
	
create external table glance_started_part6(
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
    location 'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/testorcdata/hour_glance_started_###/part6';
	
	
create external table glance_started_part7(
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
    location 'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/testorcdata/hour_glance_started_###/part7';
	
	
create external table glance_started_part8(
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
    location 'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/testorcdata/hour_glance_started_###/part8';
	
	
	
create external table glance_started_part9(
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
    location 'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/testorcdata/hour_glance_started_###/part9';
	
	
create external table glance_started_part10(
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
    location 'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/testorcdata/hour_glance_started_###/part10';
	
	
create external table glance_started_part11(
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
    location 'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/testorcdata/hour_glance_started_###/part11';
	
create external table glance_started_part12(
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
    location 'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/testorcdata/hour_glance_started_###/part12';