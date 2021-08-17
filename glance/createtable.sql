CREATE TABLE `glancecdn-sandbox-c32a.glance_inmobi_analytics_poc.day_glance_bucketed_output`(
  glanceid string,
  userid string,
  glancestarttime bigint,
  notificationcount int,
  networktype string,
  sdkversion int,
  sessionid string,
  sessionmode string,
  impressionid string,
  gpid string,
  cityid int,
  region string,
  isfeaturebank boolean,
  livestoriescount int,
  glanceposition string,
  bubbleimpressionid string,
  impressiontype string,
  glancestartsource string,
  glanceendsource string,
  glanceduration int,
  glanceholdduration bigint,
  glanceendtime bigint,
  glance_count int,
  ctastarttime bigint,
  ctaendtime bigint,
  ctaduration int,
  ctaloadduration int,
  ctafailed int,
  cta_count int,
  peekstarttime bigint,
  peeksource string,
  peekendtime bigint,
  peekduration int,
  peeks_count int,
  videostarttime bigint,
  videoendtime bigint,
  videoduration int,
  playcallduration int,
  playstartduration int,
  videoloadduration int,
  video_count int,
  liketime bigint,
  like_count int,
  sharetime bigint,
  share_count int,
  cta_count_started bigint
  apikey string
  process_date date)
PARTITIONED BY (
  apikey string,
  process_date date)
CLUSTERED BY (
  userid)
SORTED BY (
  userid ASC)
;