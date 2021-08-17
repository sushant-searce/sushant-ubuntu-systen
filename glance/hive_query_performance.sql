INSERT INTO searce.day_glance_bucketed_orc
select
  a.glanceid,
  a.Userid,
  a.glancestarttime,
  a.notificationcount,
  a.networktype,
  a.Sdkversion,
  a.sessionid,
  a.sessionmode,
  a.impressionid,
  a.gpid,
  a.cityid,
  a.region,
  a.isfeaturebank,
  a.livestoriescount,
  a.glanceposition,
  a.bubbleimpressionid,
  a.impressiontype,
  a.glancestartsource,
  a.glanceendsource,
  a.glanceduration,
  a.glanceholdduration,
  a.glanceendtime,
  a.glance_count,
  c.ctastarttime,
  b.ctaendtime,
  b.ctaduration,
  b.ctaloadduration,
  b.ctafailed,
  b.cta_count,
  e.peekstarttime,
  e.peeksource,
  d.peekendtime,
  d.peekduration,
  d.peeks_count,
  g.videostarttime,
  f.videoendtime,
  f.videoduration,
  f.playcallduration,
  f.playstartduration,
  f.videoloadduration,
  f.video_count,
  h.liketime,
  h.like_count,
  i.sharetime,
  i.share_count,
  c.cta_count_started,
  a.Apikey,
  a.process_date
from
  (
    select
      a.glanceid,
      a.Userid,
      max(a.networktype) as networktype,
      a.sessionid,
      a.sessionmode,
      a.impressionid,
      a.gpid,
      max(a.region) as region,
      a.isfeaturebank,
      max(a.glanceposition) as glanceposition,
      a.bubbleimpressionid,
      a.impressiontype,
      a.glancestartsource,
      a.glanceendsource,
      max(a.glanceduration) as glanceduration,
      max(a.glanceholdduration) as glanceholdduration,
      max(a.glanceendtime) as glanceendtime,
      a.process_date,
      a.Apikey,
      max(a.glancestarttime) as glancestarttime,
      max(a.cityid) as cityid,
      sum(a.glance_count) as glance_count,
      max(a.livestoriescount) as livestoriescount,
      max(a.Sdkversion) as Sdkversion,
      max(a.notificationcount) as notificationcount
    from
      (select
a.glanceid,
a.Userid,
c.notificationcount,
c.networktype,
a.Sdkversion,
a.sessionid,
a.sessionmode,
a.impressionid,
a.gpid,
a.region,
c.isfeaturebank,
c.livestoriescount,
c.glanceposition,
c.bubbleimpressionid,
a.impressiontype,
c.glancestartsource,
c.glancestarttime,
a.cityid,
a.glance_count,
a.glanceendsource,
a.glanceduration,
a.glanceholdduration,
a.glanceendtime,
a.Apikey,
a.process_date,
a.hour
from
(select
glanceid,
Userid,
coalesce(impressionid,'null') as impressionid,
sessionid,
sessionmode,
gpid,
region,
impressiontype,
source as glanceendsource,
process_date,
Apikey,max(hour) as hour,
max(Sdkversion) as Sdkversion,
max(duration) as glanceduration,
max(holdduration) as glanceholdduration,
min(time) as glanceendtime,
max(cityid) as cityid,
count(*) as glance_count
from searce.glance_ended
where process_date = date('2021-06-23')
and apikey in ('4f2b0052677b39f442a43747df880c2c','30aedfec48ddd7c42cb8cd855b431a774a0d6b17','00841810b5ff444c03ace404b9458667')
group by 1,2,3,4,5,6,7,8,9,10,11) a
left join
(select
glanceid,
Userid,
networktype,
coalesce(impressionid,'null') as impressionid,
sessionid,
sessionmode,
isfeaturebank,
glanceposition,
bubbleimpressionid,
source as glancestartsource,
max(notificationcount) as notificationcount,
max(livestoriescount) as livestoriescount,
min(time) as glancestarttime
from searce.glance_started
where process_date = date('2021-06-23')
and apikey in ('4f2b0052677b39f442a43747df880c2c','30aedfec48ddd7c42cb8cd855b431a774a0d6b17','00841810b5ff444c03ace404b9458667')
group by 1,2,3,4,5,6,7,8,9,10) c
on a.userid = c.userid and a.glanceid = c.glanceid and a.impressionid = c.impressionid and a.sessionid= c.sessionid
where (case when c.sessionmode = 'HIGHLIGHTS' and c.glancestartsource = 'LS' then a.glanceduration > 500 else TRUE END))a
    where
      process_date = date('2021-06-23')
      and apikey in (
        '4f2b0052677b39f442a43747df880c2c',
        '30aedfec48ddd7c42cb8cd855b431a774a0d6b17',
        '00841810b5ff444c03ace404b9458667'
      )
    group by
      1,
      2,
      4,
      5,
      6,
      7,
      9,
      11,
      12,
      13,
      14,
      18,
      19
  ) A
  left join (
    SELECT
      userid,
      glanceid,
      coalesce(impressionid, 'null') as impressionid,
      sessionid,
      min(time) as ctaendtime,
      max(duration) as ctaduration,
      max(loadduration) as ctaloadduration,
      count(failed) as ctafailed,
      count(*) as cta_count
    from
      searce.cta_ended
    where
      process_date =date('2021-06-23')
      and apikey in (
        '4f2b0052677b39f442a43747df880c2c',
        '30aedfec48ddd7c42cb8cd855b431a774a0d6b17',
        '00841810b5ff444c03ace404b9458667'
      )
    group by
      1,
      2,
      3,
      4
  ) b on (
    a.userid = b.userid
    and a.glanceid = b.glanceid
    and a.impressionid = b.impressionid
    and a.sessionid = b.sessionid
  )
  left join (
    SELECT
      userid,
      glanceid,
      coalesce(impressionid, 'null') as impressionid,
      sessionid,
      min(time) as ctastarttime,
      count(*) as cta_count_started
    from
      searce.cta_started
    where
      process_date = date('2021-06-23')
      and apikey in (
        '4f2b0052677b39f442a43747df880c2c',
        '30aedfec48ddd7c42cb8cd855b431a774a0d6b17',
        '00841810b5ff444c03ace404b9458667'
      )
    group by
      1,
      2,
      3,
      4
  ) C on (
    a.userid = c.userid
    and a.glanceid = c.glanceid
    and a.impressionid = c.impressionid
    and a.sessionid = c.sessionid
  )
  left join (
    SELECT
      userid,
      glanceid,
      coalesce(impressionid, 'null') as impressionid,
      sessionid,
      min(time) as peekendtime,
      max(duration) as peekduration,
      count(*) as peeks_count
    from
      searce.peek_ended
    where
      process_date = date('2021-06-23')
      and apikey in (
        '4f2b0052677b39f442a43747df880c2c',
        '30aedfec48ddd7c42cb8cd855b431a774a0d6b17',
        '00841810b5ff444c03ace404b9458667'
      )
    group by
      1,
      2,
      3,
      4
  ) D on (
    a.userid = d.userid
    and a.glanceid = d.glanceid
    and a.impressionid = d.impressionid
    and a.sessionid = d.sessionid
  )
  left join (
    SELECT
      userid,
      glanceid,
      coalesce(impressionid, 'null') as impressionid,
      sessionid,
      min(time) as peekstarttime,
      max(peeksource) as peeksource
    from
      searce.peek_started
    where
      process_date = date('2021-06-23')
      and apikey in (
        '4f2b0052677b39f442a43747df880c2c',
        '30aedfec48ddd7c42cb8cd855b431a774a0d6b17',
        '00841810b5ff444c03ace404b9458667'
      )
    group by
      1,
      2,
      3,
      4
  ) E on (
    a.userid = e.userid
    and a.glanceid = e.glanceid
    and a.impressionid = e.impressionid
    and a.sessionid = e.sessionid
  )
  left join (
    SELECT
      userid,
      glanceid,
      coalesce(impressionid, 'null') as impressionid,
      sessionid,
      min(time) as videoendtime,
      max(duration) as videoduration,
      max(playcallduration) as playcallduration,
      max(playstartduration) as playstartduration,
      max(loadduration) as videoloadduration,
      count(*) as video_count
    from
      searce.video_ended
    where
      process_date = date('2021-06-23')
      and apikey in (
        '4f2b0052677b39f442a43747df880c2c',
        '30aedfec48ddd7c42cb8cd855b431a774a0d6b17',
        '00841810b5ff444c03ace404b9458667'
      )
    group by
      1,
      2,
      3,
      4
  ) F on (
    a.userid = f.userid
    and a.glanceid = f.glanceid
    and a.impressionid = f.impressionid
    and a.sessionid = f.sessionid
  )
  left join (
    SELECT
      userid,
      glanceid,
      coalesce(impressionid, 'null') as impressionid,
      sessionid,
      min(time) as videostarttime
    from
      searce.video_started
    where
      process_date = date('2021-06-23')
      and apikey in (
        '4f2b0052677b39f442a43747df880c2c',
        '30aedfec48ddd7c42cb8cd855b431a774a0d6b17',
        '00841810b5ff444c03ace404b9458667'
      )
    group by
      1,
      2,
      3,
      4
  ) G on (
    a.userid = g.userid
    and a.glanceid = g.glanceid
    and a.impressionid = g.impressionid
    and a.sessionid = g.sessionid
  )
  left join (
    SELECT
      userid,
      glanceid,
      coalesce(impressionid, 'null') as impressionid,
      sessionid,
      min(time) as liketime,
      COUNT(*) as like_count
    from
      searce.glance_like
    where
      process_date =date('2021-06-23')
      and apikey in (
        '4f2b0052677b39f442a43747df880c2c',
        '30aedfec48ddd7c42cb8cd855b431a774a0d6b17',
        '00841810b5ff444c03ace404b9458667'
      )
    group by
      1,
      2,
      3,
      4
  ) h on (
    a.userid = h.userid
    and a.glanceid = h.glanceid
    and a.impressionid = h.impressionid
    and a.sessionid = h.sessionid
  )
  left join (
    SELECT
      userid,
      glanceid,
      coalesce(impressionid, 'null') as impressionid,
      sessionid,
      min(time) as sharetime,
      count(*) as share_count
    from
      searce.glance_shared
    where
      process_date = date('2021-06-23')
      and apikey in (
        '4f2b0052677b39f442a43747df880c2c',
        '30aedfec48ddd7c42cb8cd855b431a774a0d6b17',
        '00841810b5ff444c03ace404b9458667'
      )
    group by
      1,
      2,
      3,
      4
  ) i on (
    a.userid = i.userid
    and a.glanceid = i.glanceid
    and a.impressionid = i.impressionid
    and a.sessionid = i.sessionid
  )
union all
  (
    select
      coalesce(a.glanceid, c.glanceid) as glanceid,
      coalesce(a.userid, c.userid) as userid,
      a.glancestarttime,
      a.notificationcount,
      a.networktype,
      coalesce(a.Sdkversion, c.Sdkversion) as Sdkversion,
      coalesce(a.sessionid, c.sessionid) as sessionid,
      coalesce(a.sessionmode, c.sessionmode) as sessionmode,
      coalesce(a.impressionid, c.impressionid) as impressionid,
      a.gpid,
      a.cityid,
      coalesce(a.Region, c.Region) as Region,
      a.isfeaturebank,
      a.livestoriescount,
      a.glanceposition,
      a.bubbleimpressionid,
      a.impressiontype,
      a.glancestartsource,
      a.glanceendsource,
      a.glanceduration,
      a.glanceholdduration,
      a.glanceendtime,
      a.glance_count,
      c.ctastarttime,
      c.ctaendtime,
      c.ctaduration,
      c.ctaloadduration,
      c.ctafailed,
      c.cta_count,
      c.peekstarttime,
      c.peeksource,
      c.peekendtime,
      c.peekduration,
      c.peeks_count,
      c.videostarttime,
      c.videoendtime,
      c.videoduration,
      c.playcallduration,
      c.playstartduration,
      c.videoloadduration,
      c.video_count,
      c.liketime,
      c.like_count,
      c.sharetime,
      c.share_count,
      c.cta_count_started,
      coalesce(a.apikey, c.apikey) as apikey,
      coalesce(a.process_date, c.process_date) as process_date
    from
      (
        select
          a.glanceid,
          a.Userid,
          max(a.networktype) as networktype,
          a.sessionid,
          a.sessionmode,
          a.impressionid,
          a.gpid,
          max(a.region) as region,
          a.isfeaturebank,
          max(a.glanceposition) as glanceposition,
          a.bubbleimpressionid,
          a.impressiontype,
          a.glancestartsource,
          a.glanceendsource,
          max(a.glanceduration) as glanceduration,
          max(a.glanceholdduration) as glanceholdduration,
          max(a.glanceendtime) as glanceendtime,
          a.process_date,
          a.Apikey,
          max(a.glancestarttime) as glancestarttime,
          max(a.cityid) as cityid,
          sum(a.glance_count) as glance_count,
          max(a.livestoriescount) as livestoriescount,
          max(a.Sdkversion) as Sdkversion,
          max(a.notificationcount) as notificationcount
        from
          (select
coalesce(a.glanceid,c.glanceid) as glanceid,
coalesce(a.Userid,c.Userid) as userid,
a.notificationcount,
a.networktype,
cast (greatest (coalesce(a.Sdkversion,0),coalesce(c.Sdkversion,0)) as integer) as Sdkversion,
coalesce(a.sessionid,c.sessionid) as sessionid,
coalesce(a.sessionmode,c.sessionmode) as sessionmode,
coalesce(a.impressionid,c.impressionid) as impressionid,
a.gpid,
coalesce(a.region,c.region) as region,
a.isfeaturebank,
a.livestoriescount,
a.glanceposition,
a.bubbleimpressionid,
a.impressiontype,
a.glancestartsource,
a.glancestarttime,
cast (greatest (coalesce(a.cityid,0),coalesce(c.cityid,0)) as integer) as cityid,
cast(greatest (coalesce(a.glance_count,0),coalesce(c.glance_count,0)) as integer) as glance_count,
c.glanceendsource,
c.glanceduration,
c.glanceholdduration,
c.glanceendtime,
coalesce(a.Apikey,c.Apikey) as Apikey,
coalesce(a.process_date,c.process_date) as process_date,
cast (greatest (coalesce(a.hour,0),coalesce(c.hour,0)) as integer) as hour
from
(select
glanceid,
Userid,
networktype,
sessionid,
sessionmode,
coalesce(impressionid,'null') as impressionid,
gpid,
region,
isfeaturebank,
glanceposition,
bubbleimpressionid,
impressiontype,
source as glancestartsource,
process_date,
Apikey,max(hour) as hour,
max(Sdkversion) as Sdkversion,
max(notificationcount) as notificationcount,
max(livestoriescount) as livestoriescount,
min(time) as glancestarttime,
max(cityid) as cityid,
count(*) as glance_count
from searce.glance_started
where process_date =  date('2021-06-23')
and apikey in ('c7a713888d8131a6358089ebd33c2d13','ddd4faee55dc0631d618f5c67a9bc330','04ad407cef3eeee507168469e70ef20c')
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15) a
full outer join
(select
apikey,
glanceid,
Userid,
coalesce(impressionid,'null') as impressionid,
sessionmode,
process_date,sessionid,
source as glanceendsource,
region,
max(hour) as hour,
max(cityid) as cityid,
max(Sdkversion) as Sdkversion,
max(duration) as glanceduration,
max(holdduration) as glanceholdduration,
min(time) as glanceendtime,
count(*) as glance_count
from searce.glance_ended
where process_date =  date('2021-06-23')
and apikey in ('c7a713888d8131a6358089ebd33c2d13','ddd4faee55dc0631d618f5c67a9bc330','04ad407cef3eeee507168469e70ef20c')
group by 1,2,3,4,5,6,7,8,9) c
on a.userid = c.userid and a.glanceid = c.glanceid and a.sessionmode=c.sessionmode and a.process_date=c.process_date
and a.impressionid=c.impressionid and a.sessionid=c.sessionid
where (case when a.sessionmode = 'HIGHLIGHTS' and a.glancestartsource = 'LS' then c.glanceduration > 500 else TRUE END))a
        where
          process_date = date('2021-06-23')
          and apikey in ('c7a713888d8131a6358089ebd33c2d13','ddd4faee55dc0631d618f5c67a9bc330','04ad407cef3eeee507168469e70ef20c')
        group by
          1,
          2,
          4,
          5,
          6,
          7,
          9,
          11,
          12,
          13,
          14,
          18,
          19
      ) A full
      outer join (
        select
          apikey,
          sessionmode,
          userid,
          glanceid,
          impressionid,
          sessionid,
          process_date,
          max(ctastarttime) as ctastarttime,
          max(ctaendtime) as ctaendtime,
          max(ctaduration) as ctaduration,
          max(ctaloadduration) as ctaloadduration,
          max(ctafailed) as ctafailed,
          max(cta_count) as cta_count,
          max(peekstarttime) as peekstarttime,
          max(peeksource) as peeksource,
          max(peekendtime) as peekendtime,
          max(peekduration) as peekduration,
          max(peeks_count) as peeks_count,
          max(videostarttime) as videostarttime,
          max(videoendtime) as videoendtime,
          max(videoduration) as videoduration,
          max(playcallduration) as playcallduration,
          max(playstartduration) as playstartduration,
          max(videoloadduration) as videoloadduration,
          max(video_count) as video_count,
          max(liketime) as liketime,
          max(like_count) as like_count,
          max(sharetime) as sharetime,
          max(share_count) as share_count,
          max(Region) as Region,
          max(Sdkversion) as Sdkversion,
          max (cta_count_started) as cta_count_started
        from
          (
            (
              SELECT
                apikey,
                sessionmode,
                userid,
                glanceid,
                coalesce(impressionid, 'null') as impressionid,
                coalesce(sessionid, 'null') as sessionid,
                process_date,
                null as ctastarttime,
                min(time) as ctaendtime,
                max(duration) as ctaduration,
                max(loadduration) as ctaloadduration,
                count(failed) as ctafailed,
                count(*) as cta_count,
                null as peekstarttime,
                null as peeksource,
                null as peekendtime,
                null as peekduration,
                null as peeks_count,
                null as videostarttime,
                null as videoendtime,
                null as videoduration,
                null as playcallduration,
                null as playstartduration,
                null as videoloadduration,
                null as video_count,
                null as liketime,
                null as like_count,
                null as sharetime,
                null as share_count,
                max(Region) as Region,
                max(Sdkversion) as Sdkversion,
                null as cta_count_started
              from
                searce.cta_ended
              where
                process_date =date('2021-06-23')
                and apikey in ('c7a713888d8131a6358089ebd33c2d13','ddd4faee55dc0631d618f5c67a9bc330','04ad407cef3eeee507168469e70ef20c')
              group by
                1,
                2,
                3,
                4,
                5,
                6,
                7
            )
            union all
              (
                SELECT
                  apikey,
                  sessionmode,
                  userid,
                  glanceid,
                  coalesce(impressionid, 'null') as impressionid,
                  coalesce(sessionid, 'null') as sessionid,
                  process_date,
                  min(time) as ctastarttime,
                  null as ctaendtime,
                  null as ctaduration,
                  null as ctaloadduration,
                  null as ctafailed,
                  null as cta_count,
                  null as peekstarttime,
                  null as peeksource,
                  null as peekendtime,
                  null as peekduration,
                  null as peeks_count,
                  null as videostarttime,
                  null as videoendtime,
                  null as videoduration,
                  null as playcallduration,
                  null as playstartduration,
                  null as videoloadduration,
                  null as video_count,
                  null as liketime,
                  null as like_count,
                  null as sharetime,
                  null as share_count,
                  max(Region) as Region,
                  max(Sdkversion) as Sdkversion,
                  count(*) as cta_count_started
                from
                  searce.cta_started
                where
                  process_date =date('2021-06-23')
                  and apikey in ('c7a713888d8131a6358089ebd33c2d13','ddd4faee55dc0631d618f5c67a9bc330','04ad407cef3eeee507168469e70ef20c')
                group by
                  1,
                  2,
                  3,
                  4,
                  5,
                  6,
                  7
              )
            union all
              (
                SELECT
                  apikey,
                  sessionmode,
                  userid,
                  glanceid,
                  coalesce(impressionid, 'null') as impressionid,
                  coalesce(sessionid, 'null') as sessionid,
                  process_date,
                  null as ctastarttime,
                  null as ctaendtime,
                  null as ctaduration,
                  null as ctaloadduration,
                  null as ctafailed,
                  null as cta_count,
                  null as peekstarttime,
                  null as peeksource,
                  min(time) as peekendtime,
                  max(duration) as peekduration,
                  count(*) as peeks_count,
                  null as videostarttime,
                  null as videoendtime,
                  null as videoduration,
                  null as playcallduration,
                  null as playstartduration,
                  null as videoloadduration,
                  null as video_count,
                  null as liketime,
                  null as like_count,
                  null as sharetime,
                  null as share_count,
                  max(Region) as Region,
                  max(Sdkversion) as Sdkversion,
                  null as cta_count_started
                from
                  searce.peek_ended
                where
                  process_date = date('2021-06-23')
                  and apikey in ('c7a713888d8131a6358089ebd33c2d13','ddd4faee55dc0631d618f5c67a9bc330','04ad407cef3eeee507168469e70ef20c')
                group by
                  1,
                  2,
                  3,
                  4,
                  5,
                  6,
                  7
              )
            union all
              (
                SELECT
                  apikey,
                  sessionmode,
                  userid,
                  glanceid,
                  coalesce(impressionid, 'null') as impressionid,
                  coalesce(sessionid, 'null') as sessionid,
                  process_date,
                  null as ctastarttime,
                  null as ctaendtime,
                  null as ctaduration,
                  null as ctaloadduration,
                  null as ctafailed,
                  null as cta_count,
                  min(time) as peekstarttime,
                  max(peeksource) as peeksource,
                  null as peekendtime,
                  null as peekduration,
                  null as peeks_count,
                  null as videostarttime,
                  null as videoendtime,
                  null as videoduration,
                  null as playcallduration,
                  null as playstartduration,
                  null as videoloadduration,
                  null as video_count,
                  null as liketime,
                  null as like_count,
                  null as sharetime,
                  null as share_count,
                  max(Region) as Region,
                  max(Sdkversion) as Sdkversion,
                  null as cta_count_started
                from
                  searce.peek_started
                where
                  process_date = date('2021-06-23')
                  and apikey in ('c7a713888d8131a6358089ebd33c2d13','ddd4faee55dc0631d618f5c67a9bc330','04ad407cef3eeee507168469e70ef20c')
                group by
                  1,
                  2,
                  3,
                  4,
                  5,
                  6,
                  7
              )
            union all
              (
                SELECT
                  apikey,
                  sessionmode,
                  userid,
                  glanceid,
                  coalesce(impressionid, 'null') as impressionid,
                  coalesce(sessionid, 'null') as sessionid,
                  process_date,
                  null as ctastarttime,
                  null as ctaendtime,
                  null as ctaduration,
                  null as ctaloadduration,
                  null as ctafailed,
                  null as cta_count,
                  null as peekstarttime,
                  null as peeksource,
                  null as peekendtime,
                  null as peekduration,
                  null as peeks_count,
                  null as videostarttime,
                  min(time) as videoendtime,
                  max(duration) as videoduration,
                  max(playcallduration) as playcallduration,
                  max(playstartduration) as playstartduration,
                  max(loadduration) as videoloadduration,
                  count(*) as video_count,
                  null as liketime,
                  null as like_count,
                  null as sharetime,
                  null as share_count,
                  max(Region) as Region,
                  max(Sdkversion) as Sdkversion,
                  null as cta_count_started
                from
                  searce.video_ended
                where
                  process_date = date('2021-06-23')
                  and apikey in ('c7a713888d8131a6358089ebd33c2d13','ddd4faee55dc0631d618f5c67a9bc330','04ad407cef3eeee507168469e70ef20c')
                group by
                  1,
                  2,
                  3,
                  4,
                  5,
                  6,
                  7
              )
            union all
              (
                SELECT
                  apikey,
                  sessionmode,
                  userid,
                  glanceid,
                  coalesce(impressionid, 'null') as impressionid,
                  coalesce(sessionid, 'null') as sessionid,
                  process_date,
                  null as ctastarttime,
                  null as ctaendtime,
                  null as ctaduration,
                  null as ctaloadduration,
                  null as ctafailed,
                  null as cta_count,
                  null as peekstarttime,
                  null as peeksource,
                  null as peekendtime,
                  null as peekduration,
                  null as peeks_count,
                  min(time) as videostarttime,
                  null as videoendtime,
                  null as videoduration,
                  null as playcallduration,
                  null as playstartduration,
                  null as videoloadduration,
                  null as video_count,
                  null as liketime,
                  null as like_count,
                  null as sharetime,
                  null as share_count,
                  max(Region) as Region,
                  max(Sdkversion) as Sdkversion,
                  null as cta_count_started
                from
                  searce.video_started
                where
                  process_date = date('2021-06-23')
                  and apikey in ('c7a713888d8131a6358089ebd33c2d13','ddd4faee55dc0631d618f5c67a9bc330','04ad407cef3eeee507168469e70ef20c')
                group by
                  1,
                  2,
                  3,
                  4,
                  5,
                  6,
                  7
              )
            union all
              (
                SELECT
                  apikey,
                  sessionmode,
                  userid,
                  glanceid,
                  coalesce(impressionid, 'null') as impressionid,
                  coalesce(sessionid, 'null') as sessionid,
                  process_date,
                  null as ctastarttime,
                  null as ctaendtime,
                  null as ctaduration,
                  null as ctaloadduration,
                  null as ctafailed,
                  null as cta_count,
                  null as peekstarttime,
                  null as peeksource,
                  null as peekendtime,
                  null as peekduration,
                  null as peeks_count,
                  null as videostarttime,
                  null as videoendtime,
                  null as videoduration,
                  null as playcallduration,
                  null as playstartduration,
                  null as videoloadduration,
                  null as video_count,
                  min(time) as liketime,
                  COUNT(*) as like_count,
                  null as sharetime,
                  null as share_count,
                  max(Region) as Region,
                  max(Sdkversion) as Sdkversion,
                  null as cta_count_started
                from
                  searce.glance_like
                where
                  process_date = date('2021-06-23')
                  and apikey in ('c7a713888d8131a6358089ebd33c2d13','ddd4faee55dc0631d618f5c67a9bc330','04ad407cef3eeee507168469e70ef20c')
                group by
                  1,
                  2,
                  3,
                  4,
                  5,
                  6,
                  7
              )
            union all
              (
                SELECT
                  apikey,
                  sessionmode,
                  userid,
                  glanceid,
                  coalesce(impressionid, 'null') as impressionid,
                  coalesce(sessionid, 'null') as sessionid,
                  process_date,
                  null as ctastarttime,
                  null as ctaendtime,
                  null as ctaduration,
                  null as ctaloadduration,
                  null as ctafailed,
                  null as cta_count,
                  null as peekstarttime,
                  null as peeksource,
                  null as peekendtime,
                  null as peekduration,
                  null as peeks_count,
                  null as videostarttime,
                  null as videoendtime,
                  null as videoduration,
                  null as playcallduration,
                  null as playstartduration,
                  null as videoloadduration,
                  null as video_count,
                  null as liketime,
                  null as like_count,
                  min(time) as sharetime,
                  count(*) as share_count,
                  max(Region) as Region,
                  max(Sdkversion) as Sdkversion,
                  null as cta_count_started
                from
                  searce.glance_shared
                where
                  process_date = date('2021-06-23')
                  and apikey in ('c7a713888d8131a6358089ebd33c2d13','ddd4faee55dc0631d618f5c67a9bc330','04ad407cef3eeee507168469e70ef20c')
                group by
                  1,
                  2,
                  3,
                  4,
                  5,
                  6,
                  7
              )
          )
        group by
          1,
          2,
          3,
          4,
          5,
          6,
          7
      ) c on a.userid = c.userid
      and a.glanceid = c.glanceid
      and a.impressionid = c.impressionid
      and a.sessionmode = c.sessionmode
      and a.sessionid = c.sessionid
      and a.apikey = c.apikey
      and a.process_date = c.process_date
  )