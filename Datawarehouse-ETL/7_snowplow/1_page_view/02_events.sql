-- Copyright (c) 2016 Snowplow Analytics Ltd. All rights reserved.
--
-- This program is licensed to you under the Apache License Version 2.0,
-- and you may not use this file except in compliance with the Apache License Version 2.0.
-- You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the Apache License Version 2.0 is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
--
-- Version:     0.1.0
--
-- Authors:     Christophe Bogaert
-- Copyright:   Copyright (c) 2016 Snowplow Analytics Ltd
-- License:     Apache License Version 2.0

DROP TABLE IF EXISTS scratch.web_events;
CREATE TABLE scratch.web_events
  DISTKEY(page_view_id)
AS 

  -- select the relevant dimensions from atomic.events

  WITH prep AS (
    SELECT
      TRIM(ev.user_id) AS user_id,
      TRIM(ev.domain_userid) AS domain_userid,
      TRIM(ev.network_userid) AS network_userid,
      TRIM(ev.domain_sessionid) as domain_sessionid,
      ev.domain_sessionidx,
      wp.root_id AS ROOT_ID,
      TRIM(wp.page_view_id) AS page_view_id,
      ev.page_title,
      ev.page_urlhost || ev.page_urlpath AS page_url,
      ev.page_urlscheme,
      ev.page_urlhost,
      ev.page_urlport,
      ev.page_urlpath,
      ev.page_urlquery,
      ev.page_urlfragment,
      ev.refr_urlscheme,
      ev.refr_urlhost,
      ev.refr_urlport,
      ev.refr_urlpath,
      ev.refr_urlquery,
      ev.refr_urlfragment,
      ev.refr_medium,
      ev.refr_source,
      ev.refr_term,
      ev.mkt_medium,
      ev.mkt_source,
      ev.mkt_term,
      ev.mkt_content,
      ev.mkt_campaign,
      ev.mkt_clickid,
      ev.mkt_network,
      ev.geo_country,
      ev.geo_region,
      ev.geo_region_name,
      ev.geo_city,
      ev.geo_zipcode,
      ev.geo_latitude,
      ev.geo_longitude,
      ev.geo_timezone,
      ev.user_ipaddress,
      ev.ip_isp,
      ev.ip_organization,
      ev.ip_domain,
      ev.ip_netspeed,
      ev.app_id,
      ev.useragent,
      ev.br_name,
      ev.br_family,
      ev.br_version,
      ev.br_type,
      ev.br_renderengine,
      ev.br_lang,
      ev.platform,
      ev.dvce_type,
      ev.dvce_ismobile,
      ev.os_name,
      ev.os_family,
      ev.os_manufacturer,
      ev.os_timezone,
      ev.name_tracker, -- included to filter on
      ev.etl_tstamp,           
      ev.dvce_created_tstamp -- included to sort on   
    FROM atomic.events AS ev
    INNER JOIN scratch.web_page_context AS wp -- an INNER JOIN guarantees that all rows have a page view ID
      ON ev.event_id = wp.root_id
    WHERE  (ev.event_name = 'page_view')-- filtering on page view events removes the need for a FIRST_VALUE function
    AND ev.name_tracker NOT IN ('snplow1')
    AND DATE(dvce_created_tstamp) >= '2022-06-01'
    AND ev.useragent NOT LIKE '%Datadog%'
  )

SELECT * FROM (SELECT *, ROW_NUMBER () OVER (PARTITION BY page_view_id ORDER BY dvce_created_tstamp) AS n FROM prep) WHERE n = 1;

INSERT INTO scratch.web_events 
SELECT
    user_id,
    domain_userid,
    network_userid,
    domain_sessionid,
    domain_sessionidx,
    root_id,
    page_view_id,
    page_title,
    page_url,
    page_urlscheme,
    page_urlhost,
    page_urlport,
    page_urlpath,
    page_urlquery,
    page_urlfragment,
    refr_urlscheme,
    refr_urlhost,
    refr_urlport,
    refr_urlpath,
    refr_urlquery,
    refr_urlfragment,
    refr_medium,
    refr_source,
    refr_term,
    mkt_medium,
    mkt_source,
    mkt_term,
    mkt_content,
    mkt_campaign,
    mkt_clickid,
    mkt_network,
    geo_country,
    geo_region,
    geo_region_name,
    geo_city,
    geo_zipcode,
    geo_latitude,
    geo_longitude,
    geo_timezone,
    user_ipaddress,
    ip_isp,
    ip_organization,
    ip_domain,
    ip_netspeed,
    app_id,
    useragent,
    br_name,
    br_family,
    br_version,
    br_type,
    br_renderengine,
    br_lang,
    platform,
    dvce_type,
    dvce_ismobile,
    os_name,
    os_family,
    os_manufacturer,
    os_timezone,
    name_tracker,
    etl_tstamp,
    dvce_created_tstamp,
    n
FROM scratch.web_events_backup
WHERE DATE(dvce_created_tstamp) < '2022-06-01'
;
