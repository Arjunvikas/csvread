create volatile table rpt_dt as (
select max(ec_cre_dt) as rpt_dt from PP_PRODUCT_VIEWS.FACT_FPTI_MS_EC_RPT
where ec_cre_dt = 1190619
) with data on commit preserve rows
;

DROP TABLE pp_scratch.dl_ul_evnts_tokens;
CREATE MULTISET TABLE pp_scratch.dl_ul_evnts_tokens AS (
SELECT
evnt_dt,
evnt_key,
evnt_ts_epoch + evnt_ts_msecs AS evnt_ts,
buyer_ip_country AS buyer_ip_country,
component AS component,
Coalesce(Cast(td_sysfnlib.Nvp(payload,'browser_type', '&', '=') AS VARCHAR(100)),'#') AS browser_type,
Coalesce(Cast(td_sysfnlib.Nvp(payload,'device_type', '&', '=') AS VARCHAR(100)),'#') AS device_type,
sessn_id AS user_session_guid,
visitor_id AS visitor_id,
CASE WHEN cust_id NOT LIKE 'EAP%' THEN cust_id end AS cust_id,
Coalesce(Cast(td_sysfnlib.Nvp(payload,'buyer_id', '&', '=') AS VARCHAR(100)),'#') AS buyer_id,
context_id AS context_id,

is_cookied_user AS  is_cookied_user,
Cast(td_sysfnlib.Nvp(payload,'is_cookied', '&', '=') AS VARCHAR(100)) AS is_cookied,
Cast(td_sysfnlib.Nvp(payload,'login_status', '&', '=') AS VARCHAR(100)) AS login_status,
state_name AS   state_name,
transition_name AS   transition_name,
Cast(td_sysfnlib.Nvp(payload,'ul_process_state', '&', '=') AS VARCHAR(100)) AS ul_process_state,
int_error_code AS   int_error_code,
int_error_desc AS   int_error_desc,
Cast(td_sysfnlib.Nvp(payload,'experimentation_experience', '&', '=') AS VARCHAR(100)) AS experimentation_experience,
Cast(td_sysfnlib.Nvp(payload,'experimentation_treatment', '&', '=') AS VARCHAR(100)) AS experimentation_treatment,
Cast(td_sysfnlib.Nvp(payload,'pub_cred_type', '&', '=') AS VARCHAR(100)) AS pub_cred_type,
Cast(td_sysfnlib.Nvp(payload,'login_error', '&', '=') AS VARCHAR(100)) AS login_error
--payload
FROM pp_polestar_views.fact_ea_evnt
WHERE biz_evnt_key = 37
AND evnt_dt = (select rpt_dt - 1 from rpt_dt)
AND component ='unifiedloginnodeweb'
AND context_id <> '#'
)  WITH DATA PRIMARY INDEX (evnt_key)
;

DROP TABLE pp_oap_sing_agnal_k_t.dl_ul_hybrid_pxp_start_xo_tokens;
CREATE MULTISET TABLE pp_oap_sing_agnal_k_t.dl_ul_hybrid_pxp_start_xo_tokens AS (
SELECT
evnt_dt,
user_session_guid,
context_id,
is_cookied,
CASE 
WHEN experimentation_treatment LIKE ANY ('%9712%','%9714%','%11171%','%11177%','%11575%','%11572%','%11579%','%11577%','%11583%','%11581%','%11587%','%11585%','%11591%','%11589%','%11595%','%11593%') THEN 'Test' 
WHEN experimentation_treatment  LIKE ANY  ('%9713%','%9715%','%11172%','%11178%','%11574%','%11571%','%11578%','%11576%','%11582%','%11580%','%11586%','%11584%','%11590%','%11588%','%11594%','%11592%') THEN 'Control' ELSE 'Others' END AS pxp_test_group,

CASE WHEN experimentation_experience LIKE ANY ( '%4075%', '%4637%','%4791%','%4793%','%4795%','%4797%','%4799%','%4801%')THEN 'Hybrid-Uncookied'
WHEN experimentation_experience LIKE Any  ( '%4076%', '%4639%','%4792%','%4794%','%4796%','%4798%','%4800%','%4802%') THEN 'Hybrid-cookied' end AS EXPER_NAME,

CASE WHEN experimentation_experience LIKE ANY ( '%4639%', '%4637%')THEN 'IN'
WHEN experimentation_experience LIKE any ( '%4076%', '%4075%') THEN 'US' 
WHEN experimentation_experience LIKE any ( '%4792%', '%4791%') THEN 'DE' 
WHEN experimentation_experience LIKE any ( '%4794%', '%4793%') THEN 'GB' 
WHEN experimentation_experience LIKE any ( '%4796%', '%4795%') THEN 'AUCA' 
WHEN experimentation_experience LIKE any ( '%4798%', '%4797%') THEN 'EMEA' 
WHEN experimentation_experience LIKE any ( '%4800%', '%4799%') THEN 'LATAM' 
WHEN experimentation_experience LIKE any ( '%4802%', '%4801%') THEN 'APAC' 
end AS Country

FROM pp_scratch.dl_ul_evnts_tokens
WHERE experimentation_experience LIKE ANY ('%4075%', '%4076%', '%4639%', '%4637%','%4792%','%4791%','%4794%','%4793%','%4796%','%4795%','%4798%','%4797%','%4800%','%4799%','%4801%','%4802%') 
QUALIFY Row_Number() Over(PARTITION BY context_id ORDER BY evnt_ts) = 1 
)WITH DATA UNIQUE PRIMARY INDEX(context_id)
;



DROP TABLE pp_oap_sing_agnal_k_t.dl_ul_hybrid_pxp_start_rpt;
CREATE MULTISET TABLE pp_oap_sing_agnal_k_t.dl_ul_hybrid_pxp_start_rpt AS (
SELECT 
evnt_dt,
context_id,
user_session_guid,
is_cookied,
pxp_test_group,
EXPER_NAME,
Country,
subsequent_y_n,
CASE 
WHEN 
rpt.xo_product = 'EC'
AND rpt.pp_test_merch_y_n = 'N' 
AND rpt.intrnl_traffic_y_n = 'N'
AND rpt.rt_rp_txn_y_n = 'N'
--and cntxt_stitched_y_n = 'N'
AND rpt.redirect_blacklist_y_n = 'N'
AND rpt.multi_slr_unilateral_y_n = 'N'
AND rpt.ec_token_deduped_y_n = 'N'
--and rpt.rcvr_id <> '1219054521827007990'
AND rpt.pp_plus_noise_y_n = 'N'
AND rpt.cntxt_type NOT IN ('flowlogging','WPS-Token','Cart-ID') THEN 'Y' 
WHEN rpt.xo_product = 'WPS'
AND rpt.wps_official_conversion_y_n = 'Y' 
AND rpt.wps_bot_y_n = 'N' THEN 'Y'
ELSE 'N' end AS official_conversion_y_n,
starts,
prepare_rvw_y_n,
dones
FROM pp_oap_sing_agnal_k_t.dl_ul_hybrid_pxp_start_xo_tokens a
LEFT JOIN PP_PRODUCT_VIEWS.FACT_FPTI_MS_EC_RPT rpt ON a.context_id = rpt.ec_token_id AND rpt.ec_cre_dt = (select rpt_dt - 1 from rpt_dt)
) WITH DATA PRIMARY INDEX(context_id)
;


DROP TABLE PP_OAP_SING_MANI_T.me_hybridXO_submit_pub_cred_type;
CREATE MULTISET TABLE PP_OAP_SING_MANI_T.me_hybridXO_submit_pub_cred_type AS (
SELECT
context_id,
pub_cred_type
FROM pp_scratch.dl_ul_evnts_tokens
WHERE state_name = 'begin_hybrid_login' AND transition_name IN ('process_hybrid','process_next') AND pub_cred_type IS NOT NULL
QUALIFY Row_Number() Over (PARTITION BY context_id ORDER BY evnt_ts) = 1
) WITH DATA PRIMARY INDEX(context_id)
;


DROP TABLE PP_OAP_SING_MANI_T.me_hybridXO_pwd_render_pub_cred_type;
CREATE MULTISET TABLE PP_OAP_SING_MANI_T.me_hybridXO_pwd_render_pub_cred_type AS (
SELECT
context_id,
pub_cred_type
FROM pp_scratch.dl_ul_evnts_tokens
WHERE state_name = 'begin_hybrid_pwd' AND transition_name LIKE 'prepare_hybrid_pwd%' AND pub_cred_type IS NOT NULL
QUALIFY Row_Number() Over (PARTITION BY context_id ORDER BY evnt_ts) = 1
) WITH DATA PRIMARY INDEX(context_id)
;

DROP TABLE PP_OAP_SING_MANI_T.me_hybridXO_pwd_submit_pub_cred_type;
CREATE MULTISET TABLE PP_OAP_SING_MANI_T.me_hybridXO_pwd_submit_pub_cred_type AS (
SELECT
context_id,
pub_cred_type
FROM pp_scratch.dl_ul_evnts_tokens
WHERE state_name = 'begin_hybrid_pwd' AND transition_name  IN ('process_hybrid_pwd','process_hybrid_pwd_ot','process_2fa','process_stepupRequired','process_safeRequired') AND pub_cred_type IS NOT NULL
QUALIFY Row_Number() Over (PARTITION BY context_id ORDER BY evnt_ts) = 1
) WITH DATA PRIMARY INDEX(context_id)
;

DROP TABLE pp_scratch.me_regularXO_submit_pub_cred_type;
CREATE MULTISET TABLE pp_scratch.me_regularXO_submit_pub_cred_type AS (
SELECT
context_id,
pub_cred_type
FROM pp_scratch.dl_ul_evnts_tokens
WHERE state_name IN ('begin_email','begin_phone') AND transition_name IN ('process_email','process_phone','process_next') AND pub_cred_type IS NOT NULL
QUALIFY Row_Number() Over (PARTITION BY context_id ORDER BY evnt_ts) = 1
) WITH DATA PRIMARY INDEX(context_id)
;


DROP TABLE PP_OAP_SING_MANI_T.me_regularXO_pwd_render_pub_cred_type;
CREATE MULTISET TABLE PP_OAP_SING_MANI_T.me_regularXO_pwd_render_pub_cred_type AS (
SELECT
context_id,
pub_cred_type
FROM pp_scratch.dl_ul_evnts_tokens
WHERE state_name = 'begin_pwd' AND transition_name LIKE 'prepare_pwd%' AND pub_cred_type IS NOT NULL
QUALIFY Row_Number() Over (PARTITION BY context_id ORDER BY evnt_ts) = 1
) WITH DATA PRIMARY INDEX(context_id)
;


DROP TABLE PP_OAP_SING_MANI_T.me_regularXO_pwd_submit_pub_cred_type;
CREATE MULTISET TABLE PP_OAP_SING_MANI_T.me_regularXO_pwd_submit_pub_cred_type AS (
SELECT
context_id,
pub_cred_type
FROM pp_scratch.dl_ul_evnts_tokens
WHERE state_name IN ('begin_phone_pwd','begin_pwd') AND transition_name  IN ('process_pwd','process_pwd_ot','process_2fa','process_stepupRequired','process_safeRequired') AND pub_cred_type IS NOT NULL
QUALIFY Row_Number() Over (PARTITION BY context_id ORDER BY evnt_ts) = 1
) WITH DATA PRIMARY INDEX(context_id)
;

DROP TABLE PP_OAP_SING_MANI_T.me_final_pub_cred_type;
CREATE MULTISET TABLE PP_OAP_SING_MANI_T.me_final_pub_cred_type AS (
SELECT
context_id,
pub_cred_type
FROM pp_scratch.dl_ul_evnts_tokens
WHERE pub_cred_type IS NOT NULL
QUALIFY Row_Number() Over (PARTITION BY context_id ORDER BY evnt_ts DESC ) = 1
) WITH DATA PRIMARY INDEX(context_id)
;

DROP TABLE pp_oap_sing_agnal_k_t.dl_ul_hybrid_tokens;
CREATE MULTISET TABLE pp_oap_sing_agnal_k_t.dl_ul_hybrid_tokens AS (
SELECT 
evnt_dt,
a.context_id,
Max(buyer_ip_country) AS buyer_ip_country,
Max(login_status) AS login_status,
Max(buyer_id) AS buyer_id,
Max(cust_id) AS cust_id,
Max(browser_type) AS browser_type,
Max(device_type) AS device_type,
Max(login_error) AS login_error,
Max(CASE WHEN state_name IN ('begin_email','begin_phone') AND transition_name IN ('prepare_email','prepare_phone') THEN 1 ELSE 0 end) AS email_page_rendered_y_n,
Max(CASE WHEN state_name IN ('begin_email','begin_phone') AND transition_name IN ('process_email','process_next','process_phone') THEN 1 ELSE 0 end) AS email_page_submit_y_n,
Max(CASE WHEN state_name IN ('begin_email','begin_phone') AND transition_name IN ('process_email','process_next','process_phone') AND Coalesce(int_error_code,'#') = '#' THEN 1 ELSE 0 end) AS email_page_success_y_n,
Max(CASE WHEN state_name IN ('begin_pwd','begin_phone_pwd') AND transition_name LIKE 'prepare_pwd%' THEN 1 ELSE 0 end) AS pwd_page_rendered_y_n,
Max(CASE WHEN state_name IN ('begin_pwd','begin_phone_pwd') AND transition_name IN ('process_pwd','process_pwd_ot','process_phone_pwd','process_2fa','process_stepupRequired','process_safeRequired') THEN 1 ELSE 0 end) AS pwd_page_submit_y_n,
Max(CASE WHEN state_name IN ('begin_pwd','begin_phone_pwd') AND transition_name IN ('process_pwd','process_pwd_ot','process_phone_pwd') AND Coalesce(int_error_code,'#') = '#' THEN 1 ELSE 0 end) AS pwd_page_success_y_n,

Max(CASE WHEN state_name IN ('begin_phone') AND transition_name IN ('prepare_email') THEN 1 ELSE 0 end) AS phone_email_switch,
Max(CASE WHEN state_name IN ('begin_email') AND transition_name IN ('prepare_phone') THEN 1 ELSE 0 end) AS email_phone_switch,

--hybrid
Max(CASE WHEN state_name = 'begin_hybrid_login' AND transition_name = 'prepare_hybrid' THEN 1 ELSE 0 end) AS hybrid_page_rendered_y_n,
Max(CASE WHEN state_name = 'begin_hybrid_login' AND transition_name IN ('process_hybrid','process_next') THEN 1 ELSE 0 end) AS hybrid_page_submit_y_n,
Max(CASE WHEN state_name = 'begin_hybrid_login' AND transition_name IN ('process_hybrid','process_next') AND Coalesce(int_error_code,'#') = '#' THEN 1 ELSE 0 end) AS hybrid_page_success_y_n,
Max(CASE WHEN state_name = 'begin_hybrid_pwd' AND transition_name IN ('prepare_hybrid_pwd', 'prepare_hybrid_pwd_ot') THEN 1 ELSE 0 end) AS hybrid_pwd_page_rendered_y_n,
Max(CASE WHEN state_name = 'begin_hybrid_pwd'  AND transition_name IN ('process_hybrid_pwd','process_hybrid_pwd_ot','process_2fa','process_stepupRequired','process_safeRequired') THEN 1 ELSE 0 end) AS hybrid_pwd_page_submit_y_n,
Max(CASE WHEN state_name = 'begin_hybrid_pwd'  AND transition_name IN ('process_hybrid_pwd', 'process_hybrid_pwd_ot') AND Coalesce(int_error_code,'#') = '#' THEN 1 ELSE 0 end) AS hybrid_pwd_page_success_y_n,

-- change in hybrid cookied
Max(CASE WHEN state_name = 'begin_hybrid_pwd'  AND transition_name = 'process_hybrid_pwd_not_you' THEN 1 ELSE 0 end) AS hybrid_pwd_page_change_y_n,

Cast ('#' AS VARCHAR (100)) AS hybrid_email_submit_pub_cred,
Cast ('#' AS VARCHAR (100)) AS hybrid_pwd_render_pub_cred,
Cast ('#' AS VARCHAR (100)) AS hybrid_pwd_submit_pub_cred,
Cast ('#' AS VARCHAR (100)) AS regular_email_submit_pub_cred,
Cast ('#' AS VARCHAR (100)) AS regular_pwd_render_pub_cred,
Cast ('#' AS VARCHAR (100)) AS regular_pwd_submit_pub_cred,
Cast ('#' AS VARCHAR (100)) AS last__pub_cred

FROM pp_scratch.dl_ul_evnts_tokens a

GROUP BY 1,2
) WITH DATA PRIMARY INDEX(context_id)
;


UPDATE  a
FROM pp_oap_sing_agnal_k_t.dl_ul_hybrid_tokens a,
PP_OAP_SING_MANI_T.me_hybridXO_submit_pub_cred_type b
SET hybrid_email_submit_pub_cred = b.pub_cred_type

WHERE 
a.context_id = b.context_id;


UPDATE  a
FROM pp_oap_sing_agnal_k_t.dl_ul_hybrid_tokens a,
PP_OAP_SING_MANI_T.me_hybridXO_pwd_render_pub_cred_type b
SET hybrid_pwd_render_pub_cred = b.pub_cred_type

WHERE 
a.context_id = b.context_id;

--->Processed

UPDATE  a
FROM pp_oap_sing_agnal_k_t.dl_ul_hybrid_tokens a,
PP_OAP_SING_MANI_T.me_hybridXO_pwd_submit_pub_cred_type b
SET hybrid_pwd_submit_pub_cred = b.pub_cred_type

WHERE 
a.context_id = b.context_id;


UPDATE  a
FROM pp_oap_sing_agnal_k_t.dl_ul_hybrid_tokens a,
PP_OAP_SING_MANI_T.me_regularXO_pwd_render_pub_cred_type b
SET regular_pwd_render_pub_cred = b.pub_cred_type

WHERE 
a.context_id = b.context_id;


UPDATE  a
FROM pp_oap_sing_agnal_k_t.dl_ul_hybrid_tokens a,
pp_scratch.me_regularXO_submit_pub_cred_type b
SET  regular_email_submit_pub_cred= b.pub_cred_type
WHERE 
a.context_id = b.context_id;


UPDATE  a
FROM pp_oap_sing_agnal_k_t.dl_ul_hybrid_tokens a,
PP_OAP_SING_MANI_T.me_regularXO_pwd_render_pub_cred_type b

SET regular_pwd_render_pub_cred = b.pub_cred_type

WHERE 
a.context_id = b.context_id;


UPDATE  a

FROM pp_oap_sing_agnal_k_t.dl_ul_hybrid_tokens a,
PP_OAP_SING_MANI_T.me_regularXO_pwd_submit_pub_cred_type b

SET regular_pwd_submit_pub_cred = b.pub_cred_type

WHERE 
a.context_id = b.context_id;


UPDATE  a
FROM pp_oap_sing_agnal_k_t.dl_ul_hybrid_tokens a,
PP_OAP_SING_MANI_T.me_final_pub_cred_type b
SET  last__pub_cred = b.pub_cred_type

WHERE 
a.context_id = b.context_id;


/* DELETE FROM pp_scratch.me_sts_login_status_value  ;
INSERT INTO pp_scratch.me_sts_login_status_value
SELECT 
evnt_key,
evnt_dt,
evnt_ts_msecs + evnt_ts_epoch AS evnt_ts,
COALESCE(CAST(td_sysfnlib.NVP(payload,'user_session_guid', '&', '=') AS VARCHAR(100)),'#') AS user_session_guid,
COALESCE(CAST(td_sysfnlib.NVP(payload,'bizeventname', '&', '=') AS VARCHAR(100)),'#')  AS bizeventname ,
COALESCE(CAST(td_sysfnlib.NVP(payload,'api_name', '&', '=') AS VARCHAR(100)),'#') AS api_name,
COALESCE(CAST(td_sysfnlib.NVP(payload,'auth_req_status', '&', '=') AS VARCHAR(100)),'#') AS auth_req_status,
COALESCE(CAST(td_sysfnlib.NVP(payload,'context_id', '&', '=') AS VARCHAR(100)),'#') AS context_id

FROM pp_eap_access_views.fact_ea_evnt

WHERE eap_stream_key = 4 
AND eap_evnt_key = 37
AND evnt_dt = (select rpt_dt - 1 from rpt_dt)
AND payload LIKE '%component=identitysecuretokenserv%'
AND payload LIKE '%auth_req_status%'
AND payload LIKE ANY ('%/v1/oauth2/login%','%/v1/oauth2/token%'); */


DELETE FROM pp_scratch.me_sts_login_status_value  ;
INSERT INTO pp_scratch.me_sts_login_status_value
SELECT 
evnt_key,
evnt_dt,
evnt_ts_msecs + evnt_ts_epoch AS evnt_ts,
Cast( NEW JSON(td_sysfnlib.Nvp(payload,'x_paypal_fpti_hdr','&','=') ).JSONExtractvalue('$..user_session_guid') AS VARCHAR(100) )AS user_session_guid,
Coalesce(Cast(td_sysfnlib.Nvp(payload,'bizeventname', '&', '=') AS VARCHAR(100)),'#')  AS bizeventname ,
api_name,
auth_req_status,
context_id

FROM pp_polestar_views.fact_ea_evnt

WHERE biz_evnt_key = 140
AND evnt_dt = (select rpt_dt - 1 from rpt_dt)
AND component='identitysecuretokenserv'
AND api_name LIKE ANY ('%/v1/oauth2/login%','%/v1/oauth2/token%')
AND context_id <> '#';

 
DELETE FROM pp_oap_sing_mani_t.me_sts_last_evnt_login WHERE evnt_dt = (select rpt_dt - 1 from rpt_dt);
--CREATE MULTISET TABLE pp_oap_sing_mani_t.me_sts_last_evnt_login AS (
INSERT INTO pp_oap_sing_mani_t.me_sts_last_evnt_login

SELECT
* 
FROM 
pp_scratch.me_sts_login_status_value

WHERE  context_id <> '#'
AND  evnt_dt = (select rpt_dt - 1 from rpt_dt)
AND context_id NOT IN ( SEL Context_id FROM pp_oap_sing_mani_t.me_sts_last_evnt_login GROUP BY 1)
QUALIFY Row_Number() Over( PARTITION BY Context_id ORDER BY evnt_ts DESC) = 1 ; 

--) WITH DATA 
--UNIQUE PRIMARY INDEX (context_id) ;
/* 
DROP TABLE pp_oap_sing_mani_t.me_cust_id_token;
CREATE MULTISET TABLE pp_oap_sing_mani_t.me_cust_id_token AS ( */
DROP TABLE pp_oap_sing_agnal_k_t.SCIM_Call;
CREATE MULTISET TABLE pp_oap_sing_agnal_k_t.SCIM_Call AS(
SELECT 
evnt_key,
evnt_dt,
evnt_ts_epoch + evnt_ts_msecs AS evnt_ts,
cust_id,
Coalesce(Cast(td_sysfnlib.Nvp(payload,'user_guid_hdr', '&', '=') AS VARCHAR(100)),'#') AS user_guid,
Coalesce(Cast(td_sysfnlib.Nvp(payload,'user_session_guid_hdr', '&', '=') AS VARCHAR(100)),'#') AS user_session_guid,
encr_cust_id AS encrypted_customer_id,
Coalesce(context_id,'#') AS context_id,
Coalesce(api_name,'#') AS api_name,
Coalesce(api_response_code,'#') AS api_response_code,
Coalesce(int_error_code,'#') AS int_error_code,
Coalesce(int_error_desc,'#') AS int_error_desc,
Coalesce(ext_error_code,'#') AS ext_error_code,
Coalesce(ext_error_desc,'#') AS ext_error_desc,
component,
correlation_id AS cal_correlation_id

FROM 
 pp_polestar_views.fact_ea_evnt

WHERE biz_evnt_key = 99
AND evnt_dt = (select rpt_dt - 1 from rpt_dt)
AND api_name='TOKEN'
AND api_response_code IN ('200','201')
AND encrypted_customer_id <> '#'
) WITH DATA PRIMARY INDEX (evnt_key);


DELETE FROM  pp_oap_sing_mani_t.me_cust_id_token WHERE evnt_dt = (select rpt_dt - 1 from rpt_dt);
INSERT INTO pp_oap_sing_mani_t.me_cust_id_token
SEL
a.evnt_dt,
a.Context_id,
Cust_id,
evnt_ts

FROM
pp_oap_sing_agnal_k_t.SCIM_Call a
JOIN
pp_oap_sing_agnal_k_t.dl_ul_hybrid_pxp_start_xo_tokens b
ON
a.context_id = b.context_id

WHERE a.context_id <> '#'
AND a.evnt_dt = (select rpt_dt - 1 from rpt_dt)
AND b.evnt_dt = (select rpt_dt - 1 from rpt_dt)

QUALIFY Row_Number() Over (PARTITION BY a.context_id,a.evnt_dt  ORDER BY evnt_ts DESC) = 1;

/* ) WITH DATA 
UNIQUE PRIMARY INDEX (context_id, evnt_dt);
 */
DROP TABLE pp_oap_sing_mani_t.me_evnt_login_last_feed_name;
CREATE MULTISET TABLE pp_oap_sing_mani_t.me_evnt_login_last_feed_name AS 
(
SELECT 
a.context_id ,
feed_name

FROM pp_polestar_views.fact_ea_evnt a 
WHERE  a.biz_evnt_key = 23 
AND a.evnt_dt = (select rpt_dt - 1 from rpt_dt)
AND state_name <> 'pxp_check'
AND context_id not in ('#', 'EC-2AA71663NU965484J')
QUALIFY Row_Number() Over(PARTITION BY context_id ORDER BY evnt_ts_epoch+ evnt_ts_msecs DESC) = 1
)WITH DATA 
PRIMARY INDEX(context_id );

/*select
context_id,
count(*)
from pp_polestar_views.fact_ea_evnt a
where 1=1
and a.biz_evnt_key = 23 
and a.evnt_dt = (select rpt_dt - 1 from rpt_dt)
group by 1
order by 2 desc*/

-- Alter tablepp_oap_Sing_shuqi_t.me_hybrid_pxp_login_tokens add country as varchar (2);
-- update a 
-- frompp_oap_Sing_shuqi_t.me_hybrid_pxp_login_tokens a
-- set country = 'US';


--DROP TABLEpp_oap_Sing_shuqi_t.me_hybrid_pxp_login_tokens;
DELETE FROM pp_oap_Sing_shuqi_t.me_hybrid_pxp_login_tokens WHERE evnt_dt = (select rpt_dt - 1 from rpt_dt);
--CREATE MULTISET TABLE pp_oap_Sing_shuqi_t.me_hybrid_pxp_login_tokens AS (
INSERT INTO pp_oap_Sing_shuqi_t.me_hybrid_pxp_login_tokens

SELECT
a.evnt_dt,
a.context_id,
a.user_session_guid,

-- seg
a.pxp_test_group,
a.is_cookied,
e.buyer_ip_country,
e.browser_type,
e.device_type,

--rpt
official_conversion_y_n,
starts,
prepare_rvw_y_n,
dones,

-- login
e.login_status,

-- Regular
CASE WHEN email_page_rendered_y_n = 1  THEN 1 ELSE 0 end AS email_shown,
CASE WHEN email_page_submit_y_n = 1 THEN 1 ELSE 0 end AS email_submit,
CASE WHEN email_page_success_y_n = 1 THEN 1 ELSE 0 end AS email_success,
CASE WHEN pwd_page_rendered_y_n = 1  THEN 1 ELSE 0 end AS pwd_shown,
CASE WHEN pwd_page_submit_y_n = 1 THEN 1 ELSE 0 end AS pwd_submit,
CASE WHEN pwd_page_success_y_n = 1 THEN 1 ELSE 0 end AS pwd_success,

-- hybrid
CASE WHEN hybrid_page_rendered_y_n = 1 THEN 1 ELSE 0 end AS hybrid_shown,
CASE WHEN hybrid_page_submit_y_n = 1 THEN 1 ELSE 0 end AS hybrid_submit,
CASE WHEN hybrid_page_success_y_n = 1 THEN 1 ELSE 0 end AS hybrid_success,
CASE WHEN hybrid_pwd_page_rendered_y_n = 1 THEN 1 ELSE 0 end AS hybrid_pwd_shown,
CASE WHEN hybrid_pwd_page_submit_y_n = 1 THEN 1 ELSE 0 end AS hybrid_pwd_submit,
CASE WHEN hybrid_pwd_page_success_y_n = 1 THEN 1 ELSE 0 end AS hybrid_pwd_success,
CASE WHEN hybrid_pwd_page_change_y_n = 1 THEN 1 ELSE 0 end AS hybrid_pwd_change,

--- Signup and STS success
CASE WHEN b.auth_req_status = '0'  THEN 1  ELSE 0  END AS STS_success, 
CASE WHEN b.bizeventname = 'LOGIN_ENDPOINT_PASSWORD_Phone' THEN 1 ELSE 0 END AS phone_login,
CASE WHEN c.feed_name = 'xoonboardingnodeweb' THEN 1 ELSE 0 end AS Signup,
CASE WHEN d.cust_id IS NOT NULL THEN 'Loginable' ELSE 'Non-Loginable' end  AS Login_flag,

---hybrid pubcred
hybrid_email_submit_pub_cred,
hybrid_pwd_render_pub_cred,
hybrid_pwd_submit_pub_cred,

---regular pubcred
regular_email_submit_pub_cred,
regular_pwd_render_pub_cred,
regular_pwd_submit_pub_cred,

--last pubcred
last__pub_cred,
login_error,
EXPER_NAME,
CASE WHEN EXPER_NAME = 'Hybrid-Uncookied'  THEN 'N'  ELSE subsequent_y_n END AS subsequent_y_n,
Country
  
FROM pp_oap_sing_agnal_k_t.dl_ul_hybrid_pxp_start_rpt a 

LEFT JOIN pp_oap_sing_agnal_k_t.dl_ul_hybrid_tokens e 
ON a.evnt_dt = e.evnt_dt AND a.context_id = e.context_id

LEFT JOIN pp_oap_sing_mani_t.me_sts_last_evnt_login b
ON a.context_id = b.context_id 
AND a.evnt_dt = b.evnt_dt

LEFT JOIN pp_oap_sing_mani_t.me_evnt_login_last_feed_name c
ON a.context_id = c.context_id 

LEFT JOIN pp_oap_sing_mani_t.me_cust_id_token d
ON a.context_id = d.context_id

WHERE 1=1
AND a.evnt_dt = (select rpt_dt - 1 from rpt_dt)

--) WITH DATA PRIMARY INDEX(context_id)
;
/*
Alter table pp_oap_sing_agnal_k_t.dl_hybrid_agg_tokens_new subsequent_y_n Varchar(6) default "N";
update a 
from pp_oap_sing_agnal_k_t.dl_hybrid_agg_tokens_new 
set subsequent_y_n = "N"
where 
expr_name = 'Hybrid-Uncookied'
*/
-- Alter table  pp_oap_sing_agnal_k_t.dl_hybrid_agg_tokens_new add country as varchar (2);
-- update a 
-- from  pp_oap_sing_agnal_k_t.dl_hybrid_agg_tokens_new a
-- set country = 'US';


--DROP TABLE pp_oap_sing_agnal_k_t.dl_hybrid_agg_tokens_new;
--CREATE MULTISET TABLE pp_oap_sing_agnal_k_t.dl_hybrid_agg_tokens_new AS (
DELETE FROM pp_oap_sing_agnal_k_t.dl_hybrid_agg_tokens_new WHERE evnt_dt = (select rpt_dt - 1 from rpt_dt);
INSERT INTO pp_oap_sing_agnal_k_t.dl_hybrid_agg_tokens_new
SELECT
a.evnt_dt,
pxp_test_group,
--loginable,
is_cookied,
login_status,
CASE WHEN buyer_ip_country = 'United States' THEN 1 ELSE 0 end AS ip_US,
browser_type,
device_type,

official_conversion_y_n,
starts,
prepare_rvw_y_n,
dones,

-- regular
CASE WHEN email_shown = 1 OR pwd_shown = 1 OR hybrid_shown = 1 OR hybrid_pwd_shown = 1  THEN 1 ELSE 0 end AS page_shown,
CASE WHEN 	
(is_cookied = 'N' AND (email_shown = 1 OR email_submit = 1 OR pwd_shown = 1 OR pwd_submit = 1))
OR (is_cookied = 'Y' AND email_shown = 1)
THEN 1 ELSE 0 end AS email_shown,
CASE WHEN (is_cookied = 'N' AND (email_submit = 1 OR pwd_shown = 1 OR pwd_submit = 1))
OR(is_cookied = 'Y' AND email_submit = 1) THEN 1 ELSE 0 end AS email_submit,
email_success,
CASE WHEN pwd_shown = 1 OR pwd_submit = 1 THEN 1 ELSE 0 end AS pwd_shown,
pwd_submit,
pwd_success,
CASE WHEN (is_cookied = 'N' AND (hybrid_shown = 1 OR hybrid_submit = 1 OR hybrid_pwd_shown = 1 OR hybrid_pwd_submit = 1))
OR (is_cookied = 'Y' AND hybrid_shown = 1) THEN 1 ELSE 0 end AS hybrid_shown,
CASE WHEN (is_cookied = 'N' AND (hybrid_submit = 1 OR hybrid_pwd_shown = 1 OR hybrid_pwd_submit = 1))
OR (is_cookied = 'Y' AND hybrid_submit = 1) THEN 1 ELSE 0 end AS hybrid_submit,
hybrid_success,
CASE WHEN hybrid_pwd_shown = 1 OR hybrid_pwd_submit = 1 THEN 1 ELSE 0 end AS hybrid_pwd_shown,
hybrid_pwd_submit,
hybrid_pwd_success,
hybrid_pwd_change,

CASE WHEN (email_shown + email_submit + pwd_shown = 0 AND pwd_submit = 1 ) 
OR  (hybrid_shown + hybrid_submit + hybrid_pwd_shown = 0 AND hybrid_pwd_submit = 1 ) THEN 1 ELSE 0 end AS missing_FPTI,

STS_success, 
phone_login,
Signup,
Login_flag,
---hybrid pubcred
hybrid_email_submit_pub_cred,
hybrid_pwd_render_pub_cred,
hybrid_pwd_submit_pub_cred,

---regular pubcred
regular_email_submit_pub_cred,
regular_pwd_render_pub_cred,
regular_pwd_submit_pub_cred,

--last pubcred
last__pub_cred AS last_pub_cred,
login_error,
Count(DISTINCT a.context_id) AS count_tokens,
EXPER_NAME,
CASE WHEN EXPER_NAME = 'Hybrid-Uncookied'  THEN 'N'  ELSE subsequent_y_n END AS subsequent_y_n,
Country
                 
FROM pp_oap_Sing_shuqi_t.me_hybrid_pxp_login_tokens a


WHERE 1=1
AND a.evnt_dt = (select rpt_dt - 1 from rpt_dt)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,40,41,42
--) WITH DATA 
;

SEL 
evnt_dt,
Sum(starts) AS starts
FROM
pp_oap_sing_agnal_k_t.dl_hybrid_agg_tokens_new 
GROUP BY 1;