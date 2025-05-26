delete from tutorial.mz_novelty_dashboard;  -- for the subsequent update
insert into tutorial.mz_novelty_dashboard

WITH calendar AS (
SELECT date_id,
       lego_year,
       lego_month,
       lego_week
  FROM edw.d_dl_calendar  trans
  WHERE trans.date_type = 'day'
),


product_cte AS (
SELECT *,
      CASE WHEN cn_lcs_launch_date <= cn_tm_launch_date AND cn_lcs_launch_date <= cn_dy_launch_date THEN cn_lcs_launch_date  -- 考虑early street in lcs
           WHEN cn_tm_launch_date <= cn_lcs_launch_date AND cn_tm_launch_date <= cn_dy_launch_date THEN cn_tm_launch_date
           WHEN cn_dy_launch_date <= cn_lcs_launch_date AND cn_dy_launch_date <= cn_tm_launch_date THEN cn_dy_launch_date
      END AS bu_cn_launch_date
FROM (
SELECT lego_sku_id,
       lego_sku_name_cn,
        
        CASE WHEN cn_lcs_on_street_date IS NOT NULL AND cn_lcs_launch_date >= cn_lcs_on_street_date THEN cn_lcs_on_street_date 
             ELSE cn_lcs_launch_date 
        END AS cn_lcs_launch_date,
        cn_tm_launch_date,
         CASE WHEN licensed_douyin_brand_launch_date IS NOT NULL AND licensed_douyin_brand_launch_date <= licensed_douyin_family_launch_date THEN licensed_douyin_brand_launch_date
             WHEN licensed_douyin_family_launch_date IS NOT NULL AND licensed_douyin_brand_launch_date >= licensed_douyin_family_launch_date THEN licensed_douyin_family_launch_date
             ELSE COALESCE(licensed_douyin_brand_launch_date, licensed_douyin_family_launch_date)
        END  AS cn_dy_launch_date,  
        cn_line,
        rsp,
        age_mark,
        COALESCE(
                      -- Convert fraction '1 1/2' to a decimal value like '1.5'
                      CASE 
                          WHEN TRIM(age_mark) ~ '^[0-9]+ [0-9]+/[0-9]+' THEN
                              CAST(SPLIT_PART(TRIM(age_mark), ' ', 1) AS INT) + 
                              CAST(SPLIT_PART(SPLIT_PART(TRIM(age_mark), ' ', 2), '/', 1) AS INT) / 
                              CAST(SPLIT_PART(SPLIT_PART(TRIM(age_mark), ' ', 2), '/', 2) AS INT)
                          -- Remove non-numeric characters after leading digits (like '+', '-' etc.)
                          ELSE CAST(REGEXP_REPLACE(TRIM(age_mark), '[^0-9]+.*', '') AS INT)
                      END, 
                      0)                           AS product_min_age_mark,
        CASE WHEN rsp >= 0 AND rsp < 300 THEN 'LPP'
            WHEN rsp >= 300 AND rsp < 800 THEN 'MPP'
            WHEN rsp >= 800 THEN 'HPP'
         END                                        AS product_rrp_price_range             
      FROM edw.d_dl_product_info_latest
      WHERE TRIM(age_mark) ~ '^[0-9]+([ ]*[0-9]*/[0-9]+)?'   -- Filter out rows that don't start with a number or a fraction like '1 1/2'
      )
),

tmall_bind_date AS (
  select
        DISTINCT
        platform_id_value AS kyid,
        first_bind_time
    from edw.d_ec_b2c_member_shopper_detail_latest
    where 1 = 1
    and platform_id_type = 'kyid' -- platform_id_type: opendi / kyid
    and platformid = 'taobao' -- platformid: douyin / taobao
  ),
  
  douyin_bind_date AS (
  select
        DISTINCT
        platform_id_value AS openid,
        first_bind_time
    from edw.d_ec_b2c_member_shopper_detail_latest
    where 1 = 1
    and platform_id_type = 'openId' -- platform_id_type: opendi / kyid
    and platformid = 'douyin' -- platformid: douyin / taobao
  ),


omni_trans_fact as
    ( 
    SELECT
        CASE WHEN source_channel IN ( 'DOUYIN', 'DOUYIN_B2B') THEN 'DOUYIN' ELSE source_channel END AS source_channel,
        ----------------
        order_paid_time,
        date(tr.order_paid_date) as order_paid_date,
        calendar.lego_year,
        calendar.lego_month,
        calendar.lego_week,
        ----------------------------------
        -- kyid,
        case
        when tr.type_name in ('CRM_memberid', 'DY_openid', 'TMALL_kyid') then coalesce(cast(tr.crm_member_detail_id as varchar), cast(tr.type_value as varchar))
        else null end as omni_channel_member_id, -- 优先取member_detail_id，缺失情况下再取渠道内部id
        -- cast(mbr.id as varchar)       AS member_detail_id,
        mbr.member_detail_id,
        DATE(join_time)                        AS join_date,
        DATE(tmall_bind_date.first_bind_time)  AS tmall_bind_date,
        DATE(douyin_bind_date.first_bind_time) AS douyin_bind_date,
        eff_reg_channel,
        
        tr.parent_order_id,
        ----------------------------------------
        tr.lego_sku_id,
        product.lego_sku_name_cn,
        
        product.bu_cn_launch_date,
        product.cn_lcs_launch_date,
        product.cn_tm_launch_date,
        product.cn_dy_launch_date,
        
        product.cn_line,
        product.rsp,
        product.age_mark,
        CASE WHEN product.product_min_age_mark >= 13 THEN 'ADULT' ELSE 'KIDS' END AS product_kids_vs_adult,
        product.product_rrp_price_range,
        -----------------------------
      
       CASE WHEN tr.city_tier IS NULL THEN 'unspecified' ELSE tr.city_tier END                     AS city_tier,
       CASE WHEN ps.city_maturity_type IS NULL THEN '4_unspecified' ELSE ps.city_maturity_type END AS city_maturity_type,
        --------------------------
        tr.sales_qty, -- 用于为LCS判断正负单
        tr.if_eff_order_tag, -- 该字段仅对LCS有true / false之分，对于其余渠道均为true
        tr.is_member_order,
        tr.order_rrp_amt
    FROM edw.f_omni_channel_order_detail as tr
-- LEFT JOIN edw.f_crm_member_detail as mbr
--       on cast(tr.crm_member_detail_id as varchar) = cast(mbr.member_id as varchar)
LEFT JOIN edw.d_member_detail mbr
       on cast(tr.crm_member_detail_id AS integer) = cast(mbr.member_detail_id as integer)
LEFT JOIN tmall_bind_date
       ON cast(tr.type_value as varchar) = tmall_bind_date.kyid
LEFT JOIN douyin_bind_date
       ON cast(tr.type_value as varchar) = douyin_bind_date.openid
LEFT JOIN product_cte product
       ON tr.lego_sku_id = product.lego_sku_id
LEFT JOIN calendar
       ON tr.order_paid_date = calendar.date_id
LEFT JOIN (
                SELECT DISTINCT store.city_cn,city_maturity.city_type AS city_maturity_type
                FROM  edw.d_dl_phy_store store
                LEFT JOIN tutorial.mkt_city_type_roy_v1 city_maturity  
                      ON city_maturity.city_chn = store.city_cn
              ) as ps 
   ON tr.city_cn = ps.city_cn
    WHERE 1 = 1
      and source_channel in ('LCS','TMALL', 'DOUYIN', 'DOUYIN_B2B')
      and date(tr.order_paid_date) < current_date
      and ((tr.source_channel = 'LCS' and sales_type <> 3) or (tr.source_channel in ('TMALL', 'DOUYIN', 'DOUYIN_B2B') and tr.order_type = 'normal')) -- specific filtering for LCS, TM and DY
    ),
 
 ------------------- lifetime purchase ranking -----------------------------
 purchase_order_rk AS (
 SELECT *, 
       ROW_NUMBER () OVER (PARTITION BY omni_channel_member_id ORDER BY order_paid_time ASC) AS rk
  FROM
     (
          SELECT DISTINCT source_channel,parent_order_id, MIN(order_paid_time) AS order_paid_time, omni_channel_member_id
         FROM omni_trans_fact
        WHERE if_eff_order_tag = TRUE
          AND is_member_order = TRUE
          AND source_channel = 'LCS'
     GROUP BY 1,2,4
     )
UNION ALL
 SELECT *, 
       ROW_NUMBER () OVER (PARTITION BY omni_channel_member_id ORDER BY order_paid_time ASC) AS rk
  FROM
     (
          SELECT DISTINCT source_channel,parent_order_id, MIN(order_paid_time) AS order_paid_time, omni_channel_member_id
         FROM omni_trans_fact
        WHERE if_eff_order_tag = TRUE
          AND is_member_order = TRUE
          AND source_channel = 'TMALL'
     GROUP BY 1,2,4
     )
UNION ALL
 SELECT *, 
       ROW_NUMBER () OVER (PARTITION BY omni_channel_member_id ORDER BY order_paid_time ASC) AS rk
  FROM
     (
          SELECT DISTINCT source_channel, parent_order_id, MIN(order_paid_time) AS order_paid_time, omni_channel_member_id
         FROM omni_trans_fact
        WHERE if_eff_order_tag = TRUE
          AND is_member_order = TRUE
          AND source_channel = 'DOUYIN'
     GROUP BY 1,2,4
     )
UNION ALL
 SELECT *, 
       ROW_NUMBER () OVER (PARTITION BY omni_channel_member_id ORDER BY order_paid_time ASC) AS rk
  FROM
     (
          SELECT DISTINCT 'OMNI'source_channel, parent_order_id, MIN(order_paid_time) AS order_paid_time, omni_channel_member_id
         FROM omni_trans_fact
        WHERE if_eff_order_tag = TRUE
          AND is_member_order = TRUE
     GROUP BY 1,2,4
     )
 ),

---------------------------------------------------------------------------------------------------------------


UPT_table AS (
SELECT parent_order_id,
       SUM(sales_qty) AS pieces
  FROM omni_trans_fact
 WHERE is_member_order = TRUE
GROUP BY 1
),



-----------------------------
 member_KPI_TY AS (
  SELECT trans.source_channel,
         trans.lego_sku_id,
         
         trans.lego_sku_name_cn,
         trans.cn_line,
         trans.cn_lcs_launch_date            AS launch_date,
         novelty_focus_sku.tracking_end_date,
         trans.rsp,
         trans.product_rrp_price_range,
         trans.age_mark,
         trans.product_kids_vs_adult,
  
         
         trans.lego_year,
         trans.lego_week,
         
         sum(sales_qty)                AS units_sold,
         CAST(COUNT(DISTINCT CASE WHEN UPT_table.pieces >=2 THEN trans.parent_order_id ELSE NULL END) AS FLOAT)/NULLIF(COUNT(DISTINCT trans.parent_order_id),0) AS multiple_units_per_order_penetration,
         
         ----------- member ----------------
         CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/(sum(case when sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end))           AS member_sales_share,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)),0)  AS member_shopper,
          
          ----------- 首单 ------------
          CAST((count(distinct case when cn_lcs_launch_date <= join_date AND purchase_order_rk.rk = 1 AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)) AS FLOAT)                                                                                                                                        AS pure_new_0_1_purchase_member_shopper,
          CAST((count(distinct case when cn_lcs_launch_date <= join_date AND purchase_order_rk.rk = 1 AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)) AS FLOAT)/ NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)),0)    AS pure_new_0_1_purchase_member_shopper_share,

      
         ----------- existing 0-1
          CAST((count(distinct case when cn_lcs_launch_date > join_date AND (purchase_order_rk.rk = 1) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)) AS FLOAT)                                                                                                                                                AS existing_0_1_member_shopper,
          CAST((count(distinct case when cn_lcs_launch_date > join_date AND (purchase_order_rk.rk = 1) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)),0)           AS existing_0_1_member_shopper_share,
    
         ----------- existing repurchase
         
         CAST((count(distinct case when (purchase_order_rk.rk >= 2) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)) AS FLOAT)                                                                                                                                                  AS lifetime_repurchase_member_shopper,
         CAST((count(distinct case when (purchase_order_rk.rk >= 2) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)),0)             AS lifetime_repurchase_member_shopper_share
  
     FROM omni_trans_fact trans
LEFT JOIN (SELECT * FROM purchase_order_rk WHERE source_channel = 'LCS') purchase_order_rk
       ON trans.parent_order_id = purchase_order_rk.parent_order_id
LEFT JOIN UPT_table
       ON trans.parent_order_id = UPT_table.parent_order_id
INNER JOIN tutorial.mz_novelty_focus_sku_list novelty_focus_sku     -- 只看focus sku在 tracking period内的情况 （上市后八周)
       on trans.lego_sku_id = novelty_focus_sku.lego_sku_id
      AND trans.order_paid_date <= novelty_focus_sku.tracking_end_date
where 1 = 1
AND trans.source_channel = 'LCS'
AND order_paid_date >= cn_lcs_launch_date
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
UNION ALL
SELECT trans.source_channel,
         trans.lego_sku_id,
         
         trans.lego_sku_name_cn,
         trans.cn_line,
         trans.cn_tm_launch_date   AS launch_date,
         novelty_focus_sku.tracking_end_date,
         trans.rsp,
         trans.product_rrp_price_range,
         trans.age_mark,
         trans.product_kids_vs_adult,
   
         trans.lego_year,
         trans.lego_week,
         
         sum(sales_qty)                AS units_sold,
         CAST(COUNT(DISTINCT CASE WHEN UPT_table.pieces >=2 THEN trans.parent_order_id ELSE NULL END) AS FLOAT)/NULLIF(COUNT(DISTINCT trans.parent_order_id),0) AS multiple_units_per_order_penetration,
         
         ----------- member
         CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/(sum(case when sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end))           AS member_sales_share,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)),0)  AS member_shopper,
          
          
         ----------- 首单 ------------
          CAST((count(distinct case when tmall_bind_date IS NOT NULL AND cn_tm_launch_date <= tmall_bind_date AND purchase_order_rk.rk = 1 AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)) AS FLOAT)                                                                                                                                        AS pure_new_0_1_purchase_member_shopper,
          CAST((count(distinct case when tmall_bind_date IS NOT NULL AND cn_tm_launch_date <= tmall_bind_date AND purchase_order_rk.rk = 1 AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)) AS FLOAT)/ NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)),0)    AS pure_new_0_1_purchase_member_shopper_share,

      
         ----------- existing 0-1
          CAST((count(distinct case when tmall_bind_date IS NOT NULL AND cn_tm_launch_date > tmall_bind_date AND (purchase_order_rk.rk = 1) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)) AS FLOAT)                                                                                                                                                AS existing_0_1_member_shopper,
          CAST((count(distinct case when tmall_bind_date IS NOT NULL AND cn_tm_launch_date > tmall_bind_date AND (purchase_order_rk.rk = 1) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)),0)           AS existing_0_1_member_shopper_share,
    
         ----------- existing repurchase
         
         CAST((count(distinct case when (purchase_order_rk.rk >= 2) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)) AS FLOAT)                                                                                                                                                  AS lifetime_repurchase_member_shopper,
         CAST((count(distinct case when (purchase_order_rk.rk >= 2) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)),0)             AS lifetime_repurchase_member_shopper_share
  
    FROM omni_trans_fact trans
LEFT JOIN (SELECT * FROM purchase_order_rk WHERE source_channel = 'TMALL') purchase_order_rk
       ON trans.parent_order_id = purchase_order_rk.parent_order_id
LEFT JOIN UPT_table
       ON trans.parent_order_id = UPT_table.parent_order_id
INNER JOIN tutorial.mz_novelty_focus_sku_list novelty_focus_sku     -- 只看focus sku在 tracking period内的情况 （上市后八周)
       on trans.lego_sku_id = novelty_focus_sku.lego_sku_id
      AND trans.order_paid_date <= novelty_focus_sku.tracking_end_date
where 1 = 1
AND trans.source_channel = 'TMALL'
AND order_paid_date >= cn_tm_launch_date
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
UNION ALL
SELECT trans.source_channel,
         trans.lego_sku_id,
         
         trans.lego_sku_name_cn,
         trans.cn_line,
         trans.cn_dy_launch_date              AS launch_date,
         novelty_focus_sku.tracking_end_date,
         trans.rsp,
         trans.product_rrp_price_range,
         trans.age_mark,
         trans.product_kids_vs_adult,
   
         trans.lego_year,
         trans.lego_week,
         
         sum(sales_qty)                AS units_sold,
         CAST(COUNT(DISTINCT CASE WHEN UPT_table.pieces >=2 THEN trans.parent_order_id ELSE NULL END) AS FLOAT)/NULLIF(COUNT(DISTINCT trans.parent_order_id),0) AS multiple_units_per_order_penetration,
         
         ----------- member
         CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/(sum(case when sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end))           AS member_sales_share,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)),0)  AS member_shopper,
          
          
                    
         ----------- 首单 ------------
          CAST((count(distinct case when douyin_bind_date IS NOT NULL AND cn_tm_launch_date <= douyin_bind_date AND purchase_order_rk.rk = 1 AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)) AS FLOAT)                                                                                                                                        AS pure_new_0_1_purchase_member_shopper,
          CAST((count(distinct case when douyin_bind_date IS NOT NULL AND cn_tm_launch_date <= douyin_bind_date AND purchase_order_rk.rk = 1 AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)) AS FLOAT)/ NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)),0)    AS pure_new_0_1_purchase_member_shopper_share,

      
         ----------- existing 0-1
          CAST((count(distinct case when douyin_bind_date IS NOT NULL AND cn_tm_launch_date > douyin_bind_date AND (purchase_order_rk.rk = 1) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)) AS FLOAT)                                                                                                                                                AS existing_0_1_member_shopper,
          CAST((count(distinct case when douyin_bind_date IS NOT NULL AND cn_tm_launch_date > douyin_bind_date AND (purchase_order_rk.rk = 1) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)),0)           AS existing_0_1_member_shopper_share,
    
         ----------- existing repurchase
         
         CAST((count(distinct case when (purchase_order_rk.rk >= 2) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)) AS FLOAT)                                                                                                                                                  AS lifetime_repurchase_member_shopper,
         CAST((count(distinct case when (purchase_order_rk.rk >= 2) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)),0)             AS lifetime_repurchase_member_shopper_share
  
     FROM omni_trans_fact trans
LEFT JOIN (SELECT * FROM purchase_order_rk WHERE source_channel = 'DOUYIN') purchase_order_rk
       ON trans.parent_order_id = purchase_order_rk.parent_order_id
LEFT JOIN UPT_table
       ON trans.parent_order_id = UPT_table.parent_order_id
INNER JOIN tutorial.mz_novelty_focus_sku_list novelty_focus_sku     -- 只看focus sku在 tracking period内的情况 （上市后八周)
       on trans.lego_sku_id = novelty_focus_sku.lego_sku_id
      AND trans.order_paid_date <= novelty_focus_sku.tracking_end_date
where 1 = 1
AND trans.source_channel = 'DOUYIN'
AND order_paid_date >= cn_dy_launch_date
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
UNION ALL
SELECT   'OMNI' AS source_channel,
         trans.lego_sku_id,
         
         trans.lego_sku_name_cn,
         trans.cn_line,
         bu_cn_launch_date AS launch_date,
         
         novelty_focus_sku.tracking_end_date,
         trans.rsp,
         trans.product_rrp_price_range,
         trans.age_mark,
         trans.product_kids_vs_adult,
   
         trans.lego_year,
         trans.lego_week,
         
         sum(sales_qty)                AS units_sold,
         CAST(COUNT(DISTINCT CASE WHEN UPT_table.pieces >=2 THEN trans.parent_order_id ELSE NULL END) AS FLOAT)/NULLIF(COUNT(DISTINCT trans.parent_order_id),0) AS multiple_units_per_order_penetration,
         
         ----------- member
         CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/(sum(case when sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end))           AS member_sales_share,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)),0)  AS member_shopper,
          
          
           ----------- 首单 ------------
          CAST((count(distinct case when bu_cn_launch_date <= join_date AND purchase_order_rk.rk = 1 AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)) AS FLOAT)                                                                                                                                        AS pure_new_0_1_purchase_member_shopper,
          CAST((count(distinct case when bu_cn_launch_date <= join_date AND purchase_order_rk.rk = 1 AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)) AS FLOAT)/ NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)),0)    AS pure_new_0_1_purchase_member_shopper_share,

      
         ----------- existing 0-1
          CAST((count(distinct case when bu_cn_launch_date > join_date AND (purchase_order_rk.rk = 1) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)) AS FLOAT)                                                                                                                                                AS existing_0_1_member_shopper,
          CAST((count(distinct case when bu_cn_launch_date > join_date AND (purchase_order_rk.rk = 1) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)),0)           AS existing_0_1_member_shopper_share,
    
         ----------- existing repurchase
         
         CAST((count(distinct case when (purchase_order_rk.rk >= 2) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)) AS FLOAT)                                                                                                                                                  AS lifetime_repurchase_member_shopper,
         CAST((count(distinct case when (purchase_order_rk.rk >= 2) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)),0)             AS lifetime_repurchase_member_shopper_share
  
  
     FROM omni_trans_fact trans
LEFT JOIN (SELECT * FROM purchase_order_rk WHERE source_channel = 'OMNI') purchase_order_rk
       ON trans.parent_order_id = purchase_order_rk.parent_order_id
LEFT JOIN UPT_table
       ON trans.parent_order_id = UPT_table.parent_order_id
INNER JOIN tutorial.mz_novelty_focus_sku_list novelty_focus_sku     -- 只看focus sku在 tracking period内的情况 （上市后八周)
       on trans.lego_sku_id = novelty_focus_sku.lego_sku_id
      AND trans.order_paid_date <= novelty_focus_sku.tracking_end_date
where 1 = 1
AND order_paid_date >= bu_cn_launch_date
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
 )

SELECT 
     member_KPI_TY.source_channel,
     member_KPI_TY.launch_date,
     member_KPI_TY.lego_sku_id,
     
     --- sku attribute
         member_KPI_TY.lego_sku_name_cn,
         member_KPI_TY.cn_line,
         member_KPI_TY.rsp,
         member_KPI_TY.product_rrp_price_range,
         member_KPI_TY.age_mark,
         member_KPI_TY.product_kids_vs_adult,
         
    ---- calendar
         member_KPI_TY.lego_year,
         member_KPI_TY.lego_week,
     
    --- metrics   
         member_KPI_TY.units_sold,
         member_KPI_TY.multiple_units_per_order_penetration,
 
    ------ crm metrics
         member_KPI_TY.member_sales_share,
         member_KPI_TY.member_shopper,
    
    ----------- 首单 ------------
      pure_new_0_1_purchase_member_shopper,
      pure_new_0_1_purchase_member_shopper_share,

  
     ----------- existing 0-1
      existing_0_1_member_shopper,
      existing_0_1_member_shopper_share,

     ----------- existing repurchase
     
     lifetime_repurchase_member_shopper,
     lifetime_repurchase_member_shopper_share
FROM member_KPI_TY;