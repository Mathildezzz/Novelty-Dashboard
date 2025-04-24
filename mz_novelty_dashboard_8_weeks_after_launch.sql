delete from tutorial.mz_novelty_dashboard_8_weeks_after_launch;  -- for the subsequent update
insert into tutorial.mz_novelty_dashboard_8_weeks_after_launch

WITH calendar AS (
SELECT date_id,
       lego_year,
       lego_month,
       lego_week
  FROM edw.d_dl_calendar  trans
  WHERE trans.date_type = 'day'
),


product_cte AS (
SELECT lego_sku_id,
       lego_sku_name_cn,
       
        bu_cn_launch_date,
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



------------------------------------------------------
------------------------ benchmark

omni_benchmark AS (
SELECT periods.bu_cn_launch_date,
       periods.tracking_end_date,
       CAST(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND rk = 1 AND trans.bu_cn_launch_date <= join_date THEN trans.omni_channel_member_id ELSE NULL END) AS FLOAT)/ NULLIF(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE THEN trans.omni_channel_member_id ELSE NULL END),0)             AS pure_new_0_1_purchase_member_shopper_share,
       CAST((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = TRUE AND rk = 1 AND trans.bu_cn_launch_date > join_date then trans.omni_channel_member_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)),0)          AS existing_0_1_member_shopper_share,
       CAST(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND rk >= 2 THEN trans.omni_channel_member_id ELSE NULL END) AS FLOAT) / NULLIF(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE THEN trans.omni_channel_member_id ELSE NULL END),0)                                                    AS lifetime_repurchase_member_shopper_share
    
 FROM (
     SELECT product_cte.lego_sku_id,
            product_cte.bu_cn_launch_date,
            product_cte.cn_lcs_launch_date,  -- 考虑lcs early street date
            product_cte.cn_tm_launch_date,
            product_cte.cn_dy_launch_date,  
            novelty_focus_sku.tracking_end_date
      FROM product_cte
     INNER JOIN tutorial.mz_novelty_focus_sku_list novelty_focus_sku
             ON product_cte.lego_sku_id = novelty_focus_sku.lego_sku_id
         ) periods
LEFT JOIN (SELECT omni_trans_fact.*, purchase_order_rk.rk AS rk
              FROM omni_trans_fact 
              LEFT JOIN (SELECT * FROM purchase_order_rk WHERE source_channel = 'OMNI') purchase_order_rk
                   ON omni_trans_fact.parent_order_id = purchase_order_rk.parent_order_id
          ) trans
       ON trans.order_paid_date BETWEEN periods.bu_cn_launch_date AND periods.tracking_end_date
GROUP BY 1,2
),


lcs_benchmark AS (
SELECT periods.cn_lcs_launch_date,
       periods.tracking_end_date,
       CAST(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND rk = 1 AND trans.bu_cn_launch_date <= join_date THEN trans.omni_channel_member_id ELSE NULL END) AS FLOAT)/ NULLIF(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE THEN trans.omni_channel_member_id ELSE NULL END),0)             AS pure_new_0_1_purchase_member_shopper_share,
       CAST((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = TRUE AND rk = 1 AND trans.bu_cn_launch_date > join_date then trans.omni_channel_member_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)),0)          AS existing_0_1_member_shopper_share,
       CAST(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND rk >= 2 THEN trans.omni_channel_member_id ELSE NULL END) AS FLOAT) / NULLIF(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE THEN trans.omni_channel_member_id ELSE NULL END),0)                                                    AS lifetime_repurchase_member_shopper_share
    
 FROM (
     SELECT product.lego_sku_id,
            product.bu_cn_launch_date,
            product.cn_lcs_launch_date,
            product.cn_tm_launch_date,
            product.bu_cn_launch_date AS cn_dy_launch_date,   -- douyin_family_launch_date, douyin_brand_launch_date 很多空值 
            novelty_focus_sku.tracking_end_date
      FROM product_cte product
     INNER JOIN tutorial.mz_novelty_focus_sku_list novelty_focus_sku
             ON product.lego_sku_id = novelty_focus_sku.lego_sku_id
         ) periods
LEFT JOIN (SELECT omni_trans_fact.*, purchase_order_rk.rk AS rk
              FROM omni_trans_fact 
              LEFT JOIN (SELECT * FROM purchase_order_rk WHERE source_channel = 'LCS') purchase_order_rk
                   ON omni_trans_fact.parent_order_id = purchase_order_rk.parent_order_id
             WHERE omni_trans_fact.source_channel = 'LCS'
          ) trans
       ON trans.order_paid_date BETWEEN periods.cn_lcs_launch_date AND periods.tracking_end_date
GROUP BY 1,2
),


tmall_benchmark AS (
SELECT periods.cn_tm_launch_date,
       periods.tracking_end_date,
       CAST(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND rk = 1 AND trans.cn_tm_launch_date <= join_date THEN trans.omni_channel_member_id ELSE NULL END) AS FLOAT)/ NULLIF(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE THEN trans.omni_channel_member_id ELSE NULL END),0)             AS pure_new_0_1_purchase_member_shopper_share,
       CAST((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = TRUE AND rk = 1 AND trans.cn_tm_launch_date > join_date then trans.omni_channel_member_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)),0)          AS existing_0_1_member_shopper_share,
       CAST(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND rk >= 2 THEN trans.omni_channel_member_id ELSE NULL END) AS FLOAT) / NULLIF(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE THEN trans.omni_channel_member_id ELSE NULL END),0)                                                    AS lifetime_repurchase_member_shopper_share
FROM (
     SELECT product.lego_sku_id,
            product.bu_cn_launch_date,
            product.cn_lcs_launch_date,
            product.cn_tm_launch_date,
            product.bu_cn_launch_date AS cn_dy_launch_date,   -- douyin_family_launch_date, douyin_brand_launch_date 很多空值 
            novelty_focus_sku.tracking_end_date
      FROM product_cte product
     INNER JOIN tutorial.mz_novelty_focus_sku_list novelty_focus_sku
             ON product.lego_sku_id = novelty_focus_sku.lego_sku_id
         ) periods
LEFT JOIN (SELECT omni_trans_fact.*, purchase_order_rk.rk AS rk
              FROM omni_trans_fact 
              LEFT JOIN (SELECT * FROM purchase_order_rk WHERE source_channel = 'TMALL') purchase_order_rk
                   ON omni_trans_fact.parent_order_id = purchase_order_rk.parent_order_id
             WHERE omni_trans_fact.source_channel = 'TMALL'
          ) trans
       ON trans.order_paid_date BETWEEN periods.cn_tm_launch_date AND periods.tracking_end_date
GROUP BY 1,2
),


douyin_benchmark AS (
SELECT periods.cn_dy_launch_date,
       periods.tracking_end_date,
       CAST(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND rk = 1 AND trans.cn_dy_launch_date <= join_date THEN trans.omni_channel_member_id ELSE NULL END) AS FLOAT)/ NULLIF(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE THEN trans.omni_channel_member_id ELSE NULL END),0)             AS pure_new_0_1_purchase_member_shopper_share,
       CAST((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = TRUE AND rk = 1 AND trans.cn_dy_launch_date > join_date then trans.omni_channel_member_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)),0)          AS existing_0_1_member_shopper_share,
       CAST(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND rk >= 2 THEN trans.omni_channel_member_id ELSE NULL END) AS FLOAT) / NULLIF(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE THEN trans.omni_channel_member_id ELSE NULL END),0)                                                    AS lifetime_repurchase_member_shopper_share
  FROM (
     SELECT product.lego_sku_id,
            product.bu_cn_launch_date,
            product.cn_lcs_launch_date,
            product.cn_tm_launch_date,
            product.bu_cn_launch_date AS cn_dy_launch_date,   -- douyin_family_launch_date, douyin_brand_launch_date 很多空值 
            novelty_focus_sku.tracking_end_date
      FROM product_cte product
     INNER JOIN tutorial.mz_novelty_focus_sku_list novelty_focus_sku
             ON product.lego_sku_id = novelty_focus_sku.lego_sku_id
         ) periods
LEFT JOIN (SELECT omni_trans_fact.*, purchase_order_rk.rk AS rk
              FROM omni_trans_fact 
              LEFT JOIN (SELECT * FROM purchase_order_rk WHERE source_channel = 'DOUYIN') purchase_order_rk
                   ON omni_trans_fact.parent_order_id = purchase_order_rk.parent_order_id
            WHERE omni_trans_fact.source_channel = 'DOUYIN'
          ) trans
       ON trans.order_paid_date BETWEEN periods.cn_dy_launch_date AND periods.tracking_end_date
GROUP BY 1,2
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
  
         
         sum(sales_qty)                AS units_sold,
         CAST(COUNT(DISTINCT CASE WHEN UPT_table.pieces >=2 THEN trans.parent_order_id ELSE NULL END) AS FLOAT)/NULLIF(COUNT(DISTINCT trans.parent_order_id),0) AS multiple_units_per_order_penetration,
         
         ----------- member
         CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/(sum(case when sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end))           AS member_sales_share,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)  AS member_shopper,
          
          ----------- 首单 ------------
          CAST((count(distinct case when cn_lcs_launch_date <= join_date AND purchase_order_rk.rk = 1 AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)                                                                                                                                        AS pure_new_0_1_purchase_member_shopper,
          CAST((count(distinct case when cn_lcs_launch_date <= join_date AND purchase_order_rk.rk = 1 AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)/ NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)    AS pure_new_0_1_purchase_member_shopper_share,

      
         ----------- existing 0-1
          CAST((count(distinct case when cn_lcs_launch_date > join_date AND (purchase_order_rk.rk = 1) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)                                                                                                                                                AS existing_0_1_member_shopper,
          CAST((count(distinct case when cn_lcs_launch_date > join_date AND (purchase_order_rk.rk = 1) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)           AS existing_0_1_member_shopper_share,
    
         ----------- existing repurchase
         
         CAST((count(distinct case when (purchase_order_rk.rk >= 2) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)                                                                                                                                                  AS lifetime_repurchase_member_shopper,
         CAST((count(distinct case when (purchase_order_rk.rk >= 2) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)             AS lifetime_repurchase_member_shopper_share
from omni_trans_fact trans
LEFT JOIN (SELECT * FROM purchase_order_rk WHERE source_channel = 'LCS') purchase_order_rk
       ON trans.parent_order_id = purchase_order_rk.parent_order_id
LEFT JOIN UPT_table
       ON trans.parent_order_id = UPT_table.parent_order_id
INNER JOIN tutorial.mz_novelty_focus_sku_list novelty_focus_sku     -- 只看focus sku在 tracking period内的情况 （上市后八周)
       on trans.lego_sku_id = novelty_focus_sku.lego_sku_id
      AND trans.order_paid_date <= novelty_focus_sku.tracking_end_date
where 1 = 1
AND trans.source_channel = 'LCS'
AND trans.order_paid_date >= trans.cn_lcs_launch_date
GROUP BY 1,2,3,4,5,6,7,8,9,10
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
   
         
         sum(sales_qty)                AS units_sold,
         CAST(COUNT(DISTINCT CASE WHEN UPT_table.pieces >=2 THEN trans.parent_order_id ELSE NULL END) AS FLOAT)/NULLIF(COUNT(DISTINCT trans.parent_order_id),0) AS multiple_units_per_order_penetration,
         
         ----------- member
         CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/(sum(case when sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end))           AS member_sales_share,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)  AS member_shopper,
          
               ----------- 首单 ------------
          CAST((count(distinct case when tmall_bind_date IS NOT NULL AND cn_tm_launch_date <= tmall_bind_date AND purchase_order_rk.rk = 1 AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)                                                                                                                                        AS pure_new_0_1_purchase_member_shopper,
          CAST((count(distinct case when tmall_bind_date IS NOT NULL AND cn_tm_launch_date <= tmall_bind_date AND purchase_order_rk.rk = 1 AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)/ NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)    AS pure_new_0_1_purchase_member_shopper_share,

      
         ----------- existing 0-1
          CAST((count(distinct case when tmall_bind_date IS NOT NULL AND cn_tm_launch_date > tmall_bind_date AND (purchase_order_rk.rk = 1) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)                                                                                                                                                AS existing_0_1_member_shopper,
          CAST((count(distinct case when tmall_bind_date IS NOT NULL AND cn_tm_launch_date > tmall_bind_date AND (purchase_order_rk.rk = 1) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)           AS existing_0_1_member_shopper_share,
    
         ----------- existing repurchase
         
         CAST((count(distinct case when (purchase_order_rk.rk >= 2) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)                                                                                                                                                  AS lifetime_repurchase_member_shopper,
         CAST((count(distinct case when (purchase_order_rk.rk >= 2) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)             AS lifetime_repurchase_member_shopper_share
from omni_trans_fact trans
LEFT JOIN (SELECT * FROM purchase_order_rk WHERE source_channel = 'TMALL') purchase_order_rk
       ON trans.parent_order_id = purchase_order_rk.parent_order_id
LEFT JOIN UPT_table
       ON trans.parent_order_id = UPT_table.parent_order_id
INNER JOIN tutorial.mz_novelty_focus_sku_list novelty_focus_sku     -- 只看focus sku在 tracking period内的情况 （上市后八周)
       on trans.lego_sku_id = novelty_focus_sku.lego_sku_id
      AND trans.order_paid_date <= novelty_focus_sku.tracking_end_date
where 1 = 1
AND trans.source_channel = 'TMALL'
AND trans.order_paid_date >= trans.cn_tm_launch_date
GROUP BY 1,2,3,4,5,6,7,8,9,10
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
   
         
         sum(sales_qty)                AS units_sold,
         CAST(COUNT(DISTINCT CASE WHEN UPT_table.pieces >=2 THEN trans.parent_order_id ELSE NULL END) AS FLOAT)/NULLIF(COUNT(DISTINCT trans.parent_order_id),0) AS multiple_units_per_order_penetration,
         
         ----------- member
         CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/(sum(case when sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end))           AS member_sales_share,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)  AS member_shopper,
          
                      
         ----------- 首单 ------------
          CAST((count(distinct case when douyin_bind_date IS NOT NULL AND cn_tm_launch_date <= douyin_bind_date AND purchase_order_rk.rk = 1 AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)                                                                                                                                        AS pure_new_0_1_purchase_member_shopper,
          CAST((count(distinct case when douyin_bind_date IS NOT NULL AND cn_tm_launch_date <= douyin_bind_date AND purchase_order_rk.rk = 1 AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)/ NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)    AS pure_new_0_1_purchase_member_shopper_share,

      
         ----------- existing 0-1
          CAST((count(distinct case when douyin_bind_date IS NOT NULL AND cn_tm_launch_date > douyin_bind_date AND (purchase_order_rk.rk = 1) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)                                                                                                                                                AS existing_0_1_member_shopper,
          CAST((count(distinct case when douyin_bind_date IS NOT NULL AND cn_tm_launch_date > douyin_bind_date AND (purchase_order_rk.rk = 1) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)           AS existing_0_1_member_shopper_share,
    
         ----------- existing repurchase
         
         CAST((count(distinct case when (purchase_order_rk.rk >= 2) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)                                                                                                                                                  AS lifetime_repurchase_member_shopper,
         CAST((count(distinct case when (purchase_order_rk.rk >= 2) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)             AS lifetime_repurchase_member_shopper_share
from omni_trans_fact trans
LEFT JOIN (SELECT * FROM purchase_order_rk WHERE source_channel = 'DOUYIN') purchase_order_rk
       ON trans.parent_order_id = purchase_order_rk.parent_order_id
LEFT JOIN UPT_table
       ON trans.parent_order_id = UPT_table.parent_order_id
INNER JOIN tutorial.mz_novelty_focus_sku_list novelty_focus_sku     -- 只看focus sku在 tracking period内的情况 （上市后八周)
       on trans.lego_sku_id = novelty_focus_sku.lego_sku_id
      AND trans.order_paid_date <= novelty_focus_sku.tracking_end_date
where 1 = 1
AND trans.source_channel = 'DOUYIN'
AND trans.order_paid_date >= trans.cn_dy_launch_date
GROUP BY 1,2,3,4,5,6,7,8,9,10
UNION ALL
SELECT   'OMNI' AS source_channel,
         trans.lego_sku_id,
         
         trans.lego_sku_name_cn,
         trans.cn_line,
         trans.bu_cn_launch_date AS launch_date,
         
         novelty_focus_sku.tracking_end_date,
         trans.rsp,
         trans.product_rrp_price_range,
         trans.age_mark,
         trans.product_kids_vs_adult,
   
         
         sum(sales_qty)                AS units_sold,
         CAST(COUNT(DISTINCT CASE WHEN UPT_table.pieces >=2 THEN trans.parent_order_id ELSE NULL END) AS FLOAT)/NULLIF(COUNT(DISTINCT trans.parent_order_id),0) AS multiple_units_per_order_penetration,
         
         ----------- member
         CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/(sum(case when sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end))           AS member_sales_share,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)  AS member_shopper,
          
          ----------- 首单 ------------
          CAST((count(distinct case when bu_cn_launch_date <= join_date AND purchase_order_rk.rk = 1 AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)                                                                                                                                        AS pure_new_0_1_purchase_member_shopper,
          CAST((count(distinct case when bu_cn_launch_date <= join_date AND purchase_order_rk.rk = 1 AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)/ NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)    AS pure_new_0_1_purchase_member_shopper_share,

      
         ----------- existing 0-1
          CAST((count(distinct case when bu_cn_launch_date > join_date AND (purchase_order_rk.rk = 1) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)                                                                                                                                                AS existing_0_1_member_shopper,
          CAST((count(distinct case when bu_cn_launch_date > join_date AND (purchase_order_rk.rk = 1) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)           AS existing_0_1_member_shopper_share,
    
         ----------- existing repurchase
         
         CAST((count(distinct case when (purchase_order_rk.rk >= 2) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)                                                                                                                                                  AS lifetime_repurchase_member_shopper,
         CAST((count(distinct case when (purchase_order_rk.rk >= 2) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)             AS lifetime_repurchase_member_shopper_share
from omni_trans_fact trans
LEFT JOIN (SELECT * FROM purchase_order_rk WHERE source_channel = 'OMNI') purchase_order_rk
       ON trans.parent_order_id = purchase_order_rk.parent_order_id
LEFT JOIN UPT_table
       ON trans.parent_order_id = UPT_table.parent_order_id
INNER JOIN tutorial.mz_novelty_focus_sku_list novelty_focus_sku     -- 只看focus sku在 tracking period内的情况 （上市后八周)
       on trans.lego_sku_id = novelty_focus_sku.lego_sku_id
      AND trans.order_paid_date <= novelty_focus_sku.tracking_end_date
where 1 = 1
AND trans.order_paid_date >= trans.bu_cn_launch_date
GROUP BY 1,2,3,4,5,6,7,8,9,10
 )

SELECT 
     member_KPI_TY.source_channel,
     member_KPI_TY.launch_date,
     DATE(member_KPI_TY.tracking_end_date) AS tracking_end_date,
     member_KPI_TY.lego_sku_id,
     
     --- sku attribute
         member_KPI_TY.lego_sku_name_cn,
         member_KPI_TY.cn_line,
         member_KPI_TY.rsp,
         member_KPI_TY.product_rrp_price_range,
         member_KPI_TY.age_mark,
         member_KPI_TY.product_kids_vs_adult,

     
    --- metrics   
         member_KPI_TY.units_sold,
         member_KPI_TY.multiple_units_per_order_penetration,
 

    ------ crm metrics
         member_KPI_TY.member_sales_share,
         member_KPI_TY.member_shopper,
      
      
       ----------- 首单 ------------
      member_KPI_TY.pure_new_0_1_purchase_member_shopper,
      member_KPI_TY.pure_new_0_1_purchase_member_shopper_share,

  
     ----------- existing 0-1
      member_KPI_TY.existing_0_1_member_shopper,
      member_KPI_TY.existing_0_1_member_shopper_share,

     ----------- existing repurchase
     
     member_KPI_TY.lifetime_repurchase_member_shopper,
     member_KPI_TY.lifetime_repurchase_member_shopper_share,
     
     ---------------- benchmark --------------
      
      COALESCE(omni_benchmark.pure_new_0_1_purchase_member_shopper_share, lcs_benchmark.pure_new_0_1_purchase_member_shopper_share,tmall_benchmark.pure_new_0_1_purchase_member_shopper_share, douyin_benchmark.pure_new_0_1_purchase_member_shopper_share)                AS benchmark_pure_new_0_1_purchase_member_shopper_share,
      COALESCE(omni_benchmark.existing_0_1_member_shopper_share, lcs_benchmark.existing_0_1_member_shopper_share, tmall_benchmark.existing_0_1_member_shopper_share, douyin_benchmark.existing_0_1_member_shopper_share)                                                   AS benchmark_existing_0_1_member_shopper_share,
      COALESCE(omni_benchmark.lifetime_repurchase_member_shopper_share, lcs_benchmark.lifetime_repurchase_member_shopper_share, tmall_benchmark.lifetime_repurchase_member_shopper_share, douyin_benchmark.lifetime_repurchase_member_shopper_share)                       AS benchmark_lifetime_repurchase_member_shopper_share
         
FROM member_KPI_TY
LEFT JOIN omni_benchmark
       ON member_KPI_TY.launch_date = omni_benchmark.bu_cn_launch_date
      AND member_KPI_TY.tracking_end_date = omni_benchmark.tracking_end_date
      AND member_KPI_TY.source_channel = 'OMNI'
LEFT JOIN lcs_benchmark
       ON member_KPI_TY.launch_date = lcs_benchmark.cn_lcs_launch_date
      AND member_KPI_TY.tracking_end_date = lcs_benchmark.tracking_end_date
      AND member_KPI_TY.source_channel = 'LCS'
LEFT JOIN tmall_benchmark
       ON member_KPI_TY.launch_date = tmall_benchmark.cn_tm_launch_date
      AND member_KPI_TY.tracking_end_date = tmall_benchmark.tracking_end_date
      AND member_KPI_TY.source_channel = 'TMALL'
LEFT JOIN douyin_benchmark
       ON member_KPI_TY.launch_date = douyin_benchmark.cn_dy_launch_date
      AND member_KPI_TY.tracking_end_date = douyin_benchmark.tracking_end_date
      AND member_KPI_TY.source_channel = 'DOUYIN'
;
      