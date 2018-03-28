select a.row_id,
       a.x_max_amount,
       a.asset_num,
       p.name,
       a.x_deal_date,
       to_char(a.x_deal_date, 'yyyy-iw') deal_week,
       c.row_id,
       s.name,
       s.cust_trgt_type_cd,
       x.attrib_05,
       comm.created,
       comm_created.login,
       comm_created.cti_acd_userid,
       RANK() OVER(PARTITION BY a.row_id ORDER BY comm.created) login_rank
  from siebel.s_asset a
  join siebel.s_prod_int p
    on p.row_id = a.prod_id
   and p.detail_type_cd = 'Кредит'
  join siebel.s_contact c
    on c.row_id = a.x_fincorp_con_id
  join siebel.s_communication comm
    on comm.Pr_Con_Id = c.row_id
   and comm.created between add_months(a.x_deal_date, -3) and a.x_deal_date
  join siebel.s_user comm_created
    on comm_created.row_id = comm.created_by
   and comm_created.cti_acd_userid is not null
   and comm_created.login not in ('SADMIN', 'WS_FLINK', 'SADMIN_MARKET')
  join siebel.s_src s
    on s.row_id = comm.src_id
   and s.cust_trgt_type_cd = 'Cross-Sell'
  join siebel.s_src_x x
    on x.row_id = s.row_id
   and x.attrib_05 = 'Cross Sell PiL'
 where a.x_deal_date >= to_date('19/03/2018', 'dd/mm/yyyy')
   and a.x_deal_date < to_date('26/03/2018', 'dd/mm/yyyy')
