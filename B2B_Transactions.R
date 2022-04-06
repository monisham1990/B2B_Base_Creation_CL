source('~/Redshift-connexion.R')
library(lubridate)
library("mailR")
library(data.table)
currentDate <-Sys.Date()
x <- as.Date(currentDate,format="%Y-%m-%d")
x_month <-  format(x,"%m")
x_year <-  format(x,"%Y")
print(x_month)
print(x_year)
if (x_month == "01"){
end_last_month <- floor_date(x,"month")-1
}else{
end_last_month <- rollback(x)
}
print(end_last_month)
init_last_month <- rollback(end_last_month, roll_to_first = TRUE)
print(init_last_month)
sample_query <- sqlInterpolate(conn,"select the_transaction_id compras,the_date_transaction fecha_trn,a.transaction_link_id factura ,ctm_customer_id,dbu.but_num_business_unit tienda,
                f_to_tax_in monto,sku.sku_num_sku_r3,f_qty_item,hsp.unv_label,the_to_Type,'F'
                from cds.f_transaction_detail_current b
                left JOIN (select distinct transaction_id,transaction_link_id,max(rs_technical_date)
                           from ods_retail.psl_rtl_transaction_link psl where reason_code ='DKT:FiscalTransaction'
                           group by 1,2) a on a.transaction_id = b.the_Transaction_id
                INNER JOIN cds.d_business_unit dbu ON dbu.but_idr_business_unit = b.but_idr_business_unit
                inner join cds.d_sku sku on sku.sku_idr_sku =b.sku_idr_sku
                inner join cds_supply.d_hierarchy_supply hsp ON (hsp.mdl_num_model_r3 = sku.mdl_num_model_r3 AND hsp.org_fa =1)
                where trunc(the_date_transaction) between ?init and ?end
                and b.cnt_idr_country =39 and len(a.transaction_link_id) < 6 and the_to_Type='offline' and b.tdt_type_detail='sale'",init =dbQuoteString(conn,as.character(init_last_month)),end =dbQuoteString(conn,as.character(end_last_month)))
insert1 <- paste0("INSERT INTO dtm_cl.f_transaction_b2b_cl(",sample_query,")")
dbSendUpdate(conn,insert1)
sample_query1 <- sqlInterpolate(conn,"select the_transaction_id compras,the_date_transaction fecha_trn,a.transaction_link_id factura ,ctm_customer_id,dbu.but_num_business_unit tienda,
f_to_tax_in monto,sku.sku_num_sku_r3,f_qty_item,hsp.unv_label,the_to_Type,'F'
from cds.f_delivery_detail_current b
left JOIN (select distinct transaction_id,transaction_link_id,max(rs_technical_date)
 from ods_retail.psl_rtl_transaction_link psl where reason_code ='DKT:FiscalTransaction'
group by 1,2) a on a.transaction_id = b.the_Transaction_id
INNER JOIN cds.d_business_unit dbu ON dbu.but_idr_business_unit = b.but_idr_business_unit_economical
inner join cds.d_sku sku on sku.sku_idr_sku =b.sku_idr_sku
inner join cds_supply.d_hierarchy_supply hsp ON (hsp.mdl_num_model_r3 = sku.mdl_num_model_r3 AND hsp.org_fa =1)
where trunc(the_date_transaction) between ?init and ?end
and b.cnt_idr_country =39 and len(a.transaction_link_id) <6 and b.tdt_type_detail='sale'",init =dbQuoteString(conn,as.character(init_last_month)),end =dbQuoteString(conn,as.character(end_last_month)))
insert2 <- paste0("INSERT INTO dtm_cl.f_transaction_b2b_cl(",sample_query1,")")
dbSendUpdate(conn,insert2)
sample_query2 <- sqlInterpolate(conn,"select the_transaction_id compras,the_date_transaction fecha_trn,a.transaction_link_id factura ,ctm_customer_id,dbu.but_num_business_unit tienda,
f_to_tax_in monto,sku.sku_num_sku_r3,f_qty_item,hsp.unv_label,the_to_type,'B'
                                from cds.f_transaction_detail_current b
                                left JOIN (select distinct transaction_id,transaction_link_id,max(rs_technical_date)
                                from ods_retail.psl_rtl_transaction_link psl where reason_code ='DKT:FiscalTransaction'
                                group by 1,2) a on a.transaction_id = b.the_Transaction_id
                                INNER JOIN cds.d_business_unit dbu ON dbu.but_idr_business_unit = b.but_idr_business_unit
                                inner join cds.d_sku sku on sku.sku_idr_sku =b.sku_idr_sku
                                inner join cds_supply.d_hierarchy_supply hsp ON (hsp.mdl_num_model_r3 = sku.mdl_num_model_r3 AND hsp.org_fa =1)
                                where trunc(the_date_transaction) between ?init and ?end  and b.cnt_idr_country =39  and len(a.transaction_link_id) >= 6
                                and ctm_customer_id in (select loyalty_card_num from cds.d_customers where cnt_country_code_usual ='CL' and person_type =2 and loyalty_card_num <>'') and the_to_type ='offline' 
                                and b.tdt_type_detail='sale'",init =dbQuoteString(conn,as.character(init_last_month)),end =dbQuoteString(conn,as.character(end_last_month)))
insert3 <- paste0("INSERT INTO dtm_cl.f_transaction_b2b_cl(",sample_query2,")")
dbSendUpdate(conn,insert3)
sample_query3 <- sqlInterpolate(conn,"select the_transaction_id compras,the_date_transaction fecha_trn,a.transaction_link_id factura ,ctm_customer_id,dbu.but_num_business_unit tienda,
f_to_tax_in monto,sku.sku_num_sku_r3,f_qty_item,hsp.unv_label,'online' ,'B'
                                from cds.f_delivery_detail_current b
                                left JOIN (select distinct transaction_id,transaction_link_id,max(rs_technical_date)
                                from ods_retail.psl_rtl_transaction_link psl where reason_code ='DKT:FiscalTransaction'
                                group by 1,2) a on a.transaction_id = b.the_Transaction_id
                                INNER JOIN cds.d_business_unit dbu ON dbu.but_idr_business_unit = b.but_idr_business_unit_Economical
                                inner join cds.d_sku sku on sku.sku_idr_sku =b.sku_idr_sku
                                inner join cds_supply.d_hierarchy_supply hsp ON (hsp.mdl_num_model_r3 = sku.mdl_num_model_r3 AND hsp.org_fa =1)
                                where trunc(b.the_date_transaction) between ?init and ?end  and b.cnt_idr_country =39  and len(a.transaction_link_id) >= 6
                                and  ctm_customer_id in (select loyalty_card_num from cds.d_customers where cnt_country_code_usual ='CL' and person_type =2 and loyalty_card_num <>'')
                                and b.tdt_type_detail='sale'",init =dbQuoteString(conn,as.character(init_last_month)),end =dbQuoteString(conn,as.character(end_last_month)))
insert4 <- paste0("INSERT INTO dtm_cl.f_transaction_b2b_cl(",sample_query3,")")
dbSendUpdate(conn,insert4)
query1 <- sqlInterpolate(conn,"select count(distinct transaction_id) Total_Trn,sum(monto) Monto,count(distinct(case when loyalty_card ='' then (transaction_id) end)) trn_sin_club,  
count(distinct(case when loyalty_card <>'' then (transaction_id) end)) Trn_con_club
from dtm_cl.f_transaction_b2b_cl where trunc(fecha) between ?init and ?end and tipo_boleta_factura ='F'",init =dbQuoteString(conn,as.character(init_last_month)),end =dbQuoteString(conn,as.character(end_last_month)))
que1 <- dbSendQuery(conn,query1)
result1 <- dbFetch(que1)
query3 <- sqlInterpolate(conn,"select count(distinct loyalty_Card) Client_N from dtm_cl.f_transaction_b2b_cl where trunc(fecha) between ?init and ?end
and loyalty_Card  in (select distinct loyalty_Card from  dtm_cl.f_transaction_b2b_cl where trunc(fecha) < ?init and loyalty_Card!='')
and loyalty_Card!=''",init =dbQuoteString(conn,as.character(init_last_month)),end =dbQuoteString(conn,as.character(end_last_month)))
que3 <- dbSendQuery(conn,query3)
result3 <- dbFetch(que3)
query4 <- sqlInterpolate(conn,"select sum(monto) Total_mes from (select but_num_business_unit,sum(f_to_tax_in) monto from cds.f_transaction_detail_Current tdt
inner join cds.d_business_unit but on tdt.but_idr_business_unit = but.but_idr_business_unit
where tdt.cnt_idr_country =39 and tdt.tdt_date_to_ordered between ?init and ?end and the_to_type='offline' group by 1
union
select but_num_business_unit,sum(f_to_Tax_in) Monto FROM cds.f_delivery_detail_current dyd
inner join cds.d_business_unit but on dyd.but_idr_business_unit_economical = but.but_idr_business_unit
where dyd.tdt_date_to_ordered between ?init and ?end
and the_to_type = 'online' and dyd.cnt_idr_country =39
GROUP BY 1 )",init =dbQuoteString(conn,as.character(init_last_month)),end =dbQuoteString(conn,as.character(end_last_month)))
que4 <- dbSendQuery(conn,query4)
result4 <- dbFetch(que4)
result5 <- round((result1[,c("monto")]/result4[,c("total_mes")])*100,2)
title1 <-paste("Reporte de B2B Transacciones Mes ",format(init_last_month,"%m"),',',format(init_last_month,"%Y"), sep="")
smtp5 = list(host.name = Sys.getenv("SMTP5.HOSTNAME"),
             port = Sys.getenv("SMTP5.PORT"),
             user.name=Sys.getenv("SMTP5.USER_NAME"),
             passwd=Sys.getenv("SMTP5.PASSWD"),
             ssl=FALSE)

send_mail = mailR::send.mail(from =Sys.getenv("SMTP5.SENDER"),
                             to=c('monisha.murugesan@decathlon.com','franck.blumenfeld@decathlon.com','tomas.saavedra@decathlon.com','marioluca.broccolo@decathlon.com','mathieu.champion@decathlon.com'),
                             smtp = smtp5,
                             subject = title1,
                             body = paste0('Hola Tod@s,

El base de B2B para mes ',format(init_last_month,"%m"),',',format(init_last_month,"%Y"),' esta actualizada.El monto total de ventas B2B es $',result1[,c("monto")],',',result5,'% de Ventas pais.

Numero de B2B transacciones son ',result1[,c("total_trn")],', ',result1[,c("trn_sin_club")],' transacciones sin tarjeta Club y ',result1[,c("trn_con_club")],' transacciones con tarjeta Club.','

Cantidad de nuevo compradores este mes son:',result3,'.
                                           
Es un correo automatizado.Por favor no respondan.

Saludos'),
                             authenticate = TRUE,
                             send = TRUE)
dbDisconnect(conn)
