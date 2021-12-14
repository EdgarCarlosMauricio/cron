library(sqldf)
library(RJDBC)
library(DBI)
library(RPostgreSQL)
library(RODBC)
library(data.table)
library(stringr)
library(filesstrings)
library(odbc)
library(dplyr)

# Sys.setenv(TZ = "EST")
# Sys.setlocale("LC_ALL", 'es_CO.iso88591')

#---Alert: fecha_ultimo_documento, estadoCuenta, fechaAviso, fechaRad, Obj
t_ini <- Sys.time()
t_ini

setwd("/var/www/html/api_R/t/")
source("t_config.R")

#1. PgSql
pg = dbDriver("PostgreSQL")
con = dbConnect(pg, user=innpgusr, password=innpgpwd, host=innpghst, port=innpgprt, dbname=innpgdbn)

q_pg = paste0("SET search_path TO alert_sch; SELECT date_part('minute',now())*1000 AS pag, ASSS.a_siniestro, ASSS.nro_siniestro, ASSS.nro_cuenta FROM alert_sch.alert_soat_se_siniestro ASSS LEFT JOIN alert_sch.alert_soat_se_siniestro_bandeja ASSSB ON ASSS.nro_siniestro=ASSSB.nro_siniestro AND ASSS.a_siniestro=ASSSB.a_siniestro AND ASSS.nro_cuenta=ASSSB.nro_cuenta AND ASSSB.id_siniestro_bandeja=(SELECT max(ASSST.id_siniestro_bandeja) FROM alert_sch.alert_soat_se_siniestro_bandeja ASSST WHERE ASSST.nro_siniestro=ASSSB.nro_siniestro AND ASSST.a_siniestro=ASSSB.a_siniestro AND ASSST.nro_cuenta=ASSSB.nro_cuenta) WHERE ASSS.estado!=2 AND ASSS.estado!=3 AND ASSS.estado!=25 AND ASSS.nro_siniestro < 10000000 ORDER BY ASSSB.fecha_bandeja DESC, ASSSB.a_siniestro DESC, ASSSB.nro_siniestro DESC, ASSSB.nro_cuenta DESC LIMIT 1000 OFFSET date_part('minute',now())*1000;")
dataAlert = dbGetQuery(con,q_pg)  

print("Pg 1: OK")
print(q_pg)
print(nrow(dataAlert))
print(dataAlert$pag)
filter_asc = ""

for(i in 1:nrow(dataAlert)){
    filter_asc = paste0(filter_asc," OR (CU.nro_siniestro=",dataAlert$nro_siniestro[i]," AND CU.a_siniestro=",dataAlert$a_siniestro[i]," AND CU.nro_cuenta=",dataAlert$nro_cuenta[i],")")
}
#print(filter_asc)

#2. SqlSvr
if(nrow(dataAlert) > 0){
    drv <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver",innsslib) 
    uat_conn <- dbConnect(drv, innsscon, innssusr, innsspwd)
    rm(innsspwd)

    alert_data <- dbGetQuery(uat_conn,paste("use soat_gruposis; SELECT T2.nro_siniestro, T2.a_siniestro, T2.nro_cuenta, 0 AS id_estado, 0 AS id_estado_cuenta, T2.codigo_estado_cuenta AS estado_cuenta, '' AS estado_cuenta_nombre, T2.fecha_aviso_cliente, T2.fecha_radicacion, ISNULL(T2.fecha_mayor, T2.fecha_aviso_cliente) AS fecha_ultimo_documento, 0 AS id_objecion, ISNULL(T2.codigo_objecion,'') AS objecion, '' AS objecion_nombre, 0 AS id_tipo_objecion, '' AS tipo_objecion_nombre, 0 AS id_siniestro_bandeja, '1970-01-01' AS fecha_bandeja, 0 AS id_bandeja, '' AS bandeja_nombre, 0 AS id_tipo_bandeja, '' AS tipo_bandeja_nombre, ISNULL(T2.id_RA,0) AS id_ra, ISNULL(T2.fecha_egreso,'1970-01-01') AS fecha_pago, ISNULL(T2.estado,0) AS id_estado_pago, '' AS estado_pago_nombre FROM (SELECT T1.a_siniestro, T1.nro_siniestro, T1.nro_cuenta, T1.codigo_estado_cuenta, T1.codigo_objecion, CASE WHEN DATEDIFF(dd,T1.fecha_aviso_calc_corregida, T1.fecha_aviso) > 6 THEN T1.fecha_aviso ELSE T1.fecha_aviso_calc_corregida END AS fecha_aviso_cliente, T1.fecha_radicacion_cal_nf AS fecha_radicacion, T1.id_RA, T1.fecha_egreso, T1.estado, CASE WHEN (T1.dias_fecha_ultimo_documento >= 0 AND T1.dias_fecha_ultimo_documento IS NOT NULL AND T1.dias_fecha_ultimo_documento <= (CASE WHEN (T1.dias_fecha_respuesta IS NULL OR T1.dias_fecha_respuesta < 0 ) THEN T1.dias_fecha_ultimo_documento ELSE T1.dias_fecha_respuesta END) AND T1.dias_fecha_ultimo_documento <= (CASE WHEN (T1.dias_fecha_aviso_cal_ajustada IS NULL OR T1.dias_fecha_aviso_cal_ajustada < 0 ) THEN T1.dias_fecha_ultimo_documento ELSE T1.dias_fecha_aviso_cal_ajustada END)) THEN T1.fecha_ultimo_documento ELSE CASE WHEN (T1.dias_fecha_respuesta >= 0 AND T1.dias_fecha_respuesta IS NOT NULL AND T1.dias_fecha_respuesta <= (CASE WHEN (T1.dias_fecha_ultimo_documento IS NULL OR T1.dias_fecha_ultimo_documento < 0 ) THEN T1.dias_fecha_respuesta ELSE T1.dias_fecha_ultimo_documento END) AND T1.dias_fecha_respuesta <= (CASE WHEN (T1.dias_fecha_aviso_cal_ajustada IS NULL OR T1.dias_fecha_aviso_cal_ajustada < 0 ) THEN T1.dias_fecha_respuesta ELSE T1.dias_fecha_aviso_cal_ajustada END)) THEN T1.fecha_respuesta ELSE CASE WHEN (T1.dias_fecha_aviso_cal_ajustada >=0 AND T1.dias_fecha_aviso_cal_ajustada IS NOT NULL AND T1.dias_fecha_aviso_cal_ajustada <= (CASE WHEN (T1.dias_fecha_ultimo_documento IS NULL OR T1.dias_fecha_ultimo_documento < 0 ) THEN T1.dias_fecha_aviso_cal_ajustada ELSE T1.dias_fecha_ultimo_documento END) AND T1.dias_fecha_aviso_cal_ajustada <= (CASE WHEN (T1.dias_fecha_respuesta IS NULL OR T1.dias_fecha_respuesta < 0 ) THEN T1.dias_fecha_aviso_cal_ajustada ELSE T1.dias_fecha_respuesta END)) THEN T1.fecha_aviso_cal_ajustada END END END AS fecha_mayor FROM (SELECT T0.a_siniestro, T0.nro_siniestro, T0.nro_cuenta, T0.codigo_estado_cuenta, T0.codigo_objecion, T0.fecha_aviso, T0.fecha_respuesta, T0.id_RA, CASE WHEN (T0.fecha_radicacion_calc < T0.fecha_radicacion_nf OR T0.fecha_radicacion_nf IS NULL) THEN T0.fecha_radicacion_calc ELSE T0.fecha_radicacion_nf END AS fecha_radicacion_cal_nf, CASE WHEN convert(date,T0.fecha_aviso_calc) > convert(date,CASE WHEN (T0.fecha_radicacion_calc < T0.fecha_radicacion_nf OR T0.fecha_radicacion_nf IS NULL) THEN T0.fecha_radicacion_calc ELSE T0.fecha_radicacion_nf END) THEN T0.fecha_aviso ELSE T0.fecha_aviso_calc END AS fecha_aviso_calc_corregida, T0.fecha_ultimo_documento, T0.fecha_egreso, T0.estado, CASE WHEN DATEDIFF(dd,T0.fecha_aviso_calc, T0.fecha_aviso ) > 6 THEN DATEDIFF(dd, T0.fecha_aviso, T0.fecha_egreso) ELSE DATEDIFF(dd, T0.fecha_aviso_calc, T0.fecha_egreso) END AS dias_fecha_aviso_cal_ajustada, CASE WHEN DATEDIFF(dd,T0.fecha_aviso_calc, T0.fecha_aviso ) > 6 THEN T0.fecha_aviso ELSE T0.fecha_aviso_calc END AS fecha_aviso_cal_ajustada, T0.dias_fecha_ultimo_documento, T0.dias_fecha_respuesta FROM (SELECT DISTINCT CU.a_siniestro, CU.nro_siniestro, CU.nro_cuenta, CU.codigo_estado_cuenta, CU.codigo_objecion, CU.fecha_aviso, CU.fecha_respuesta, CASE WHEN (ISNULL(ISNULL(CU.fecha_aviso_cliente, RAmin.Fecha_Ultimo_Documento), CU.fecha_aviso) > CU.fecha_radicacion) THEN CU.fecha_aviso ELSE ISNULL(ISNULL(CU.fecha_aviso_cliente, RAmin.Fecha_Ultimo_Documento), CU.fecha_aviso) END AS fecha_aviso_calc, ISNULL(CUmig.fecha_radicacion, CU.fecha_radicacion) AS fecha_radicacion_calc, CUnf.fecha_radicacion AS fecha_radicacion_nf, RA.id_RA, RA.Fecha_Ultimo_Documento AS fecha_ultimo_documento, EG.fecha_egreso, EG.estado, DATEDIFF(dd,RA.Fecha_Ultimo_Documento, EG.fecha_egreso) AS dias_fecha_ultimo_documento, DATEDIFF(dd, CU.fecha_respuesta, EG.fecha_egreso) AS dias_fecha_respuesta FROM cuentas CU WITH (NOLOCK) INNER JOIN siniestros SI WITH (NOLOCK) ON CU.a_siniestro=SI.a_siniestro AND CU.nro_siniestro=SI.nro_siniestro LEFT JOIN Registro_Analisis RAmin WITH (NOLOCK) ON CU.a_siniestro=RAmin.a_siniestro AND CU.nro_siniestro=RAmin.nro_siniestro AND CU.nro_cuenta=RAmin.nro_cuenta AND RAmin.id_RA = (SELECT min(id_RA) FROM Registro_Analisis WHERE a_siniestro=RAmin.a_siniestro AND nro_siniestro=RAmin.nro_siniestro AND nro_cuenta=RAmin.nro_cuenta) LEFT JOIN cuentas CUmig WITH (NOLOCK) ON CU.nro_factura=CUmig.nro_factura AND CU.identificacion_reclamante=CUmig.identificacion_reclamante AND CUmig.codigo_objecion='OB27' AND CUmig.fecha_radicacion=(SELECT min(fecha_radicacion) FROM cuentas WHERE nro_factura=CUmig.nro_factura AND identificacion_reclamante=CUmig.identificacion_reclamante AND codigo_objecion='OB27') LEFT JOIN cuentas_NF CUnf WITH (NOLOCK) ON CU.nro_factura=CUnf.nro_factura AND CU.fecha_aviso=CUnf.fecha_aviso AND CUnf.fecha_radicacion=(SELECT min(fecha_radicacion) FROM cuentas_NF WHERE nro_factura=CUnf.nro_factura AND fecha_aviso=CUnf.fecha_aviso) LEFT JOIN Registro_Analisis RA WITH (NOLOCK) ON CU.a_siniestro=RA.a_siniestro AND CU.nro_siniestro=RA.nro_siniestro AND CU.nro_cuenta=RA.nro_cuenta LEFT JOIN egresos EG WITH (NOLOCK) ON RA.nro_egreso=EG.nro_egreso WHERE ",substring(filter_asc, 4)," AND SI.estado_siniestro = 'A' AND ISNULL(EG.estado , 0) != 7) AS T0) AS T1) AS T2;"))

    dbDisconnect(uat_conn)
    # write.csv(ans_pag, "filename.csv")

    print("SqlSrv: OK")
    print(nrow(alert_data))

    #3. PgSql
    estado <- 1
    alert_data$estado <- estado
    alert_data$fecha_registro  <- Sys.time() 

    #Generate insert
    query_insert = "SET search_path TO alert_sch; "
    for(i in 1:nrow(alert_data)){
        query_insert = paste0(query_insert,"INSERT INTO alert_sch.alert_soat_se_siniestro_vw VALUES(",alert_data$nro_siniestro[i],",",alert_data$a_siniestro[i],",",alert_data$nro_cuenta[i],",0,0,'",alert_data$estado_cuenta[i],"','','",alert_data$fecha_aviso_cliente[i],"','",alert_data$fecha_radicacion[i],"','",alert_data$fecha_ultimo_documento[i],"',CURRENT_TIMESTAMP,1,0,'",alert_data$objecion[i],"','',0,'',0,'2020-07-05',0,'',1,'',",alert_data$id_ra[i],",'",alert_data$fecha_pago[i],"',",alert_data$id_estado_pago[i],",'');")
    }
    #print(query_insert)

    dtab = dbGetQuery(con,query_insert)
    dbDisconnect(con)

    print("Pg 2: OK")
}

t_fin <- Sys.time()
t_fin
t_run <- t_fin - t_ini
t_run
