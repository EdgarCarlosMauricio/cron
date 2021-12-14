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

#---Alert para NF: fecha_ultimo_documento, estadoCuenta, fechaAviso, fechaRad, Obj
t_ini <- Sys.time()
t_ini

setwd("/var/www/html/api_R/t/")
source("t_config.R")

#1. PgSql
pg = dbDriver("PostgreSQL")
con = dbConnect(pg, user=innpgusr, password=innpgpwd, host=innpghst, port=innpgprt, dbname=innpgdbn)

#q_pg = paste0("SET search_path TO alert_sch; SELECT date_part('minute',now())*2000 AS pag, ASSS.a_siniestro, (ASSS.nro_siniestro-10000000) AS nro_siniestro, ASSS.nro_cuenta FROM alert_sch.alert_soat_se_siniestro ASSS WHERE estado!=2 AND estado!=3 AND nro_siniestro > 9900000 LIMIT 2000 OFFSET date_part('minute',now())*2000;")
q_pg = paste0("SET search_path TO alert_sch; SELECT date_part('minute',now())*1000 AS pag, ASSS.a_siniestro, (ASSS.nro_siniestro-10000000) AS nro_siniestro, ASSS.nro_cuenta FROM alert_sch.alert_soat_se_siniestro ASSS WHERE estado!=2 AND estado!=3 AND nro_siniestro > 9900000 ORDER BY ASSS.a_siniestro DESC, ASSS.nro_siniestro DESC LIMIT 2000 OFFSET date_part('minute',now())*2000;")
dataAlert = dbGetQuery(con,q_pg)  

print("Pg 1: OK")
print(nrow(dataAlert))
filter_asc = ""

for(i in 1:nrow(dataAlert)){
    filter_asc = paste0(filter_asc," OR (CU_NF.nro_siniestro=",dataAlert$nro_siniestro[i]," AND CU_NF.a_siniestro=",dataAlert$a_siniestro[i]," AND CU_NF.nro_cuenta=",dataAlert$nro_cuenta[i],")")
}

#2. SqlSvr
if(nrow(dataAlert) > 0){
    drv <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver",innsslib) 
    uat_conn <- dbConnect(drv, innsscon, innssusr, innsspwd)
    rm(innsspwd)

    q_data = paste("use soat_gruposis; SELECT (10000000+CU_NF.nro_siniestro) AS nro_siniestro, CU_NF.a_siniestro, CU_NF.nro_cuenta, 0 AS id_estado, 0 AS id_estado_cuenta, CU_NF.codigo_estado_cuenta AS estado_cuenta, '' AS estado_cuenta_nombre, CU_NF.fecha_aviso AS fecha_aviso_cliente, CU_NF.fecha_radicacion, CU_NF.fecha_aviso AS fecha_ultimo_documento, 0 AS id_objecion, ISNULL(CU_NF.codigo_objecion,'') AS objecion, '' AS objecion_nombre, 0 AS id_tipo_objecion, '' AS tipo_objecion_nombre, 0 AS id_siniestro_bandeja, '1970-01-01' AS fecha_bandeja, 0 AS id_bandeja, '' AS bandeja_nombre, 0 AS id_tipo_bandeja, '' AS tipo_bandeja_nombre, 0 AS id_ra, ISNULL(CU_NF.fecha_egreso,'1970-01-01') AS fecha_pago, 0 AS id_estado_pago, '' AS estado_pago_nombre FROM cuentas_nf CU_NF WITH (NOLOCK) WHERE ",substring(filter_asc, 4),";")
    alert_data <- dbGetQuery(uat_conn,q_data)

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
    dtab = dbGetQuery(con,query_insert)
    dbDisconnect(con)

    print("Pg 2: OK")
}

t_fin <- Sys.time()
t_fin
t_run <- t_fin - t_ini
t_run
