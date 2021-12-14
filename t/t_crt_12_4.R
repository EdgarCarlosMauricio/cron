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

#---CONSULTA GENERAL BANDEJAS: desactivar ultima bandeja por siniestro-year-cuenta (s_a_c)
#----Caso-Bandeja: 8
t_ini <- Sys.time()
t_ini

setwd("/var/www/html/api_R/t/")
source("t_config.R")

#1. SqlSvr
drv <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver",innsslib) 
uat_conn <- dbConnect(drv, innsscon, innssusr, innsspwd)
rm(innsspwd)

alert_data <- dbGetQuery(uat_conn,paste("use vida_gruposis; SELECT NroSiniestro AS nro_siniestro, fecha_bandeja FROM GrupoSIS..Bandejas_General WHERE equipo='VIDA-APE' AND fecha_bandeja BETWEEN '2000-01-01' AND convert(date,dateadd(day,1, getdate())) AND [Nombre Bandeja]!='RealizaAuditoria' AND Rol!='Coordinador' AND codigo_amparo!=11 AND codigo_amparo!=12 AND [Nombre Bandeja]='AnalisisLiquidacion' AND [usuario que remite a bandeja]='conciliacionAPE';"))

alert_data_o <- dbGetQuery(uat_conn,paste("use vida_gruposis; SELECT NroSiniestro AS nro_siniestro, fecha_bandeja FROM GrupoSIS..Bandejas_General WHERE equipo='VIDA-APE' AND fecha_bandeja BETWEEN '2000-01-01' AND convert(date,dateadd(day,1, getdate())) AND [Nombre Bandeja]!='RealizaAuditoria' AND Rol!='Coordinador' AND codigo_amparo!=11 AND codigo_amparo!=12 AND [Nombre Bandeja]='ActualizacionRespuestasCartas' AND [usuario que remite a bandeja]='conciliacionAPE';"))
dbDisconnect(uat_conn)

#Split: s_a_c
s_a_c_tmp <- strsplit(as.character(alert_data$nro_siniestro), "/|[*]")

alert_data$nro_siniestro <- sapply(s_a_c_tmp, "[[" , 1)
alert_data$a_siniestro <- sapply(s_a_c_tmp, "[[", 2)
alert_data$nro_cuenta <- sapply(s_a_c_tmp, "[[", 3)
print(nrow(alert_data))

s_a_c_tmp_o <- strsplit(as.character(alert_data_o$nro_siniestro), "/|[*]")

alert_data_o$nro_siniestro <- sapply(s_a_c_tmp_o, "[[" , 1)
alert_data_o$a_siniestro <- sapply(s_a_c_tmp_o, "[[", 2)
alert_data_o$nro_cuenta <- sapply(s_a_c_tmp_o, "[[", 3)
print(nrow(alert_data_o))

print("SqlSvr: OK")

#2. PgSql
pg = dbDriver("PostgreSQL")
con = dbConnect(pg, user=innpgusr, password=innpgpwd, host=innpghst, port=innpgprt, dbname=innpgdbn)

#Generate Upd
id_bandeja <- 8
fecha_registro  <- Sys.time() 
query_update_ini = paste0("SET search_path TO alert_sch; UPDATE alert_sch.alert_ape_se_siniestro_bandeja SET estado=0, fecha_registro=CURRENT_TIMESTAMP WHERE (nro_siniestro, a_siniestro, nro_cuenta, id_siniestro_bandeja) IN (SELECT A.nro_siniestro, A.a_siniestro, A.nro_cuenta, A.id_siniestro_bandeja FROM alert_sch.alert_ape_se_siniestro_bandeja A WHERE A.id_siniestro_bandeja=(SELECT max(id_siniestro_bandeja) FROM alert_sch.alert_ape_se_siniestro_bandeja B WHERE B.nro_siniestro=A.nro_siniestro AND B.a_siniestro=A.a_siniestro AND B.nro_cuenta=A.nro_cuenta GROUP BY B.nro_siniestro, B.a_siniestro, B.nro_cuenta) AND A.estado>0 AND A.id_bandeja=",id_bandeja," ")
filter_upd = "";
for(i in 1:nrow(alert_data)){
    filter_upd = paste0(filter_upd," OR (A.nro_siniestro=",alert_data$nro_siniestro[i]," AND A.a_siniestro=",alert_data$a_siniestro[i]," AND A.nro_cuenta=",alert_data$nro_cuenta[i],")")
}

if(nrow(alert_data)>0){
    query_update_fin = paste0(query_update_ini," AND NOT (",substring(filter_upd, 4),"));")
    dtab = dbGetQuery(con, query_update_fin)
}

#Generate Upd
id_bandeja_o <- 6
query_update_ini_o = paste0("SET search_path TO alert_sch; UPDATE alert_sch.alert_ape_se_siniestro_bandeja SET estado=0, fecha_registro=CURRENT_TIMESTAMP WHERE (nro_siniestro, a_siniestro, nro_cuenta, id_siniestro_bandeja) IN (SELECT A.nro_siniestro, A.a_siniestro, A.nro_cuenta, A.id_siniestro_bandeja FROM alert_sch.alert_ape_se_siniestro_bandeja A WHERE A.id_siniestro_bandeja=(SELECT max(id_siniestro_bandeja) FROM alert_sch.alert_ape_se_siniestro_bandeja B WHERE B.nro_siniestro=A.nro_siniestro AND B.a_siniestro=A.a_siniestro AND B.nro_cuenta=A.nro_cuenta GROUP BY B.nro_siniestro, B.a_siniestro, B.nro_cuenta) AND A.estado>0 AND A.id_bandeja=",id_bandeja_o," ")
filter_upd_o = "";
for(i in 1:nrow(alert_data_o)){
    filter_upd_o = paste0(filter_upd_o," OR (A.nro_siniestro=",alert_data_o$nro_siniestro[i]," AND A.a_siniestro=",alert_data_o$a_siniestro[i]," AND A.nro_cuenta=",alert_data_o$nro_cuenta[i],")")
}

if(nrow(alert_data_o)>0){
    query_update_fin_o = paste0(query_update_ini_o," AND NOT (",substring(filter_upd_o, 4),"));")
    dtab = dbGetQuery(con, query_update_fin_o)
}

dbDisconnect(con)
print("Pg: OK")

t_fin <- Sys.time()
t_fin
t_run <- t_fin - t_ini
t_run
