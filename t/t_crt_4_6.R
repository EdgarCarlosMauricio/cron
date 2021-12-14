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

#---UPDATE CUENTAS DEFINIDAS SIN CONOCIMIENTO DE NOTIFICACION PARA EVITAR SEGUIR ESTADOS DE CUENTA
t_ini <- Sys.time()
t_ini

setwd("/var/www/html/api_R/t/")
source("t_config.R")

#1. PgSql
pg = dbDriver("PostgreSQL")
con = dbConnect(pg, user=innpgusr, password=innpgpwd, host=innpghst, port=innpgprt, dbname=innpgdbn)


# 208890 total
q_pg_vm = paste0("UPDATE alert_sch.alert_soat_se_siniestro SET estado=25 WHERE (nro_siniestro, a_siniestro, nro_cuenta) IN (SELECT ASSSB.nro_siniestro, ASSSB.a_siniestro, ASSSB.nro_cuenta FROM alert_sch.alert_soat_se_siniestro_bandeja ASSSB WHERE (ASSSB.nro_siniestro, ASSSB.a_siniestro, ASSSB.nro_cuenta, ASSSB.id_siniestro_bandeja) IN (SELECT nro_siniestro, a_siniestro, nro_cuenta, max(id_siniestro_bandeja) FROM alert_sch.alert_soat_se_siniestro_bandeja GROUP BY nro_siniestro, a_siniestro, nro_cuenta) AND id_bandeja=23 AND (nro_siniestro, a_siniestro, nro_cuenta) IN (SELECT nro_siniestro, a_siniestro, nro_cuenta FROM alert_sch.alert_soat_se_siniestro_estado WHERE (nro_siniestro, a_siniestro, nro_cuenta, id_estado) IN (SELECT nro_siniestro, a_siniestro, nro_cuenta, max(id_estado) FROM alert_sch.alert_soat_se_siniestro_estado GROUP BY nro_siniestro, a_siniestro, nro_cuenta) AND (id_estado_cuenta=12 OR id_estado_cuenta=18)) AND estado != 25 LIMIT 100);")
dataAlert_notify = dbGetQuery(con,q_pg_vm)
print(dataAlert_notify)
print("Pg Siniestro sin Notificacion: OK")

dbDisconnect(con)
print("Pg: OK")

t_fin <- Sys.time()
t_fin
t_run <- t_fin - t_ini
t_run
