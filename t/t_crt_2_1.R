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

#---UPDATE BANDEJAS DE GESTION (1 y 20) PARA EVITAR SEGUIR ESTADOS DE CUENTA
t_ini <- Sys.time()
t_ini

setwd("/var/www/html/api_R/t/")
source("t_config.R")

#1. PgSql
pg = dbDriver("PostgreSQL")
con = dbConnect(pg, user=innpgusr, password=innpgpwd, host=innpghst, port=innpgprt, dbname=innpgdbn)

q_pg_1 = paste0("SET search_path TO alert_sch; UPDATE alert_sch.alert_soat_se_siniestro SET estado=2 WHERE (nro_siniestro,a_siniestro,nro_cuenta) IN (SELECT nro_siniestro, a_siniestro, nro_cuenta FROM alert_sch.alert_soat_se_siniestro_bandeja A WHERE A.id_siniestro_bandeja = (SELECT max(id_siniestro_bandeja) FROM alert_sch.alert_soat_se_siniestro_bandeja WHERE nro_siniestro=A.nro_siniestro AND a_siniestro=A.a_siniestro AND nro_cuenta=A.nro_cuenta) AND A.id_bandeja=1) AND estado!=2;")
dataAlert_1 = dbGetQuery(con,q_pg_1)
print(dataAlert_1)
print("Pg Bandeja 1: OK")

q_pg_20 = paste0("SET search_path TO alert_sch; UPDATE alert_sch.alert_soat_se_siniestro SET estado=3 WHERE (nro_siniestro,a_siniestro,nro_cuenta) IN (SELECT nro_siniestro, a_siniestro, nro_cuenta FROM alert_sch.alert_soat_se_siniestro_bandeja A WHERE A.id_siniestro_bandeja = (SELECT max(id_siniestro_bandeja) FROM alert_sch.alert_soat_se_siniestro_bandeja WHERE nro_siniestro=A.nro_siniestro AND a_siniestro=A.a_siniestro AND nro_cuenta=A.nro_cuenta) AND A.id_bandeja=20) AND estado!=3;")
dataAlert_20 = dbGetQuery(con,q_pg_20)
print(dataAlert_20)
print("Pg Bandeja 20: OK")

q_pg_1_20 = paste0("SET search_path TO alert_sch; UPDATE alert_sch.alert_soat_se_siniestro SET estado=1 WHERE (nro_siniestro,a_siniestro,nro_cuenta) IN (SELECT nro_siniestro, a_siniestro, nro_cuenta FROM alert_sch.alert_soat_se_siniestro_bandeja A WHERE A.id_siniestro_bandeja = (SELECT max(id_siniestro_bandeja) FROM alert_sch.alert_soat_se_siniestro_bandeja WHERE nro_siniestro=A.nro_siniestro AND a_siniestro=A.a_siniestro AND nro_cuenta=A.nro_cuenta) AND A.id_bandeja!=1 AND A.id_bandeja!=20) AND (estado=2 OR estado=3);")
dataAlert_1_20 = dbGetQuery(con,q_pg_1_20)
print(dataAlert_1_20)
print("Pg Bandeja NO 1 NO 20: OK")


q_pg_vm = paste0("REFRESH MATERIALIZED VIEW CONCURRENTLY alert_sch.alert_soat_se_siniestro_estado_vw;")
dataAlert_vm = dbGetQuery(con,q_pg_vm)
print(dataAlert_vm)
print("Pg View Materialized: OK")

dbDisconnect(con)
print("Pg: OK")

t_fin <- Sys.time()
t_fin
t_run <- t_fin - t_ini
t_run
