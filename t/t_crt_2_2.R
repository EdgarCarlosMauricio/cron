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

#---UPDATE BANDEJAS DE GESTION (23)
t_ini <- Sys.time()
t_ini

#1. PgSql
pg = dbDriver("PostgreSQL")
con = dbConnect(pg, user="sis_bigdata", password="F4llst4ck2020*", host="localhost", port=5432, dbname="sis_analytics")

q_pg_1 = paste0("SET search_path TO alert_sch; INSERT INTO alert_soat_se_siniestro_bandeja SELECT A.nro_siniestro, A.a_siniestro, A.nro_cuenta, 0, now()::date, 23, CURRENT_TIMESTAMP, 1 FROM alert_sch.alert_soat_se_siniestro_bandeja A WHERE A.id_siniestro_bandeja=(SELECT max(id_siniestro_bandeja) FROM alert_sch.alert_soat_se_siniestro_bandeja B WHERE B.nro_siniestro=A.nro_siniestro AND B.a_siniestro=A.a_siniestro AND B.nro_cuenta=A.nro_cuenta GROUP BY B.nro_siniestro, B.a_siniestro, B.nro_cuenta) AND A.estado=0;")
dataAlert_1 = dbGetQuery(con,q_pg_1)
print(dataAlert_1)
print("Pg Bandeja: OK")

dbDisconnect(con)
print("Pg: OK")

t_fin <- Sys.time()
t_fin
t_run <- t_fin - t_ini
t_run
