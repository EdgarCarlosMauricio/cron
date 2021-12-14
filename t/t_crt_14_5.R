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

#1. PgSql
pg = dbDriver("PostgreSQL")
con = dbConnect(pg, user="sis_bigdata", password="F4llst4ck2020*", host="localhost", port=5432, dbname="sis_analytics")

q_pg_vm = paste0("REFRESH MATERIALIZED VIEW CONCURRENTLY alert_sch.alert_ape_se_siniestro_estado_vw;")
dataAlert_vm = dbGetQuery(con,q_pg_vm)
print(dataAlert_vm)
print("Pg View Materialized: OK")

dbDisconnect(con)
print("Pg: OK")

t_fin <- Sys.time()
t_fin
t_run <- t_fin - t_ini
t_run
