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

#---CONSULTA GENERAL BANDEJAS: insertar bandejas por siniestro-year-cuenta (s_a_c)
#-- Check
t_ini <- Sys.time()
t_ini

#1. SqlSvr
print("SqlSvr: START")
pw <- {"F8FM2h8foL*"}

drv <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver","/home/admin/lib_R/sqljdbc_7.2/enu/mssql-jdbc-7.2.1.jre8.jar") 
uat_conn <- dbConnect(drv, "jdbc:sqlserver://172.30.25.11;databaseName=soat_gruposis", "Bigdata", pw)
rm(pw)

sac_filter = "85106/2020*1"
print(paste0("SAC: ",sac_filter))
query_cuentas_srv = paste0("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1 AND TABLE_NAME = 'egresos';")
# SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1 AND TABLE_NAME = 'Cuenta_Liquidaciones';
# SELECT TOP(1) EG.valor_total AS valor_pagado, EG.fecha_egreso AS fecha_pago, EG.nro_doc_pago, EG.nro_egreso AS id_egreso, RA.a_siniestro, RA.nro_siniestro, RA.nro_cuenta, RA.id_ra, CL.ID AS id_liq FROM Cuenta_Liquidaciones CL WITH (NOLOCK) INNER JOIN Registro_Analisis RA WITH (NOLOCK) ON CL.nro_siniestro = RA.nro_siniestro AND CL.a_siniestro = RA.a_siniestro AND CL.nro_cuenta = RA.nro_cuenta AND CL.nro_ra = RA.id_ra INNER JOIN egresos EG WITH (NOLOCK) ON EG.nro_egreso = RA.nro_egreso WHERE CL.codigo_tarifa_medica IN ('70001','70008');
# SELECT CL.ID, RA.nro_siniestro, RA.a_siniestro, RA.nro_cuenta, RA.id_ra, (CL.valor_cobrado_liquidacion*CL.cantidad_tarifa) AS valor_cobrado_tarifa, CL.valor_pagar_liquidacion, ((CL.valor_cobrado_liquidacion*CL.cantidad_tarifa) - CL.valor_pagar_liquidacion) AS valor_glosa, RA.fecha_liquidacion, CL.codigo_tarifa_medica FROM Cuenta_Liquidaciones CL WITH (NOLOCK) INNER JOIN Registro_Analisis RA WITH (NOLOCK) ON CL.nro_siniestro = RA.nro_siniestro AND CL.a_siniestro = RA.a_siniestro AND CL.nro_cuenta = RA.nro_cuenta AND CL.nro_ra = RA.id_ra WHERE CL.codigo_tarifa_medica IN ('70001','70008') AND RA.nro_siniestro=43949 AND RA.a_siniestro=2018 AND RA.nro_cuenta=1;
# SELECT TOP(1) RA.nro_siniestro, RA.a_siniestro, RA.nro_cuenta, RA.id_ra, CL.codigo_tarifa_medica FROM Cuenta_Liquidaciones CL WITH (NOLOCK) INNER JOIN Registro_Analisis RA WITH (NOLOCK) ON CL.nro_siniestro = RA.nro_siniestro AND CL.a_siniestro = RA.a_siniestro AND CL.nro_cuenta = RA.nro_cuenta AND CL.nro_ra = RA.id_ra WHERE CL.codigo_tarifa_medica IN ('70001','70008') GROUP BY RA.nro_siniestro, RA.a_siniestro, RA.nro_cuenta, RA.id_ra, CL.codigo_tarifa_medica HAVING count(*) > 1;
# SELECT TOP(1) RA.nro_siniestro, RA.a_siniestro, RA.nro_cuenta, RA.id_ra, CL.ID, (CL.valor_cobrado_liquidacion*CL.cantidad_tarifa) AS valor_cobrado_tarifa, CL.valor_pagar_liquidacion, ((CL.valor_cobrado_liquidacion*CL.cantidad_tarifa) - CL.valor_pagar_liquidacion) AS valor_glosa, RA.fecha_liquidacion, CL.codigo_tarifa_medica FROM Cuenta_Liquidaciones CL WITH (NOLOCK) INNER JOIN Registro_Analisis RA WITH (NOLOCK) ON CL.nro_siniestro = RA.nro_siniestro AND CL.a_siniestro = RA.a_siniestro AND CL.nro_cuenta = RA.nro_cuenta AND CL.nro_ra = RA.id_ra WHERE CL.codigo_tarifa_medica IN ('70001','70008');
data_cuentas_srv <- dbGetQuery(uat_conn,query_cuentas_srv)
print("Cuentas")
print(data_cuentas_srv)

dbDisconnect(uat_conn)
print("SqlSvr: END")

#2. PgSql
print("Pg: START")
pg = dbDriver("PostgreSQL")
con = dbConnect(pg, user="sis_bigdata", password="F4llst4ck2020*", host="localhost", port=5432, dbname="sis_analytics")

#query_sac_pg = paste0("SET search_path TO alert_sch; SELECT * FROM alert_sch.alert_soat_se_siniestro A WHERE A.nro_siniestro||'/'||A.a_siniestro||'*'||A.nro_cuenta = '",sac_filter,"';")
#data_sac_pg = dbGetQuery(con,query_sac_pg)
#print("BaseCuentas")
#print(data_sac_pg)


dbDisconnect(con)
print("Pg: END")

t_fin <- Sys.time()
t_fin
t_run <- t_fin - t_ini
t_run
