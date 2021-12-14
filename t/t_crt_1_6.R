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
#-- Caso: Persona Natural (Bandejas: AnalisisLiquidacion, RealizaAuditoria)
t_ini <- Sys.time()
t_ini

setwd("/var/www/html/api_R/t/")
source("t_config.R")

#1. SqlSvr
drv <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver",innsslib) 
uat_conn <- dbConnect(drv, innsscon, innssusr, innsspwd)
rm(innsspwd)

alert_data_al <- dbGetQuery(uat_conn,paste("use soat_gruposis; SELECT NroSiniestro AS nro_siniestro, fecha_bandeja FROM GrupoSIS..Bandejas_General WHERE equipo='SOAT-SE' AND fecha_bandeja BETWEEN '2000-01-01' AND convert(date,dateadd(day,1, getdate())) AND [Nombre Bandeja]='AnalisisLiquidacion' AND (codigo_amparo=2 OR codigo_amparo=3 OR codigo_amparo=6 OR tipo_entidad='PN') AND [usuario que remite a bandeja]!='conciliacionSE' AND NOT (codigo_amparo=11 OR codigo_amparo=12) AND fecha_bandeja >= convert(date,dateadd(day,-7, getdate()));"))

alert_data_ra <- dbGetQuery(uat_conn,paste("use soat_gruposis; SELECT NroSiniestro AS nro_siniestro, fecha_bandeja FROM GrupoSIS..Bandejas_General WHERE equipo='SOAT-SE' AND fecha_bandeja BETWEEN '2000-01-01' AND convert(date,dateadd(day,1, getdate())) AND [Nombre Bandeja]='RealizaAuditoria' AND Rol!='Coordinador' AND Rol!='Auditor Administrativo' AND (codigo_amparo=2 OR codigo_amparo=3 OR codigo_amparo=6 OR tipo_entidad='PN') AND [usuario que remite a bandeja]!='conciliacionSE' AND NOT (codigo_amparo=11 OR codigo_amparo=12) AND fecha_bandeja >= convert(date,dateadd(day,-7, getdate()));"))
dbDisconnect(uat_conn)

#Split: s_a_c AnalisisLiquidacion
if(nrow(alert_data_al) > 0){
	s_a_c_al_tmp <- strsplit(as.character(alert_data_al$nro_siniestro), "/|[*]")

	alert_data_al$nro_siniestro <- sapply(s_a_c_al_tmp, "[[" , 1)
	alert_data_al$a_siniestro <- sapply(s_a_c_al_tmp, "[[", 2)
	alert_data_al$nro_cuenta <- sapply(s_a_c_al_tmp, "[[", 3)
	#print(alert_data_al)
}
print(nrow(alert_data_al))

print("SqlSvr: AL OK")

#Split: s_a_c RealizaAuditoria
if(nrow(alert_data_ra) > 0){
	s_a_c_ra_tmp <- strsplit(as.character(alert_data_ra$nro_siniestro), "/|[*]")

	alert_data_ra$nro_siniestro <- sapply(s_a_c_ra_tmp, "[[" , 1)
	alert_data_ra$a_siniestro <- sapply(s_a_c_ra_tmp, "[[", 2)
	alert_data_ra$nro_cuenta <- sapply(s_a_c_ra_tmp, "[[", 3)
	#print(alert_data_ra)
}
print(nrow(alert_data_ra))

print("SqlSvr: RA OK")

#2. PgSql
pg = dbDriver("PostgreSQL")
con = dbConnect(pg, user=innpgusr, password=innpgpwd, host=innpghst, port=innpgprt, dbname=innpgdbn)

# AnalisisLiquidacion
if(nrow(alert_data_al) > 0){
	alert_data_al$id_siniestro_bandeja <- 0
	alert_data_al$id_bandeja <- 10
	alert_data_al$estado <- 7
	alert_data_al$fecha_registro  <- Sys.time() 
	dtab = dbGetQuery(con,"SET search_path TO alert_sch;")
	dbWriteTable(con, "alert_soat_se_siniestro_bandeja", alert_data_al, row.names=FALSE, append=TRUE)
	print("Pg: AL OK")
}

# RealizaAuditoria
if(nrow(alert_data_ra) > 0){
	alert_data_ra$id_siniestro_bandeja <- 0
	alert_data_ra$id_bandeja <- 4
	alert_data_ra$estado <- 8
	alert_data_ra$fecha_registro  <- Sys.time() 
	dtab = dbGetQuery(con,"SET search_path TO alert_sch;")
	dbWriteTable(con, "alert_soat_se_siniestro_bandeja", alert_data_ra, row.names=FALSE, append=TRUE)
	print("Pg: ARC OK")
}

dbDisconnect(con)
print("Pg: OK")

t_fin <- Sys.time()
t_fin
t_run <- t_fin - t_ini
t_run
