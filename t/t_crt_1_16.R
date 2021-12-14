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
#-- Caso: (Bandeja: VerificacionSiniestro, Rol: VerificacionCall)
t_ini <- Sys.time()
t_ini

setwd("/var/www/html/api_R/t/")
source("t_config.R")

#1. SqlSvr
drv <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver",innsslib) 
uat_conn <- dbConnect(drv, innsscon, innssusr, innsspwd)
rm(innsspwd)

#alert_data <- dbGetQuery(uat_conn,paste("use soat_gruposis; SELECT NroSiniestro AS nro_siniestro, fecha_bandeja FROM GrupoSIS..Bandejas_General WHERE equipo='SOAT-SE' AND fecha_bandeja BETWEEN '2000-01-01' AND convert(date,dateadd(day,-1, getdate())) AND [Nombre Bandeja]='VerificacionSiniestro' AND Rol LIKE 'VerificacionCall' AND NOT (codigo_amparo=11 OR codigo_amparo=12);"))
alert_data <- dbGetQuery(uat_conn,paste("use soat_gruposis; SELECT NroSiniestro AS nro_siniestro, fecha_bandeja FROM GrupoSIS..Bandejas_General WHERE equipo='SOAT-SE' AND fecha_bandeja BETWEEN '2000-01-01' AND convert(date,dateadd(day,1, getdate())) AND [Nombre Bandeja]='VerificacionSiniestro' AND Rol LIKE 'VerificacionCall' AND NOT (codigo_amparo=11 OR codigo_amparo=12) AND fecha_bandeja >= convert(date,dateadd(day,-7, getdate()));"))

dbDisconnect(uat_conn)

#Split: s_a_c
if(nrow(alert_data) > 0){
	s_a_c_tmp <- strsplit(as.character(alert_data$nro_siniestro), "/|[*]")

	alert_data$nro_siniestro <- sapply(s_a_c_tmp, "[[" , 1)
	alert_data$a_siniestro <- sapply(s_a_c_tmp, "[[", 2)
	alert_data$nro_cuenta <- sapply(s_a_c_tmp, "[[", 3)
}
print(nrow(alert_data))
print("SqlSvr: VerificacionSiniestro-VerificacionCall OK")

#2. PgSql
pg = dbDriver("PostgreSQL")
con = dbConnect(pg, user=innpgusr, password=innpgpwd, host=innpghst, port=innpgprt, dbname=innpgdbn)

if(nrow(alert_data) > 0){
	alert_data$id_siniestro_bandeja <- 0
	alert_data$id_bandeja <- 19
	alert_data$estado <- 19
	alert_data$fecha_registro  <- Sys.time() 
	dtab = dbGetQuery(con,"SET search_path TO alert_sch;")
	dbWriteTable(con, "alert_soat_se_siniestro_bandeja", alert_data, row.names=FALSE, append=TRUE)
	print("Pg: VerificacionSiniestro-VerificacionCall OK")
}

dbDisconnect(con)
print("Pg: OK")

t_fin <- Sys.time()
t_fin
t_run <- t_fin - t_ini
t_run
