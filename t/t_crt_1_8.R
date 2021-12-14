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
#-- Caso: EGRC (Bandeja: AnalisisLiquidacion, Rol: Analista a AuditorMedico)
t_ini <- Sys.time()
t_ini

setwd("/var/www/html/api_R/t/")
source("t_config.R")

#1. SqlSvr
drv <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver",innsslib) 
uat_conn <- dbConnect(drv, innsscon, innssusr, innsspwd)
rm(innsspwd)

alert_data <- dbGetQuery(uat_conn,paste("use soat_gruposis; SELECT NroSiniestro AS nro_siniestro, fecha_bandeja FROM GrupoSIS..Bandejas_General WHERE equipo='SOAT-SE' AND fecha_bandeja BETWEEN '2000-01-01' AND convert(date,dateadd(day,1, getdate())) AND [Nombre Bandeja]='AnalisisLiquidacion' AND Rol='Analista' AND [usuario que remite a bandeja]!='conciliacionSE' AND codigo_estado_cuenta='EGRC' AND NOT ((codigo_amparo=2 OR codigo_amparo=3 OR codigo_amparo=6 OR tipo_entidad='PN') AND [usuario que remite a bandeja]!='conciliacionSE') AND NOT (codigo_amparo=11 OR codigo_amparo=12) AND fecha_bandeja >= convert(date,dateadd(day,-7, getdate()));"))

alert_data_o <- dbGetQuery(uat_conn,paste("use soat_gruposis; SELECT NroSiniestro AS nro_siniestro, fecha_bandeja FROM GrupoSIS..Bandejas_General WHERE equipo='SOAT-SE' AND fecha_bandeja BETWEEN '2000-01-01' AND convert(date,dateadd(day,1, getdate())) AND [Nombre Bandeja]='AnalisisLiquidacion' AND Rol LIKE 'Auditor M%' AND Rol LIKE '%dico' AND [usuario que remite a bandeja]!='conciliacionSE' AND NOT ((codigo_amparo=2 OR codigo_amparo=3 OR codigo_amparo=6 OR tipo_entidad='PN') AND [usuario que remite a bandeja]!='conciliacionSE') AND NOT (codigo_amparo=11 OR codigo_amparo=12) AND fecha_bandeja >= convert(date,dateadd(day,-7, getdate()));"))
# SELECT [Nombre Bandeja] AS 'nombre_bandeja', Rol AS rol, paquete, estado_paquete, NroSiniestro AS s_a_c, codigo_estado_cuenta AS estado_cuenta, ISNULL (codigo_objecion,'') AS objecion, CONVERT (VARCHAR (10),fecha_bandeja,103) AS fecha_bandeja, codigo_amparo, [usuario que remite a bandeja] AS 'usuario_bandeja' FROM GrupoSIS..Bandejas_General WHERE equipo = 'SOAT-SE' AND fecha_bandeja BETWEEN '2020-01-01' AND convert(date,dateadd(day,-1, getdate())) AND ROL != '' ORDER BY [Nombre Bandeja], Rol
dbDisconnect(uat_conn)

#Split: s_a_c
if(nrow(alert_data) > 0){
	s_a_c_tmp <- strsplit(as.character(alert_data$nro_siniestro), "/|[*]")

	alert_data$nro_siniestro <- sapply(s_a_c_tmp, "[[" , 1)
	alert_data$a_siniestro <- sapply(s_a_c_tmp, "[[", 2)
	alert_data$nro_cuenta <- sapply(s_a_c_tmp, "[[", 3)
}
print(nrow(alert_data))
print("SqlSvr: EGRC OK")

#Split: s_a_c
if(nrow(alert_data_o) > 0){
	s_a_c_o_tmp <- strsplit(as.character(alert_data_o$nro_siniestro), "/|[*]")

	alert_data_o$nro_siniestro <- sapply(s_a_c_o_tmp, "[[" , 1)
	alert_data_o$a_siniestro <- sapply(s_a_c_o_tmp, "[[", 2)
	alert_data_o$nro_cuenta <- sapply(s_a_c_o_tmp, "[[", 3)
}
print(nrow(alert_data_o))
print("SqlSvr: ORIGINAL OK")

#2. PgSql
pg = dbDriver("PostgreSQL")
con = dbConnect(pg, user=innpgusr, password=innpgpwd, host=innpghst, port=innpgprt, dbname=innpgdbn)

if(nrow(alert_data) > 0){
	alert_data$id_siniestro_bandeja <- 0
	alert_data$id_bandeja <- 9
	alert_data$estado <- 10
	alert_data$fecha_registro  <- Sys.time() 
	dtab = dbGetQuery(con,"SET search_path TO alert_sch;")
	dbWriteTable(con, "alert_soat_se_siniestro_bandeja", alert_data, row.names=FALSE, append=TRUE)
	print("Pg: EGRC OK")
}

if(nrow(alert_data_o) > 0){
	alert_data_o$id_siniestro_bandeja <- 0
	alert_data_o$id_bandeja <- 9
	alert_data_o$estado <- 11
	alert_data_o$fecha_registro  <- Sys.time() 
	dtab = dbGetQuery(con,"SET search_path TO alert_sch;")
	dbWriteTable(con, "alert_soat_se_siniestro_bandeja", alert_data_o, row.names=FALSE, append=TRUE)
	print("Pg: ORIGINAL OK")
}

dbDisconnect(con)
print("Pg: OK")

t_fin <- Sys.time()
t_fin
t_run <- t_fin - t_ini
t_run
