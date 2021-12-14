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
#-- Caso: AnalisisConjuntoGlosas
t_ini <- Sys.time()
t_ini

setwd("/var/www/html/api_R/t/")
source("t_config.R")

#1. SqlSvr
drv <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver",innsslib) 
uat_conn <- dbConnect(drv, innsscon, innssusr, innsspwd)
rm(innsspwd)

alert_data_al <- dbGetQuery(uat_conn,paste("use vida_gruposis; SELECT NroSiniestro AS nro_siniestro, fecha_bandeja FROM GrupoSIS..Bandejas_General WHERE equipo='VIDA-APE' AND fecha_bandeja BETWEEN '2000-01-01' AND convert(date,dateadd(day,1, getdate())) AND [Nombre Bandeja]='AnalisisLiquidacion' AND [usuario que remite a bandeja]='conciliacionAPE';"))

alert_data_arc <- dbGetQuery(uat_conn,paste("use vida_gruposis; SELECT NroSiniestro AS nro_siniestro, fecha_bandeja FROM GrupoSIS..Bandejas_General WHERE equipo='VIDA-APE' AND fecha_bandeja BETWEEN '2000-01-01' AND convert(date,dateadd(day,1, getdate())) AND [Nombre Bandeja]='ActualizacionRespuestasCartas' AND [usuario que remite a bandeja]='conciliacionAPE';"))

# SELECT [Nombre Bandeja] AS 'nombre_bandeja', Rol AS rol, paquete, estado_paquete, NroSiniestro AS s_a_c, codigo_estado_cuenta AS estado_cuenta, ISNULL (codigo_objecion,'') AS objecion, CONVERT (VARCHAR (10),fecha_bandeja,103) AS fecha_bandeja, codigo_amparo, [usuario que remite a bandeja] AS 'usuario_bandeja' FROM GrupoSIS..Bandejas_General WHERE equipo = 'VIDA-APE' AND fecha_bandeja BETWEEN '2020-01-01' AND convert(date,dateadd(day,-1, getdate())) AND ROL != '' ORDER BY [Nombre Bandeja], Rol
dbDisconnect(uat_conn)

if(nrow(alert_data_al) > 0){
	#Split: s_a_c AnalisisLiquidacion
	s_a_c_al_tmp <- strsplit(as.character(alert_data_al$nro_siniestro), "/|[*]")

	alert_data_al$nro_siniestro <- sapply(s_a_c_al_tmp, "[[" , 1)
	alert_data_al$a_siniestro <- sapply(s_a_c_al_tmp, "[[", 2)
	alert_data_al$nro_cuenta <- sapply(s_a_c_al_tmp, "[[", 3)
}
print(nrow(alert_data_al))

print("SqlSvr: AL OK")

if(nrow(alert_data_arc) > 0 ){
	#Split: s_a_c ActualizacionRespuestaCartas
	s_a_c_arc_tmp <- strsplit(as.character(alert_data_arc$nro_siniestro), "/|[*]")

	alert_data_arc$nro_siniestro <- sapply(s_a_c_arc_tmp, "[[" , 1)
	alert_data_arc$a_siniestro <- sapply(s_a_c_arc_tmp, "[[", 2)
	alert_data_arc$nro_cuenta <- sapply(s_a_c_arc_tmp, "[[", 3)
}
print(nrow(alert_data_arc))

print("SqlSvr: ARC OK")

#2. PgSql
pg = dbDriver("PostgreSQL")
con = dbConnect(pg, user=innpgusr, password=innpgpwd, host=innpghst, port=innpgprt, dbname=innpgdbn)

# AnalisisLiquidacion
if(nrow(alert_data_al) > 0){
	alert_data_al$id_siniestro_bandeja <- 0
	alert_data_al$id_bandeja <- 8
	alert_data_al$estado <- 4
	alert_data_al$fecha_registro  <- Sys.time() 
	dtab = dbGetQuery(con,"SET search_path TO alert_sch;")
	dbWriteTable(con, "alert_ape_se_siniestro_bandeja", alert_data_al, row.names=FALSE, append=TRUE)
	print("Pg: AL OK")
}

# ActualizacionRespuestaCartas
if(nrow(alert_data_arc) > 0 ){
	alert_data_arc$id_siniestro_bandeja <- 0
	alert_data_arc$id_bandeja <- 6
	alert_data_arc$estado <- 5
	alert_data_arc$fecha_registro  <- Sys.time() 
	dtab = dbGetQuery(con,"SET search_path TO alert_sch;")
	dbWriteTable(con, "alert_ape_se_siniestro_bandeja", alert_data_arc, row.names=FALSE, append=TRUE)
	print("Pg: ARC OK")
}

dbDisconnect(con)
print("Pg: OK")

t_fin <- Sys.time()
t_fin
t_run <- t_fin - t_ini
t_run
