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

#---CONSULTA OBJECIONES NO FORMALIZADAS: insertar bandejas por siniestro-year-cuenta (s_a_c)
#-- Caso: No Formalizado
t_ini <- Sys.time()
t_ini

setwd("/var/www/html/api_R/t/")
source("t_config.R")

#1. SqlSvr
drv <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver",innsslib) 
uat_conn <- dbConnect(drv, innsscon, innssusr, innsspwd)
rm(innsspwd)

alert_data <- dbGetQuery(uat_conn,paste("use soat_gruposis; SELECT (10000000+cu.nro_siniestro) AS nro_siniestro, cu.a_siniestro, cu.nro_cuenta, ExecutionDateTime AS fecha_bandeja FROM BIZUITPersistenceStore..filter_AutorizacionCartas FA INNER JOIN BIZUITPersistenceStore..INSTANCES I ON I.InstanceId = FA.InstanceId INNER JOIN cuentas_nf cu ON (convert(varchar(10),cu.nro_siniestro)+'/'+convert(varchar(4),cu.a_siniestro)) = substring(nrosiniestro,3,len(nrosiniestro)) AND NroCuenta = cu.nro_cuenta INNER JOIN siniestros_nf s ON s.nro_siniestro = cu.nro_siniestro AND s.a_siniestro = cu.a_siniestro INNER JOIN reclamantes rc ON rc.identificacion = cu.identificacion_reclamante AND rc.codigo_ciudad = cu.ciudad_reclamante INNER JOIN afectados af ON af.identificacion = s.identificacion_afectado INNER JOIN ciudades ci ON ci.codigo_ciudad = cu.ciudad_reclamante WHERE ExecutionDateTime BETWEEN '2000-01-01' AND convert(date,dateadd(day,1, getdate())) AND FA.AuditorAsignado = 'SOATJuridicoSE' AND I.PersistentActivity = 'AutorizacionCarta' AND FA.nrosiniestro LIKE 'NF%' AND FA.equiposoat = 'SOAT-SE';"))
dbDisconnect(uat_conn)

print(alert_data)
print(nrow(alert_data))

print("SqlSvr: OK")

#2. PgSql
if(nrow(alert_data) > 0){
	pg = dbDriver("PostgreSQL")
	con = dbConnect(pg, user=innpgusr, password=innpgpwd, host=innpghst, port=innpgprt, dbname=innpgdbn)

	# AnalisisLiquidacion
	alert_data$id_siniestro_bandeja <- 0
	alert_data$id_bandeja <- 21
	alert_data$estado <- 6
	alert_data$fecha_registro  <- Sys.time() 
	dtab = dbGetQuery(con,"SET search_path TO alert_sch;")
	dbWriteTable(con, "alert_soat_se_siniestro_bandeja", alert_data, row.names=FALSE, append=TRUE)
	print("Pg: OK")

	dbDisconnect(con)
}

t_fin <- Sys.time()
t_fin
t_run <- t_fin - t_ini
t_run
