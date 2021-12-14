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
t_ini <- Sys.time()
t_ini

setwd("/var/www/html/api_R/t/")
source("t_config.R")

#1. SqlSvr
drv <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver",innsslib) 
uat_conn <- dbConnect(drv, innsscon, innssusr, innsspwd)
rm(innsspwd)

#Persona Juridica
alert_data_pj <- dbGetQuery(uat_conn,paste("use vida_gruposis; SELECT DISTINCT NroSiniestro AS nro_siniestro, fecha_bandeja FROM GrupoSIS..Bandejas_General WHERE equipo='VIDA-APE' AND fecha_bandeja BETWEEN '2000-01-01' AND convert(date,dateadd(day,1, getdate())) AND [Nombre Bandeja]='RealizaAuditoria' AND Rol='Coordinador' AND tipo_entidad!='PN' AND fecha_bandeja >= convert(date,dateadd(day,-2, getdate()));"))
#Persona Natural
alert_data_pn <- dbGetQuery(uat_conn,paste("use vida_gruposis; SELECT DISTINCT NroSiniestro AS nro_siniestro, fecha_bandeja FROM GrupoSIS..Bandejas_General WHERE equipo='VIDA-APE' AND fecha_bandeja BETWEEN '2000-01-01' AND convert(date,dateadd(day,1, getdate())) AND [Nombre Bandeja]='RealizaAuditoria' AND Rol='Coordinador' AND tipo_entidad='PN' AND fecha_bandeja >= convert(date,dateadd(day,-2, getdate()));"))

dbDisconnect(uat_conn)

#Split: s_a_c
#Persona Juridica
if(nrow(alert_data_pj) > 0){
	s_a_c_tmp <- strsplit(as.character(alert_data_pj$nro_siniestro), "/|[*]")

	alert_data_pj$nro_siniestro <- sapply(s_a_c_tmp, "[[" , 1)
	alert_data_pj$a_siniestro <- sapply(s_a_c_tmp, "[[", 2)
	alert_data_pj$nro_cuenta <- sapply(s_a_c_tmp, "[[", 3)
    print(nrow(alert_data_pj))
    print("SqlSvr: RealizaAuditoria+Coordinador+PJ OK")
}
#Persona Natural
if(nrow(alert_data_pn) > 0){
	s_a_c_tmp <- strsplit(as.character(alert_data_pn$nro_siniestro), "/|[*]")

	alert_data_pn$nro_siniestro <- sapply(s_a_c_tmp, "[[" , 1)
	alert_data_pn$a_siniestro <- sapply(s_a_c_tmp, "[[", 2)
	alert_data_pn$nro_cuenta <- sapply(s_a_c_tmp, "[[", 3)
    print(nrow(alert_data_pn))
    print("SqlSvr: RealizaAuditoria+Coordinador+PN OK")
}
print("SqlSvr: OK")


#2. PgSql
pg = dbDriver("PostgreSQL")
con = dbConnect(pg, user=innpgusr, password=innpgpwd, host=innpghst, port=innpgprt, dbname=innpgdbn)

#Persona Juridica
if(nrow(alert_data_pj) > 0){
	alert_data_pj$id_siniestro_bandeja <- 0
	alert_data_pj$id_bandeja <- 1
	alert_data_pj$estado <- 2
	alert_data_pj$fecha_registro  <- Sys.time() 
	dtab = dbGetQuery(con,"SET search_path TO alert_sch;")
	dbWriteTable(con, "alert_ape_se_siniestro_bandeja", alert_data_pj, row.names=FALSE, append=TRUE)
	print("Pg: PreRevision OK")
}
#Persona Natural
if(nrow(alert_data_pn) > 0){
	alert_data_pn$id_siniestro_bandeja <- 0
	alert_data_pn$id_bandeja <- 30
	alert_data_pn$estado <- 30
	alert_data_pn$fecha_registro  <- Sys.time() 
	dtab = dbGetQuery(con,"SET search_path TO alert_sch;")
	dbWriteTable(con, "alert_ape_se_siniestro_bandeja", alert_data_pn, row.names=FALSE, append=TRUE)
	print("Pg: PreRevision OK")
}

dbDisconnect(con)
print("Pg: OK")

#print(alert_data)

t_fin <- Sys.time()
t_fin
t_run <- t_fin - t_ini
t_run
