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

#---CONSULTA GENERAL BANDEJAS: desactivar ultima bandeja por siniestro-year-cuenta (s_a_c)
#----Caso-Bandeja: 28 y 29
t_ini <- Sys.time()
t_ini

setwd("/var/www/html/api_R/t/")
source("t_config.R")

#1. SqlSvr
drv <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver",innsslib) 
uat_conn <- dbConnect(drv, innsscon, innssusr, innsspwd)
rm(innsspwd)

#Persona Juridica
alert_data_pj <- dbGetQuery(uat_conn,paste("use vida_gruposis; SELECT NroSiniestro AS nro_siniestro, fecha_bandeja FROM GrupoSIS..Bandejas_General WHERE equipo='VIDA-APE' AND fecha_bandeja BETWEEN '2000-01-01' AND convert(date,dateadd(day,1, getdate())) AND [Nombre Bandeja] LIKE 'PreRevision' AND Rol='AudExterna' AND tipo_entidad!='PN';"))
#Persona Natural
alert_data_pn <- dbGetQuery(uat_conn,paste("use vida_gruposis; SELECT NroSiniestro AS nro_siniestro, fecha_bandeja FROM GrupoSIS..Bandejas_General WHERE equipo='VIDA-APE' AND fecha_bandeja BETWEEN '2000-01-01' AND convert(date,dateadd(day,1, getdate())) AND [Nombre Bandeja] LIKE 'PreRevision' AND Rol='AudExterna' AND tipo_entidad='PN';"))

dbDisconnect(uat_conn)

#Persona Juridica
if(nrow(alert_data_pj) > 0){
	s_a_c_tmp <- strsplit(as.character(alert_data_pj$nro_siniestro), "/|[*]")

	alert_data_pj$nro_siniestro <- sapply(s_a_c_tmp, "[[" , 1)
	alert_data_pj$a_siniestro <- sapply(s_a_c_tmp, "[[", 2)
	alert_data_pj$nro_cuenta <- sapply(s_a_c_tmp, "[[", 3)
    print(nrow(alert_data_pj))
    print("SqlSvr: PreRevision PJ OK")
}
#Persona Natural
if(nrow(alert_data_pn) > 0){
	s_a_c_tmp <- strsplit(as.character(alert_data_pn$nro_siniestro), "/|[*]")

	alert_data_pn$nro_siniestro <- sapply(s_a_c_tmp, "[[" , 1)
	alert_data_pn$a_siniestro <- sapply(s_a_c_tmp, "[[", 2)
	alert_data_pn$nro_cuenta <- sapply(s_a_c_tmp, "[[", 3)
    print(nrow(alert_data_pn))
    print("SqlSvr: PreRevision PN OK")
}
print("SqlSvr: -OK")

#2. PgSql
pg = dbDriver("PostgreSQL")
con = dbConnect(pg, user=innpgusr, password=innpgpwd, host=innpghst, port=innpgprt, dbname=innpgdbn)

#Generate Upd
#Persona Juridica
if(nrow(alert_data_pj) >= 0){
    print(alert_data_pj)
    id_bandeja <- 28
    fecha_registro  <- Sys.time() 
    query_update_ini = paste0("SET search_path TO alert_sch; UPDATE alert_sch.alert_ape_se_siniestro_bandeja SET estado=0, fecha_registro=CURRENT_TIMESTAMP WHERE (nro_siniestro, a_siniestro, nro_cuenta, id_siniestro_bandeja) IN (SELECT A.nro_siniestro, A.a_siniestro, A.nro_cuenta, A.id_siniestro_bandeja FROM alert_sch.alert_ape_se_siniestro_bandeja A WHERE A.id_siniestro_bandeja=(SELECT max(id_siniestro_bandeja) FROM alert_sch.alert_ape_se_siniestro_bandeja B WHERE B.nro_siniestro=A.nro_siniestro AND B.a_siniestro=A.a_siniestro AND B.nro_cuenta=A.nro_cuenta GROUP BY B.nro_siniestro, B.a_siniestro, B.nro_cuenta) AND A.estado>0 AND A.id_bandeja=",id_bandeja," ")

    if(nrow(alert_data_pj) > 0){
        filter_upd = "";
        for(i in 1:nrow(alert_data_pj)){
            filter_upd = paste0(filter_upd," OR (A.nro_siniestro=",alert_data_pj$nro_siniestro[i]," AND A.a_siniestro=",alert_data_pj$a_siniestro[i]," AND A.nro_cuenta=",alert_data_pj$nro_cuenta[i],")")
        }

        query_update_fin = paste0(query_update_ini," AND NOT (",substring(filter_upd, 4),"));")
    }
    else{
        query_update_fin = paste0(query_update_ini,");")
    }
    dtab = dbGetQuery(con, query_update_fin)
}

#Persona Natural
if(nrow(alert_data_pn) >= 0){
    id_bandeja <- 29
    fecha_registro  <- Sys.time() 
    query_update_ini = paste0("SET search_path TO alert_sch; UPDATE alert_sch.alert_ape_se_siniestro_bandeja SET estado=0, fecha_registro=CURRENT_TIMESTAMP WHERE (nro_siniestro, a_siniestro, nro_cuenta, id_siniestro_bandeja) IN (SELECT A.nro_siniestro, A.a_siniestro, A.nro_cuenta, A.id_siniestro_bandeja FROM alert_sch.alert_ape_se_siniestro_bandeja A WHERE A.id_siniestro_bandeja=(SELECT max(id_siniestro_bandeja) FROM alert_sch.alert_ape_se_siniestro_bandeja B WHERE B.nro_siniestro=A.nro_siniestro AND B.a_siniestro=A.a_siniestro AND B.nro_cuenta=A.nro_cuenta GROUP BY B.nro_siniestro, B.a_siniestro, B.nro_cuenta) AND A.estado>0 AND A.id_bandeja=",id_bandeja," ")

    if(nrow(alert_data_pn) >= 0){
        filter_upd = "";
        for(i in 1:nrow(alert_data_pn)){
            filter_upd = paste0(filter_upd," OR (A.nro_siniestro=",alert_data_pn$nro_siniestro[i]," AND A.a_siniestro=",alert_data_pn$a_siniestro[i]," AND A.nro_cuenta=",alert_data_pn$nro_cuenta[i],")")
        }

        query_update_fin = paste0(query_update_ini," AND NOT (",substring(filter_upd, 4),"));")
    }
    else{
        query_update_fin = paste0(query_update_ini,");")
    }
    dtab = dbGetQuery(con, query_update_fin)
}

dbDisconnect(con)
print("Pg: -OK")

t_fin <- Sys.time()
t_fin
t_run <- t_fin - t_ini
t_run
