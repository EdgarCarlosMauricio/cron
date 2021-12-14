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

#---Agregar nuevos SAC presentes en ConsultaGeneralBandejas y que no esten en Pg
t_ini <- Sys.time()
t_ini

setwd("/var/www/html/api_R/t/")
source("t_config.R")

#1. PgSql
pg = dbDriver("PostgreSQL")
con = dbConnect(pg, user=innpgusr, password=innpgpwd, host=innpghst, port=innpgprt, dbname=innpgdbn)

q_pg = paste0("SET search_path TO alert_sch; SELECT ASSS.nro_siniestro||'/'||ASSS.a_siniestro||'*'||ASSS.nro_cuenta AS nro_siniestro FROM alert_sch.alert_ape_se_siniestro ASSS;")
dataAlert = dbGetQuery(con,q_pg)  

print("Pg 1: OK")
print(nrow(dataAlert))

#2. SqlSvr
drv <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver",innsslib) 
uat_conn <- dbConnect(drv, innsscon, innssusr, innsspwd)
rm(innsspwd)

alert_data <- dbGetQuery(uat_conn,paste("use vida_gruposis; SELECT DISTINCT NroSiniestro AS nro_siniestro FROM GrupoSIS..Bandejas_General WHERE equipo='VIDA-APE' AND fecha_bandeja BETWEEN '2000-01-01' AND convert(date,dateadd(day,+1, getdate())) AND NroSiniestro != '';"))

dbDisconnect(uat_conn)
# write.csv(ans_pag, "filename.csv")

print("SqlSrv: OK")
print(nrow(alert_data))

#3. PgSql
diffSAC = alert_data$nro_siniestro[!alert_data$nro_siniestro %in% dataAlert$nro_siniestro]
diff_sac_tmp <- strsplit(as.character(diffSAC), "/|[*]")
print("Diff: OK")
print(length(diffSAC))

#Generate insert
if(length(diffSAC)>0){
    query_insert = "SET search_path TO alert_sch; "
    i = 0
    for (i_s in diff_sac_tmp) {
        i = i + 1
        query_insert = paste0(query_insert,"INSERT INTO alert_sch.alert_ape_se_siniestro VALUES(",i_s[1],",",i_s[2],",",i_s[3],",712,CURRENT_TIMESTAMP,1);")
        if((i-(floor(i/10)*10))==0){
            dtab = dbGetQuery(con,query_insert)
            print(paste0("Pg: Tmp Save"))
            query_insert = "SET search_path TO alert_sch; "
        }
        print(paste0(i," de ",length(diffSAC)))
    }
    dtab = dbGetQuery(con,query_insert)
}
dbDisconnect(con)

print("Pg 2: OK")

t_fin <- Sys.time()
t_fin
t_run <- t_fin - t_ini
t_run
