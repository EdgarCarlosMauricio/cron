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

#---CONSULTA GENERAL BANDEJAS: desactivar metodoPago=7 para siniestro-year-cuenta (s_a_c)
#----Caso-Bandeja: 20 (por reserva judicial)
t_ini <- Sys.time()
t_ini

setwd("/var/www/html/api_R/t/")
source("t_config.R")

#1. PgSql
pg = dbDriver("PostgreSQL")
con = dbConnect(pg, user=innpgusr, password=innpgpwd, host=innpghst, port=innpgprt, dbname=innpgdbn)

q_pg = paste0("SET search_path TO alert_sch; SELECT ASSS.nro_siniestro||'/'||ASSS.a_siniestro||'*'||ASSS.nro_cuenta AS nro_siniestro FROM alert_sch.alert_ape_se_siniestro ASSS WHERE (ASSS.nro_siniestro, ASSS.a_siniestro, ASSS.nro_cuenta) IN (SELECT A.nro_siniestro, A.a_siniestro, A.nro_cuenta FROM alert_sch.alert_ape_se_siniestro_bandeja A WHERE A.id_siniestro_bandeja=(SELECT max(id_siniestro_bandeja) FROM alert_sch.alert_ape_se_siniestro_bandeja B WHERE B.nro_siniestro=A.nro_siniestro AND B.a_siniestro=A.a_siniestro AND B.nro_cuenta=A.nro_cuenta GROUP BY B.nro_siniestro, B.a_siniestro, B.nro_cuenta) AND A.estado>0);")
dataAlert = dbGetQuery(con,q_pg)  
#  AND ASSS.nro_siniestro||'/'||ASSS.a_siniestro||'*'||ASSS.nro_cuenta='10335/2016*1'
print("Pg 1: OK")
print(nrow(dataAlert))

#2. SqlSvr
drv <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver",innsslib) 
uat_conn <- dbConnect(drv, innsscon, innssusr, innsspwd)
rm(innsspwd)

alert_data_pj <- dbGetQuery(uat_conn,paste("USE vida_gruposis; SELECT BG.NroSiniestro AS nro_siniestro FROM cuentas CU WITH (NOLOCK) INNER JOIN GrupoSIS..Bandejas_General BG WITH (NOLOCK) ON CONCAT(CU.nro_siniestro,'/',CU.a_siniestro,'*',CU.nro_cuenta)=BG.NroSiniestro WHERE CU.metodo_pago=7 AND BG.equipo='VIDA-APE' AND BG.tipo_entidad!='PN' AND BG.fecha_bandeja BETWEEN '2000-01-01' AND convert(date,dateadd(day,1, getdate()));"))

alert_data_pn <- dbGetQuery(uat_conn,paste("USE vida_gruposis; SELECT BG.NroSiniestro AS nro_siniestro FROM cuentas CU WITH (NOLOCK) INNER JOIN GrupoSIS..Bandejas_General BG WITH (NOLOCK) ON CONCAT(CU.nro_siniestro,'/',CU.a_siniestro,'*',CU.nro_cuenta)=BG.NroSiniestro WHERE CU.metodo_pago=7 AND BG.equipo='VIDA-APE' AND BG.tipo_entidad='PN' AND BG.fecha_bandeja BETWEEN '2000-01-01' AND convert(date,dateadd(day,1, getdate()));"))

print("SqlSrv: MetodoPago=7")
print(nrow(alert_data_pj))
print(nrow(alert_data_pn))
dbDisconnect(uat_conn)

print("SqlSvr: OK")

#3. PgSql
diffSACpj = dataAlert$nro_siniestro[dataAlert$nro_siniestro %in% alert_data_pj$nro_siniestro]
diff_sac_tmp_pj <- strsplit(as.character(diffSACpj), "/|[*]")
print("Diff: OK")
print(length(diffSACpj))

diffSACpn = dataAlert$nro_siniestro[dataAlert$nro_siniestro %in% alert_data_pn$nro_siniestro]
diff_sac_tmp_pn <- strsplit(as.character(diffSACpn), "/|[*]")
print("Diff: OK")
print(length(diffSACpn))


#Generate insert
#Persona Juridica
if(length(diffSACpj)>0){
    id_bandeja <- 20
    estado <- 22
    fecha_bandeja <- format(Sys.time(), "%Y-%m-%d")
    query_insert = "SET search_path TO alert_sch; "
    i = 0
    for (i_s in diff_sac_tmp_pj) {
        i = i + 1
        query_insert = paste0(query_insert,"INSERT INTO alert_sch.alert_ape_se_siniestro_bandeja VALUES(",i_s[1],",",i_s[2],",",i_s[3],",0,'",fecha_bandeja,"',",id_bandeja,",CURRENT_TIMESTAMP,",estado,");")
        if((i-(floor(i/10)*10))==0){
            dtab = dbGetQuery(con,query_insert)
            print(paste0("Pg: Tmp Save"))
            query_insert = "SET search_path TO alert_sch; "
        }
        print(paste0(i," de ",length(diffSACpj)))
    }
    dtab = dbGetQuery(con,query_insert)
}
#Persona Natural
if(length(diffSACpn)>0){
    id_bandeja <- 34
    estado <- 34
    fecha_bandeja <- format(Sys.time(), "%Y-%m-%d")
    query_insert = "SET search_path TO alert_sch; "
    i = 0
    for (i_s in diff_sac_tmp_pn) {
        i = i + 1
        query_insert = paste0(query_insert,"INSERT INTO alert_sch.alert_ape_se_siniestro_bandeja VALUES(",i_s[1],",",i_s[2],",",i_s[3],",0,'",fecha_bandeja,"',",id_bandeja,",CURRENT_TIMESTAMP,",estado,");")
        if((i-(floor(i/10)*10))==0){
            dtab = dbGetQuery(con,query_insert)
            print(paste0("Pg: Tmp Save"))
            query_insert = "SET search_path TO alert_sch; "
        }
        print(paste0(i," de ",length(diffSACpn)))
    }
    dtab = dbGetQuery(con,query_insert)
}
dbDisconnect(con)
print("Pg: OK")

t_fin <- Sys.time()
t_fin
t_run <- t_fin - t_ini
t_run
