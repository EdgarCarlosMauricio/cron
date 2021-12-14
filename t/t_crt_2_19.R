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

#---CONSULTA GENERAL BANDEJAS: siniestro-year-cuenta (s_a_c) registrados ausentes en BandejaGeneral y sin ninguna bandeja
#----Caso-Bandeja: 23 (sinRol+sinBandeja)
t_ini <- Sys.time()
t_ini

#1. PgSql
pg = dbDriver("PostgreSQL")
con = dbConnect(pg, user="sis_bigdata", password="F4llst4ck2020*", host="localhost", port=5432, dbname="sis_analytics")

q_pg = paste0("SET search_path TO alert_sch; SELECT ASSS.nro_siniestro||'/'||ASSS.a_siniestro||'*'||ASSS.nro_cuenta AS nro_siniestro FROM alert_sch.alert_soat_se_siniestro ASSS WHERE NOT EXISTS (SELECT * FROM alert_sch.alert_soat_se_siniestro_bandeja ASSSB WHERE ASSS.nro_siniestro=ASSSB.nro_siniestro AND ASSS.a_siniestro=ASSSB.a_siniestro AND ASSS.nro_cuenta=ASSSB.nro_cuenta);")
dataAlert = dbGetQuery(con,q_pg)  
print("Pg 1: OK")
print(nrow(dataAlert))

#2. SqlSvr
pw <- {"F8FM2h8foL*"}

drv <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver","/home/admin/lib_R/sqljdbc_7.2/enu/mssql-jdbc-7.2.1.jre8.jar") 
uat_conn <- dbConnect(drv, "jdbc:sqlserver://172.30.25.11;databaseName=soat_gruposis", "Bigdata", pw)
rm(pw)

alert_data <- dbGetQuery(uat_conn,paste("USE soat_gruposis; SELECT BG.NroSiniestro AS nro_siniestro FROM GrupoSIS..Bandejas_General BG WITH (NOLOCK) WHERE BG.equipo='SOAT-SE' AND BG.fecha_bandeja BETWEEN '2000-01-01' AND convert(date,dateadd(day,1, getdate()));"))

print("SqlSrv: In BandejasGeneral")
print(nrow(alert_data))
dbDisconnect(uat_conn)

print("SqlSvr: OK")

#3. PgSql
diffSAC = dataAlert$nro_siniestro[!dataAlert$nro_siniestro %in% alert_data$nro_siniestro]
diff_sac_tmp <- strsplit(as.character(diffSAC), "/|[*]")
print("Diff: OK")
print(length(diffSAC))

#Generate insert
if(length(diffSAC)>0){
    id_bandeja <- 23
    estado <- 23
    fecha_bandeja <- format(Sys.time(), "%Y-%m-%d")
    query_insert = "SET search_path TO alert_sch; "
    i = 0
    for (i_s in diff_sac_tmp) {
        i = i + 1
        query_insert = paste0(query_insert,"INSERT INTO alert_sch.alert_soat_se_siniestro_bandeja VALUES(",i_s[1],",",i_s[2],",",i_s[3],",0,'",fecha_bandeja,"',",id_bandeja,",CURRENT_TIMESTAMP,",estado,");")
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
print("Pg: OK")

t_fin <- Sys.time()
t_fin
t_run <- t_fin - t_ini
t_run
