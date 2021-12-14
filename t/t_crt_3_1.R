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

#1. PgSql
pg = dbDriver("PostgreSQL")
con = dbConnect(pg, user="sis_bigdata", password="F4llst4ck2020*", host="localhost", port=5432, dbname="sis_analytics")

q_pg = paste0("SET search_path TO alert_sch; SELECT A.nro_siniestro, A.a_siniestro, A.nro_cuenta, '1970-01-01' AS fecha_bandeja FROM alert_sch.alert_soat_se_siniestro A WHERE not exists (SELECT * FROM alert_sch.alert_soat_se_siniestro_bandeja B WHERE A.nro_siniestro=B.nro_siniestro AND A.a_siniestro=B.a_siniestro AND A.nro_cuenta=B.nro_cuenta);")
dataAlert = dbGetQuery(con,q_pg)

print("Pg 1: OK")
print(nrow(dataAlert))
filter_asc = ""

for(i in 1:nrow(dataAlert)){
    filter_asc = paste0(filter_asc," OR (NroSiniestro='",dataAlert$nro_siniestro[i],"/",dataAlert$a_siniestro[i],"*",dataAlert$nro_cuenta[i],"')")
}

#2. SqlSvr
pw <- {"F8FM2h8foL*"}

drv <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver","/home/admin/lib_R/sqljdbc_7.2/enu/mssql-jdbc-7.2.1.jre8.jar") 
uat_conn <- dbConnect(drv, "jdbc:sqlserver://172.30.25.11;databaseName=soat_gruposis", "Bigdata", pw)
rm(pw)

# NroSiniestro AS nro_siniestro, fecha_bandeja
# NroSiniestro AS nro_siniestro, fecha_bandeja, [Nombre Bandeja] AS nombre_bandeja, Rol
#alert_data <- dbGetQuery(uat_conn,paste("use soat_gruposis; SELECT NroSiniestro AS nro_siniestro, fecha_bandeja FROM GrupoSIS..Bandejas_General WHERE equipo='SOAT-SE' AND (",substring(filter_asc, 4),");"))
#  AND [Nombre Bandeja]='AnalisisLiquidacion' AND Rol='Analista'
# SELECT [Nombre Bandeja] AS 'nombre_bandeja', Rol AS rol, paquete, estado_paquete, NroSiniestro AS s_a_c, codigo_estado_cuenta AS estado_cuenta, ISNULL (codigo_objecion,'') AS objecion, CONVERT (VARCHAR (10),fecha_bandeja,103) AS fecha_bandeja, codigo_amparo, [usuario que remite a bandeja] AS 'usuario_bandeja' FROM GrupoSIS..Bandejas_General WHERE equipo = 'SOAT-SE' AND fecha_bandeja BETWEEN '2020-01-01' AND convert(date,dateadd(day,-1, getdate())) AND ROL != '' ORDER BY [Nombre Bandeja], Rol
dbDisconnect(uat_conn)

#Split: s_a_c
#s_a_c_tmp <- strsplit(as.character(alert_data$nro_siniestro), "/|[*]")

#alert_data$nro_siniestro <- sapply(s_a_c_tmp, "[[" , 1)
#alert_data$a_siniestro <- sapply(s_a_c_tmp, "[[", 2)
#alert_data$nro_cuenta <- sapply(s_a_c_tmp, "[[", 3)
#print(alert_data)
#print(nrow(alert_data))

#print("SqlSvr: OK")

#3. PgSql
dataAlert$id_siniestro_bandeja <- 0
dataAlert$id_bandeja <- 23
dataAlert$estado <- 99
dataAlert$fecha_registro  <- Sys.time() 
dtab = dbGetQuery(con,"SET search_path TO alert_sch;")
dbWriteTable(con, "alert_soat_se_siniestro_bandeja", dataAlert, row.names=FALSE, append=TRUE)
dbDisconnect(con)
print("Pg: OK")

#print(alert_data)

t_fin <- Sys.time()
t_fin
t_run <- t_fin - t_ini
t_run
