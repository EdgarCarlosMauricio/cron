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

#---Update fecha_ultimo_documento from ConsultaGeneralBandejas
t_ini <- Sys.time()
t_ini

setwd("/var/www/html/api_R/t/")
source("t_config.R")

#1. PgSql
pg = dbDriver("PostgreSQL")
con = dbConnect(pg, user=innpgusr, password=innpgpwd, host=innpghst, port=innpgprt, dbname=innpgdbn)

q_pg = paste0("SET search_path TO alert_sch; SELECT ASSSC.nro_siniestro||'/'||ASSSC.a_siniestro||'*'||ASSSC.nro_cuenta AS nro_siniestro, ASSSC.fecha_ultimo_documento FROM alert_sch.alert_ape_se_siniestro_estado ASSSC WHERE ASSSC.id_estado=(SELECT max(id_estado) FROM alert_sch.alert_ape_se_siniestro_estado WHERE nro_siniestro=ASSSC.nro_siniestro AND a_siniestro=ASSSC.a_siniestro AND nro_cuenta=ASSSC.nro_cuenta);")
dataAlert = dbGetQuery(con,q_pg)

print("Pg 1: OK")
print(paste("PgTotalCuentasEstado:",nrow(dataAlert)))

#2. SqlSvr
drv <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver",innsslib) 
uat_conn <- dbConnect(drv, innsscon, innssusr, innsspwd)
rm(innsspwd)

alert_data <- dbGetQuery(uat_conn,paste("use vida_gruposis; SELECT DISTINCT NroSiniestro AS nro_siniestro, ISNULL(Fecha_Ultimo_Documento, '1970/01/01') AS fecha_ultimo_documento FROM GrupoSIS..Bandejas_General WHERE equipo='VIDA-APE' AND fecha_bandeja BETWEEN '2000-01-01' AND convert(date,dateadd(day,+1, getdate())) AND NroSiniestro != '';"))
dbDisconnect(uat_conn)
# write.csv(ans_pag, "filename.csv")

print("SqlSrv: OK")
print(paste("SvrTotalCuentasBandeja",nrow(alert_data)))

#3. PgSql
print("Diff: OK")
data_diff_pg = subset(dataAlert, dataAlert$nro_siniestro %in% alert_data$nro_siniestro)
data_diff = subset(alert_data, alert_data$nro_siniestro %in% data_diff_pg$nro_siniestro)
print(paste("CuentasActualizar",nrow(data_diff)))

s_a_c_tmp <- strsplit(as.character(data_diff$nro_siniestro), "/|[*]")

data_diff$nro_siniestro <- sapply(s_a_c_tmp, "[[" , 1)
data_diff$a_siniestro <- sapply(s_a_c_tmp, "[[", 2)
data_diff$nro_cuenta <- sapply(s_a_c_tmp, "[[", 3)

f_u_d_tmp <- strsplit(as.character(data_diff$fecha_ultimo_documento), "/")

data_diff$f_u_d_dia <- sapply(f_u_d_tmp, "[[" , 1)
data_diff$f_u_d_mes <- sapply(f_u_d_tmp, "[[", 2)
data_diff$f_u_d_year <- sapply(f_u_d_tmp, "[[", 3)

#Generate update
if(nrow(data_diff) > 0){
    print("Pg: FechaUltimoDocumento Save - Start")
    query_insert = "SET search_path TO alert_sch; "
    for(i in 1:nrow(data_diff)){
        print(paste0(i," de ",nrow(data_diff)))
        query_insert = paste0(query_insert,"UPDATE alert_sch.alert_ape_se_siniestro_estado SET fecha_ultimo_documento='",data_diff$f_u_d_year[i],"-",data_diff$f_u_d_mes[i],"-",data_diff$f_u_d_dia[i],"', fecha_registro=CURRENT_TIMESTAMP WHERE (nro_siniestro, a_siniestro, nro_cuenta, id_estado) = (SELECT A.nro_siniestro, A.a_siniestro, A.nro_cuenta, A.id_estado FROM alert_sch.alert_ape_se_siniestro_estado A WHERE A.nro_siniestro=",data_diff$nro_siniestro[i]," AND A.a_siniestro=",data_diff$a_siniestro[i]," AND A.nro_cuenta=",data_diff$nro_cuenta[i]," AND id_estado=(SELECT max(id_estado) FROM alert_sch.alert_ape_se_siniestro_estado WHERE nro_siniestro=A.nro_siniestro AND a_siniestro=A.a_siniestro AND nro_cuenta=A.nro_cuenta) AND A.fecha_ultimo_documento<'",data_diff$f_u_d_year[i],"-",data_diff$f_u_d_mes[i],"-",data_diff$f_u_d_dia[i],"');")

        if((i-(floor(i/100)*100))==0){
            dtab = dbGetQuery(con,query_insert)
            print(paste0("Pg: FechaUltimoDocumento Save"))
            query_insert = "SET search_path TO alert_sch; "
            print(paste0(">>Grupo que termina en: ",i))
        }
    }
    dtab = dbGetQuery(con,query_insert)
    print(paste0("Pg: FechaUltimoDocumento Save"))
    query_insert = "SET search_path TO alert_sch; "
    print(paste0(">>Grupo que termina en: ",i))
    print("Pg: FechaUltimoDocumento Save - End")
}
dbDisconnect(con)

print("Pg 2: OK")

t_fin <- Sys.time()
t_fin
t_run <- t_fin - t_ini
t_run
