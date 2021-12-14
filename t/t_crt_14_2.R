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
#-- Check
t_ini <- Sys.time()
t_ini


setwd("/var/www/html/api_R/t/")
source("t_config.R")


#1. SqlSvr
print("SqlSvr: START")
drv <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver",innsslib) 
uat_conn <- dbConnect(drv, innsscon, innssusr, innsspwd)
rm(innsspwd)

sac_filter = "1025/2021*2"
#sac_filter = "3668/2019*2" #Cuenta fantasma: 9984/2007*1
print(paste0("SAC: ",sac_filter))
query_cuentas_srv = paste0("use vida_gruposis; SELECT nro_siniestro, a_siniestro, nro_cuenta, codigo_objecion, codigo_usuario_radica, codigo_sucursal_Recepcion, codigo_sucursal_Radica, codigo_sucursal_Procesa, codigo_sucursal_Paga, codigo_sucursal_Egreso, codigo_estado_cuenta, identificacion_reclamante, ciudad_reclamante, fecha_aviso, fecha_limite_pago, fecha_objecion, fecha_prescripcion, fecha_factura,fecha_radicacion, fecha_uactualiza, fecha_respuesta, codigo_usuario_actualiza, nro_factura, nro_cuenta_cobro, valor_cobrado, codigo_recepcion, Autorizacion_Coordinador, Terminada, EquipoRA, fecha_muerte, fecha_ingreso, fecha_egreso, metodo_pago, nro_siniestro_sise, revisada_SE, usuario_SE, fecha_revision_SE, nro_autorizacion, nro_amparo_cliente, fecha_aviso_cliente, id_direccionamiento, codigo_fami AllowedUsers, PersistentActivity, AllowedRoles, InstanceId, EventName, isOdontologia, IdCausalNovedad, Nro_Nota_Credito, Valor_Nota_Credito, Fecha_Nota_Credito FROM vida_gruposis..cuentas WHERE CAST(nro_siniestro AS varchar)+'/'+CAST(a_siniestro AS varchar)+'*'+CAST(nro_cuenta AS varchar)='",sac_filter,"';")
data_cuentas_srv <- dbGetQuery(uat_conn,query_cuentas_srv)
print("Cuentas")
print(data_cuentas_srv)

query_sac_srv = paste0("use vida_gruposis; SELECT [Nombre Bandeja] AS 'nombre_bandeja', Rol AS rol, paquete, estado_paquete, NroSiniestro AS s_a_c, codigo_estado_cuenta AS estado_cuenta, ISNULL (codigo_objecion,'') AS objecion, CONVERT (VARCHAR (10),fecha_bandeja,103) AS fecha_bandeja, Fecha_Ultimo_Documento, codigo_amparo, [usuario que remite a bandeja] AS 'usuario_bandeja', tipo_entidad, metodo_pago, codigo_objecion FROM GrupoSIS..Bandejas_General WHERE equipo = 'VIDA-APE' AND fecha_bandeja BETWEEN '2000-01-01' AND convert(date,dateadd(day,1, getdate())) AND NroSiniestro='",sac_filter,"' ORDER BY [Nombre Bandeja], Rol;")
data_sac_srv <- dbGetQuery(uat_conn,query_sac_srv)
print("BandejasGeneral")
print(data_sac_srv)

dbDisconnect(uat_conn)
print("SqlSvr: END")

#2. PgSql
print("Pg: START")
pg = dbDriver("PostgreSQL")
con = dbConnect(pg, user=innpgusr, password=innpgpwd, host=innpghst, port=innpgprt, dbname=innpgdbn)

query_sac_pg = paste0("SET search_path TO alert_sch; SELECT * FROM alert_sch.alert_ape_se_siniestro A WHERE A.nro_siniestro||'/'||A.a_siniestro||'*'||A.nro_cuenta = '",sac_filter,"';")
data_sac_pg = dbGetQuery(con,query_sac_pg)
print("BaseCuentas")
print(data_sac_pg)

query_bandeja_pg = paste0("SET search_path TO alert_sch; SELECT * FROM alert_sch.alert_ape_se_siniestro_bandeja A WHERE A.nro_siniestro||'/'||A.a_siniestro||'*'||A.nro_cuenta = '",sac_filter,"' ORDER BY A.nro_siniestro ASC, A.a_siniestro ASC, A.nro_cuenta ASC, A.id_siniestro_bandeja ASC;")
data_bandeja_pg = dbGetQuery(con,query_bandeja_pg)
print("Bandejas")
print(data_bandeja_pg)

query_estado_pg = paste0("SET search_path TO alert_sch; SELECT B.clave, A.* FROM alert_sch.alert_ape_se_siniestro_estado A INNER JOIN alert_sch.estado_cuenta B ON A.id_estado_cuenta=B.id_estado_cuenta WHERE A.nro_siniestro||'/'||A.a_siniestro||'*'||A.nro_cuenta = '",sac_filter,"';")
data_estado_pg = dbGetQuery(con,query_estado_pg)
print("Estados")
print(data_estado_pg)

dbDisconnect(con)
print("Pg: END")

t_fin <- Sys.time()
t_fin
t_run <- t_fin - t_ini
t_run
