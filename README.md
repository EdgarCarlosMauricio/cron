# Para editar los cron entrar a 172.30.0.27
- sudo su admin
- Colocan el password de admin   y 
- crontab -e


- crontab [ -u usuario ] archivo
- crontab [ -u usuario ] { -l | -r | -e }
- La opción -e se utiliza para editarlo
- La opción -u se utiliza para indicar el crontab de usuario que queremos administrar.
- Sólo root podrá usar la orden crontab con esta opción.





# En /var/www/html/api_R   estan los ETL


# Ejemplos
### 00:01 de cada día del mes, de cada día de la semana
1 0 * * *
### se ejecuta cada 5 minutos.
*/5 * * * *  /home/user/test.pl
### se ejecuta todos los lunes a las 10:30
30 10 * * 1  /home/user/test.pl
### se ejecuta todos los lunes cada media hora
0,30 * * * 1  /home/user/test.pl
### se ejecuta  cada 15 minutos
0,15,30,45 * * * *  /home/user/test.pl
### se ejecuta  cada 15 minutos
*/15 * * * *  /home/user/test.pl
### ejecuta mas de un comando a la vez a laS 9:30PM
30 21 * * *  /home/user/test.pl;wget http://example.com/archivo_a_descargar.loquesea
### Programa el apagado del PC. todos los sábados a las 9:30
30 21 * * 6 /sbin/shutdown -h now
### se ejecuta el script: /home/user/test.pl cada 5 minutos.
*/5 * * * *  /home/user/test.pl

