INSERT INTO maos_sch.maos_soat_se_siniestro_usuario
SELECT a_siniestro, nro_siniestro, nro_cuenta, 858, CURRENT_TIMESTAMP, 1 FROM maos_sch.maos_soat_se_siniestro WHERE (a_siniestro, nro_siniestro, nro_cuenta) NOT IN (SELECT a_siniestro, nro_siniestro, nro_cuenta FROM maos_sch.maos_soat_se_siniestro_usuario) AND estado=1 ORDER BY id_reclamante LIMIT 200
