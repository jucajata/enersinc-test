
-- 0) Aportes diarios en metros cúbicos de los ríos de la región Antioquia durante el último año

SELECT fechaoperacion, regionhidrologica, nombrerio, aportesmm3 
FROM aportes_bi 
WHERE regionhidrologica = 'ANTIOQUIA' 
AND fechaoperacion >= DATE_TRUNC('day', CURRENT_DATE) - interval '365 day'
ORDER BY fechaoperacion;


-- 1) Aportes totales del sistema durante el último año

SELECT SUM(aportesmm3) AS sum_aportesmm3, SUM(aportesenergia) AS sum_aportesenergiakwh 
FROM aportes_bi 
WHERE fechaoperacion >= DATE_TRUNC('day', CURRENT_DATE) - interval '365 day'; 


-- 2) Reservas del SIN en porcentaje durante el último año.

SELECT AVG(volp) AS reservas_sin_porcentaje_ultimo_año 
FROM reservas_ido 
WHERE fechaoperacion >= DATE_TRUNC('day', CURRENT_DATE) - interval '365 day'
AND nombreembalse = 'Total SIN';


-- 3) Máximo diario del precio de bolsa de los últimos tres meses.

SELECT fechaoperacion, GREATEST(hora1, hora2, hora3, hora4, hora5, hora6, hora7, hora8, 
                                hora9, hora10, hora11, hora12, hora13, hora14, hora15, hora16, 
                                hora17, hora18, hora19, hora20, hora21, hora22, hora23, hora24) AS max_pb_diario 
FROM trsd 
WHERE contenido = 'Precio de Bolsa Nacional, en $/kWh.' 
AND version = 'txf'
AND fechaoperacion >= DATE_TRUNC('month', CURRENT_DATE) - interval '3 month'
ORDER BY fechaoperacion;


-- 4) Mínimo diario del precio de bolsa de los últimos tres meses.

SELECT fechaoperacion, LEAST(hora1, hora2, hora3, hora4, hora5, hora6, hora7, hora8, 
                             hora9, hora10, hora11, hora12, hora13, hora14, hora15, hora16, 
                             hora17, hora18, hora19, hora20, hora21, hora22, hora23, hora24) AS min_pb_diario 
FROM trsd 
WHERE contenido = 'Precio de Bolsa Nacional, en $/kWh.' 
AND version = 'txf'
AND fechaoperacion >= DATE_TRUNC('month', CURRENT_DATE) - interval '3 month'
ORDER BY fechaoperacion;


-- 5) Promedio diario del precio de bolsa de los últimos tres meses.

SELECT fechaoperacion, SUM(hora1 + hora2 + hora3 + hora4 + hora5 + hora6 + hora7 + hora8 + 
                           hora9 + hora10 + hora11 + hora12 + hora13 + hora14 + hora15 + hora16 + 
                           hora17 + hora18 + hora19 + hora20 + hora21 + hora22 + hora23 + hora24)/24 AS avg_pb_diario 
FROM trsd 
WHERE contenido = 'Precio de Bolsa Nacional, en $/kWh.' 
AND version = 'txf'
AND fechaoperacion >= DATE_TRUNC('month', CURRENT_DATE) - interval '3 month'
GROUP BY fechaoperacion;


-- 6) Demanda mensual del SIN durante los últimos tres años.

SELECT DATE_PART('year',fechaoperacion) AS año, 
       DATE_PART('month',fechaoperacion) AS mes, 
       SUM(hora1 + hora2 + hora3 + hora4 + hora5 + hora6 + hora7 + hora8 + 
           hora9 + hora10 + hora11 + hora12 + hora13 + hora14 + hora15 + hora16 + 
           hora17 + hora18 + hora19 + hora20 + hora21 + hora22 + hora23 + hora24) AS demanda_mensual_sin_kwh
FROM trsd 
WHERE contenido='Demandas Sistema, en kWh.'
AND fechaoperacion >= DATE_TRUNC('month', CURRENT_DATE) - interval '36 month'
GROUP BY año, mes
ORDER BY año, mes;


-- 7) Precios de oferta promedio por tecnología de generación durante el último año.

SELECT DISTINCT(tecnologia), AVG(precio_oferta) AS precio_oferta_promedio_$_mwh
FROM (SELECT CAST(subquery2.precio_oferta AS INTEGER), subquery2.fechaoperacion, C.tecnologia
      FROM (SELECT subquery.planta, subquery.precio_oferta, subquery.fechaoperacion, M.codsic_planta
            FROM 
                (SELECT TRIM(planta) AS planta, hora1 AS precio_oferta, fechaoperacion 
                FROM ofei 
                WHERE fechaoperacion >= DATE_TRUNC('month', CURRENT_DATE) - interval '12 month' 
                AND tipo = ' P' 
                ORDER BY fechaoperacion) AS subquery 
            INNER JOIN 
                (SELECT DISTINCT(recurso_ofei), codsic_planta 
                FROM maestra_recurso 
                WHERE recurso_ofei<>'MENORES_COL') AS M
            ON subquery.planta = M.recurso_ofei) AS subquery2
      INNER JOIN
           (SELECT DISTINCT(planta), tecnologia
            FROM capains) AS C
      ON subquery2.codsic_planta = C.planta) AS subquery3
GROUP BY tecnologia
ORDER BY precio_oferta_promedio_$_mwh;


-- 8) Compras y ventas en bolsa por agente durante el último mes.

SELECT agente, 
mctb AS compras_bolsa_kwh,
vctb AS compras_bolsa_$, 
mvtb AS ventas_bolsa_kwh, 
vvtb AS ventas_bolsa_$, 
fechaoperacion 
FROM afac 
WHERE fechaoperacion >= DATE_TRUNC('month', CURRENT_DATE) - interval '1 month' 
ORDER BY agente;


-- 9) Demanda de la última semana y de la misma semana del año anterior.

SELECT *
FROM (SELECT DATE_PART('year',fechaoperacion) AS año, 
             DATE_PART('week',fechaoperacion) AS semana, 
             SUM(hora1 + hora2 + hora3 + hora4 + hora5 + hora6 + hora7 + hora8 + 
                 hora9 + hora10 + hora11 + hora12 + hora13 + hora14 + hora15 + hora16 + 
                 hora17 + hora18 + hora19 + hora20 + hora21 + hora22 + hora23 + hora24) AS demanda_semanal_sin_kwh
      FROM trsd 
      WHERE contenido='Demandas Sistema, en kWh.'
      AND fechaoperacion >= DATE_TRUNC('month', CURRENT_DATE) - interval '12 month'
      GROUP BY año, semana
      ORDER BY año, semana) AS subquery
WHERE semana=(SELECT DATE_PART('week', DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 week'));