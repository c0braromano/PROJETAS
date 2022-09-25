WITH uniao AS (
	SELECT icao_aerodromo_origem AS icao_aerodromo, icao_empresa_aerea FROM t_vra
	UNION ALL
	SELECT icao_aerodromo_destino AS icao_aerodromo, icao_empresa_aerea FROM t_vra
), r_origem AS (
	SELECT icao_aerodromo_origem, icao_empresa_aerea, COUNT(DISTINCT(icao_aerodromo_destino)) as rotas_origem
	FROM PROJETO.t_vra
	GROUP BY icao_aerodromo_origem, icao_empresa_aerea
), r_destino AS (
	SELECT icao_aerodromo_destino, icao_empresa_aerea, COUNT(DISTINCT(icao_aerodromo_origem)) as rotas_destino
	FROM PROJETO.t_vra
	GROUP BY icao_aerodromo_destino, icao_empresa_aerea
)
SELECT name, icao_aerodromo, razao_social, icao_empresa_aerea, rotas_origem, rotas_destino, MAX(pousos_decolagens)
FROM (
	SELECT name, icao_aerodromo, razao_social, contagem.icao_empresa_aerea, rotas_origem, rotas_destino, n_voo AS pousos_decolagens
	FROM (
		SELECT icao_aerodromo, icao_empresa_aerea, COUNT(icao_empresa_aerea) AS n_voo
		FROM uniao
		GROUP BY icao_aerodromo, icao_empresa_aerea
		) AS contagem
	JOIN t_airfields ON contagem.icao_aerodromo=t_airfields.icao
	JOIN t_air_cia ON contagem.icao_empresa_aerea=t_air_cia.icao
	JOIN r_origem ON (contagem.icao_aerodromo=r_origem.icao_aerodromo_origem AND contagem.icao_empresa_aerea=r_origem.icao_empresa_aerea)
	JOIN r_destino ON (contagem.icao_aerodromo=r_destino.icao_aerodromo_destino AND contagem.icao_empresa_aerea=r_destino.icao_empresa_aerea)
) AS teste
GROUP BY icao_aerodromo, icao_empresa_aerea
