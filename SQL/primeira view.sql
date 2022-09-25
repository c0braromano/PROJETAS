CREATE VIEW primeira_view AS (
    WITH contagem AS (
	SELECT CONCAT(icao_aerodromo_origem, '-', icao_aerodromo_destino ) as origem_destino, 
			COUNT(icao_empresa_aerea) as cont_voo, 
			icao_empresa_aerea,
			icao_aerodromo_origem,
			icao_aerodromo_destino
			FROM PROJETO.t_vra
			GROUP BY origem_destino, icao_empresa_aerea
			ORDER BY icao_empresa_aerea, cont_voo
	)

	SELECT razao_social, MAX(cont_voo) AS cont_voo, icao_aerodromo_origem,  icao_aerodromo_destino, 
			o_airfields.state as estado_origem, d_airfields.state as estado_destino,
			o_airfields.name as aeroporto_origem, d_airfields.name as aeroporto_destino
	FROM contagem
	JOIN t_air_cia ON contagem.icao_empresa_aerea=t_air_cia.icao
	JOIN t_airfields as o_airfields ON contagem.icao_aerodromo_origem = o_airfields.icao
	JOIN t_airfields as d_airfields ON contagem.icao_aerodromo_destino = d_airfields.icao
	GROUP BY icao_empresa_aerea
)