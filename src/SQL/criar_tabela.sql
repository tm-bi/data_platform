--Código para criar uma tabela no banco de dados

create table dim_produto(
	id_produto		BIGSERIAL	primary key,
	produto			TEXT		not null,
	tipo_produto	TEXT		null, --ingresso, A&B, locação_espaços, estacionamento, armários e serviços (foto, tirolesa)
	categoria		TEXT		null, --day-use, B2B, aquacard, convênios, hotel, cortesias, vem de novo, eventos
	infantil		TEXT		null, --sim se infantil e não para outros	
	pax				BIGINT		null,
	dt_carga		timestamp 	not null default CURRENT_TIMESTAMP
	);