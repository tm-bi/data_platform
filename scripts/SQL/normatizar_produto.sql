--Código para normatizar letas maiúsculas na coluna produto
update "_silver-transacional".s_dim_produto
set produto = upper (produto)
where produto is not null
	and produto <> upper(produto);