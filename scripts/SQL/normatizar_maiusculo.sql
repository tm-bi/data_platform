
Transformar coluna para letras maiúscula

update s_novaxs_182 
set status = upper(status)
where status <> upper(status)
	or status is not null;