
CREATE TABLE public.VENDAS_BOTICARIO
(
	ID_MARCA integer,
	MARCA character varying COLLATE pg_catalog."default",
	ID_LINHA integer NOT NULL,
	LINHA character varying COLLATE pg_catalog."default",
	DATA_VENDA character varying COLLATE pg_catalog."default",
	QTD_VENDA INTEGER
   )
WITH (
	OIDS = FALSE
)
TABLESPACE pg_default;

CREATE TABLE public.VENDAS_BOTICARIO_PERIODO
(
	PERIODO_MES_ANO integer,
	QTD_VENDA INTEGER
   )
WITH (
	OIDS = FALSE
)
TABLESPACE pg_default;

CREATE TABLE public.VENDAS_BOTICARIO_MARCA_LINHA
(
	MARCA character varying COLLATE pg_catalog."default",
	LINHA character varying COLLATE pg_catalog."default",
	QTD_VENDA INTEGER

   )
WITH (
	OIDS = FALSE
)
TABLESPACE pg_default;

CREATE TABLE public.VENDAS_BOTICARIO_MARCA_PERIODO
(
	MARCA character varying COLLATE pg_catalog."default",
	PERIODO_MES_ANO integer,
	QTD_VENDA INTEGER

   )
WITH (
	OIDS = FALSE
)
TABLESPACE pg_default;

CREATE TABLE public.VENDAS_BOTICARIO_LINHA_PERIODO
(
	LINHA character varying COLLATE pg_catalog."default",
	PERIODO_MES_ANO integer,
	QTD_VENDA INTEGER

   )
WITH (
	OIDS = FALSE
)
TABLESPACE pg_default;

CREATE TABLE public.BOTICARIO_SENTIMENTOS_TWITTER
(
	DT_TWEET character varying COLLATE pg_catalog."default",
	USUARIO character varying COLLATE pg_catalog."default",
	ORIGEM character varying COLLATE pg_catalog."default",
	TWEET character varying COLLATE pg_catalog."default"
   )
WITH (
	OIDS = FALSE
)
TABLESPACE pg_default;