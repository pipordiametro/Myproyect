con <- DBI::dbConnect(drv=RPostgres::Postgres(),
                      host = "localhost", 
                      dbname = "ALFIN", 
                      user = "postgres", 
                      password = "magadan13", 
                      port = 5432)


dbExecute(con,
          "CREATE TABLE public.pedidos
          (
          \"Id_pedidos\" serial,
          \"Clave_pedido\" character(15) UNIQUE NOT NULL,
          \"Caducidad\" date NOT NULL,
          PRIMARY KEY (\"Id_pedidos\")
          )
WITH (
  OIDS = FALSE
);")

dbExecute(con,
          "ALTER TABLE public.pedidos
OWNER to postgres;")

dbExecute(con,
          "ALTER TABLE public.pedidos
          ADD COLUMN \"Usuario_creacion\" character(10) ,
          ADD COLUMN \"Fecha_creacion\" date DEFAULT current_date,
          ADD COLUMN \"Cancelado\" boolean DEFAULT FALSE,
          ADD COLUMN \"Usuario_cancel\" character(10),
          ADD COLUMN \"Fecha_cancel\" date,
          ADD COLUMN \"Motivo_cancel\" character(15);")


alias <- read_rds("alias.RDS")%>%
  mutate(Activo = "A")

dbWriteTable(con, "alias", alias%>%
               filter(Clave_producto != 450210))

dbExecute(con,
          "ALTER TABLE public.\"alias\"
           ADD COLUMN \"Id_alias\" serial")

dbExecute(con,
          "ALTER TABLE public.alias
          ADD CONSTRAINT \"Id_alias\" PRIMARY KEY (\"Id_alias\");")

dbExecute(con,
          "ALTER TABLE public.alias
          ADD CONSTRAINT \"Alias-un\" UNIQUE (\"Alias\");")

dbExecute(con,
          "ALTER TABLE public.alias
OWNER to postgres;")

dbExecute(con,
          "ALTER TABLE public.alias
          ADD COLUMN \"Usuario_creacion\" character(10) ,
          ADD COLUMN \"Fecha_creacion\" date DEFAULT current_date,
          ADD COLUMN \"Cancelado\" boolean DEFAULT FALSE,
          ADD COLUMN \"Usuario_cancel\" character(10),
          ADD COLUMN \"Fecha_cancel\" date,
          ADD COLUMN \"Motivo_cancel\" character(15);")

dbExecute(con,
          "CREATE TABLE public.\"pedidosReg\"
(
\"Id_pedidosReg\" serial,
\"Id_alias\" integer NOT NULL,
\"Id_pedidos\" integer NOT NULL,
\"Cantidad\" integer NOT NULL check (\"Cantidad\" > 0),
CONSTRAINT \"Id_pedidosReg\" PRIMARY KEY (\"Id_pedidosReg\"),
CONSTRAINT \"alias-fk-pedidosReg\" FOREIGN KEY (\"Id_alias\")
REFERENCES public.alias (\"Id_alias\") MATCH FULL
ON UPDATE CASCADE
ON DELETE NO ACTION,
CONSTRAINT \"pedidos-fk-pedidosReg\" FOREIGN KEY (\"Id_pedidos\")
REFERENCES public.pedidos (\"Id_pedidos\") MATCH FULL
ON UPDATE CASCADE
ON DELETE NO ACTION
)
WITH (
OIDS = FALSE
);")


dbExecute(con,
          "ALTER TABLE public.\"pedidosReg\"
OWNER to postgres;")

dbExecute(con,
          "ALTER TABLE public.\"pedidosReg\"
          ADD COLUMN \"Usuario_creacion\" character(10) ,
          ADD COLUMN \"Fecha_creacion\" date DEFAULT current_date,
          ADD COLUMN \"Cancelado\" boolean DEFAULT FALSE,
          ADD COLUMN \"Usuario_cancel\" character(10),
          ADD COLUMN \"Fecha_cancel\" date,
          ADD COLUMN \"Motivo_cancel\" character(15);")


#asignadas
dbExecute(con,
          "CREATE TABLE public.asignaciones
(
\"Id_asignaciones\" serial,
\"Id_pedidosReg\" integer NOT NULL,
\"Clave_productor\" integer NOT NULL,
\"Asignadas\" integer CHECK (\"Asignadas\" > 0),
CONSTRAINT \"Id_asignaciones\" PRIMARY KEY (\"Id_asignaciones\"),
CONSTRAINT \"pedidosReg-fk-asignaciones\" FOREIGN KEY (\"Id_pedidosReg\")
REFERENCES public.\"pedidosReg\" (\"Id_pedidosReg\") MATCH FULL
ON UPDATE CASCADE
ON DELETE CASCADE
)
WITH
(
OIDS = FALSE
);")

          dbExecute(con,
                    "ALTER TABLE public.\"asignaciones\"
OWNER to postgres;")
          
          dbExecute(con,
                    "ALTER TABLE public.\"asignaciones\"
          ADD COLUMN \"Usuario_creacion\" character(10) ,
          ADD COLUMN \"Fecha_creacion\" date DEFAULT current_date,
          ADD COLUMN \"Cancelado\" boolean DEFAULT FALSE,
          ADD COLUMN \"Usuario_cancel\" character(10),
          ADD COLUMN \"Fecha_cancel\" date,
          ADD COLUMN \"Motivo_cancel\" character(15);")

