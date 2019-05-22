source("funciones.R")
library(tidyr)

myfetch <- function(nombre,base = FALSE){
  
  tbl(con, nombre) 
  
  
}

myfetch1  <- function(nombre){
  # if(unname(Sys.info()["nodename"] == "DESKTOP-LQ3B302") ){
  #   var <-tbl(pool, nombre) 
  
  #}else{
  var <- read_rds(paste0("proyecto/", nombre,".RDS"))
  
  #}
  return(var)
}

productoresTang <- read.csv("Productores.csv")%>%
  rename(Clave_productor = Clave, NobreDb = Nombre)%>%
  filter(Clave_productor != 11)

#productoresTang[productoresTang$Clave_productor == 11,]$Clave_productor <- 1111

productoresReyes <- myfetch1("tbProductores")%>%
  filter(strCan_cel == "NO")%>%
  transmute(Clave_productor = intCla_pro, 
            Nombre = strNom_bre, 
            Apellido_paterno = strApe_pat,
            Apellido_materno = strApe_mat, 
            Telefono_celular = strTel_cel, 
            Telefono_casa = strTel_cas, 
            Direccion = strDir_ecc, 
            Localidad = strLoc_ali,
            Municipio = strMun_ici, 
            Estado = strEst_ado, 
            RFC = strRFC, 
            Nombre_facturacion = strNom_fac, 
            Beneficiario = strNom_ben,
            Correo_contador = strMail_cont, 
            Clabe = strCla_be,
            Nombre_contador = "", 
            Telefono_contador = "",
            Banco = "",
            Cuenta = "")

library(DBI)

con <- DBI::dbConnect(drv=RPostgres::Postgres(),
                      host = "localhost", 
                      dbname = "ALFIN", 
                      user = "postgres", 
                      password = "magadan13", 
                      port = 5432)


productores <- merge(productoresReyes, productoresTang, all.y = TRUE, by = "Clave_productor")


productores_alta <- productores%>%
  select(-Telefono_celular, -Telefono_casa, -Telefono, -Celular, 
         -Beneficiario, -Banco, -Cuenta, - Clabe)

con2 <- DBI::dbConnect(odbc::odbc(),
                       Driver   = "PostgreSQL Unicode(x64)",
                       Server   = "localhost",
                       Database = "ALFIN",
                       UID      = "postgres",
                       PWD      = "magadan13",
                       Port     = 5432)

#productores
dbWriteTable(con2, "productores", productores_alta)

dbExecute(con, 
"ALTER TABLE public.productores
ADD COLUMN \"Id_productor\" serial;")


dbExecute(conn = con, "ALTER TABLE public.productores
          ADD CONSTRAINT \"Id_productor\" PRIMARY KEY (\"Id_productor\")
          ;")

dbExecute(conn = con, "ALTER TABLE public.productores
          ADD CONSTRAINT \"Clave_productor_uni\" UNIQUE (\"Clave_productor\")
          ;")

dbExecute(con,
          "ALTER TABLE public.\"productores\"
          ADD COLUMN \"Usuario_creacion\" character(10) ,
          ADD COLUMN \"Fecha_creacion\" date DEFAULT current_date,
          ADD COLUMN \"Cancelado\" boolean DEFAULT FALSE,
          ADD COLUMN \"Usuario_cancel\" character(10),
          ADD COLUMN \"Fecha_cancel\" date,
          ADD COLUMN \"Motivo_cancel\" character(15);")
#cuentas
cuentas <- productores%>%
  select(Clave_productor, Beneficiario, Banco, Cuenta, Clabe)


dbWriteTable(con2, "productoresCuentas", cuentas)

dbExecute(con, "ALTER TABLE public.\"productoresCuentas\"
          ADD COLUMN \"Id_cuentas\" serial;")

dbExecute(conn = con, "ALTER TABLE public.\"productoresCuentas\"
          ADD CONSTRAINT \"Id_cuentas\" PRIMARY KEY (\"Id_cuentas\")
          ;")


dbExecute(conn = con, "ALTER TABLE public.\"productoresCuentas\"
          ADD CONSTRAINT \"cuentas_fk_productores\" FOREIGN KEY (\"Clave_productor\")
          REFERENCES public.productores (\"Clave_productor\") MATCH FULL
          ON UPDATE CASCADE
          ON DELETE NO ACTION
          NOT VALID;")

dbExecute(con, "CREATE INDEX \"fki_cuentas_fk_productorres\"
          ON public.\"productoresCuentas\"(\"Clave_productor\");")


dbExecute(con,
          "ALTER TABLE public.\"productoresCuentas\"
          ADD COLUMN \"Usuario_creacion\" character(10) ,
          ADD COLUMN \"Fecha_creacion\" date DEFAULT current_date,
          ADD COLUMN \"Cancelado\" boolean DEFAULT FALSE,
          ADD COLUMN \"Usuario_cancel\" character(10),
          ADD COLUMN \"Fecha_cancel\" date,
          ADD COLUMN \"Motivo_cancel\" character(15);")

contactos <- productores%>%
  select( Clave_productor, Telefono_celular, Telefono_casa, Telefono, Celular)%>%
  mutate(Telefono = as.character(Telefono), Celular = as.character(Celular))%>%
  gather("Tipo", "Contacto", -Clave_productor)%>%
  filter(Contacto != "")

dbWriteTable(con2, "productoresContactos", contactos)

dbExecute(con, "ALTER TABLE public.\"productoresContactos\"
          ADD COLUMN \"Id_contactos\" serial;")

dbExecute(conn = con, "ALTER TABLE public.\"productoresContactos\"
          ADD CONSTRAINT \"Id_contactos\" PRIMARY KEY (\"Id_contactos\")
          ;")


dbExecute(conn = con, "ALTER TABLE public.\"productoresContactos\"
          ADD CONSTRAINT \"contactos_fk_productores\" FOREIGN KEY (\"Clave_productor\")
          REFERENCES public.productores (\"Clave_productor\") MATCH FULL
          ON UPDATE CASCADE
          ON DELETE NO ACTION
          NOT VALID;")

dbExecute(con, "CREATE INDEX \"fki_contactos_fk_productores\"
          ON public.\"productoresContactos\"(\"Clave_productor\");")

dbExecute(con,
          "ALTER TABLE public.\"productoresContactos\"
          ADD COLUMN \"Usuario_creacion\" character(10) ,
          ADD COLUMN \"Fecha_creacion\" date DEFAULT current_date,
          ADD COLUMN \"Cancelado\" boolean DEFAULT FALSE,
          ADD COLUMN \"Usuario_cancel\" character(10),
          ADD COLUMN \"Fecha_cancel\" date,
          ADD COLUMN \"Motivo_cancel\" character(15);")

#huertas
huertas <- readRDS("proyecto/tbHuertas.RDS")%>%
  filter(strCan_cel == "NO")%>%
  transmute(Id_huertasDb = intNum_reg, Clave_productor = intCla_pro, Clave_huerta = intCla_hue,
            Nombre = strNom_bre, Alias = strAli_as, Superficie = floSuperficie, 
            Direccion = strDire_ccion, Clave_fruta = intCla_fru, Localidad = strLoc_ali,
            Municipio = strMun_ici, Estado = strEst_ado, Zona = strZon_a, Activo = strAct_ivo)%>%
  filter(Clave_productor %in% productores$Clave_productor)
    

dbWriteTable(con2 , name = "huertas", huertas)

dbExecute(con, "ALTER TABLE public.huertas
          ADD COLUMN \"Id_huertas\" serial;")

dbExecute(con, "ALTER TABLE public.huertas
          ADD CONSTRAINT \"huertas_pk\" PRIMARY KEY (\"Clave_productor\", \"Clave_huerta\")
          ;")


dbExecute(con, "ALTER TABLE public.huertas
          ADD CONSTRAINT \"Huertas_fk_Productores\" FOREIGN KEY (\"Clave_productor\")
          REFERENCES public.productores (\"Clave_productor\") MATCH FULL
          ON UPDATE CASCADE
          ON DELETE CASCADE;")

dbExecute(con, "CREATE INDEX \"fki_Huertas_fk_Productores\"
          ON public.huertas(\"Clave_productor\");")

dbExecute(con,
          "ALTER TABLE public.\"huertas\"
          ADD COLUMN \"Usuario_creacion\" character(10) ,
          ADD COLUMN \"Fecha_creacion\" date DEFAULT current_date,
          ADD COLUMN \"Cancelado\" boolean DEFAULT FALSE,
          ADD COLUMN \"Usuario_cancel\" character(10),
          ADD COLUMN \"Fecha_cancel\" date,
          ADD COLUMN \"Motivo_cancel\" character(15);")
#sectores
sectores <- readRDS("proyecto/tbSectores.RDS")%>%
  filter(strCan_cel == "NO")%>%
  transmute(Id_sectoresDb = intNum_reg, Clave_productor = intCla_pro, Clave_huerta = intCla_hue,
            Clave_sector = intCla_sec, Nombre = strNom_bre, Superficie = floSuper_ficie)%>%
  filter(Clave_productor %in% productores$Clave_productor)


dbWriteTable(con2 , name = "sectores", sectores)


dbExecute(con, "ALTER TABLE public.sectores
          ADD COLUMN \"Id_sectores\" serial;")

dbExecute(con, "ALTER TABLE public.sectores
          ADD CONSTRAINT \"sectores_pk\" PRIMARY KEY (\"Id_sectores\")")

dbExecute(con, "ALTER TABLE public.sectores
          ADD CONSTRAINT sectores_fk_huertas FOREIGN KEY (\"Clave_productor\", \"Clave_huerta\")
          REFERENCES public.huertas (\"Clave_productor\", \"Clave_huerta\") MATCH FULL 
          ON UPDATE CASCADE
          ON DELETE NO ACTION;")

dbExecute(con2, "CREATE INDEX fki_sectores_fk_huertas
          ON public.sectores(\"Clave_productor\");")

dbExecute(con,
          "ALTER TABLE public.\"sectores\"
          ADD COLUMN \"Usuario_creacion\" character(10) ,
          ADD COLUMN \"Fecha_creacion\" date DEFAULT current_date,
          ADD COLUMN \"Cancelado\" boolean DEFAULT FALSE,
          ADD COLUMN \"Usuario_cancel\" character(10),
          ADD COLUMN \"Fecha_cancel\" date,
          ADD COLUMN \"Motivo_cancel\" character(15);")

#fruEmp
dbExecute(con, 
"CREATE TABLE public.\"fruEmp\"
(
  \"Id_fruEmp\" serial,
  \"Clave_productor\" integer NOT NULL,
  \"Fecha_fruEmp\" date DEFAULT current_date NOT NULL,
  \"Folio\" integer,
  \"Id_cliente\" integer,
  PRIMARY KEY (\"Id_fruEmp\"),
  CONSTRAINT \"fruEmp_fk_productores\" FOREIGN KEY (\"Clave_productor\")
  REFERENCES public.productores (\"Clave_productor\") MATCH FULL
  ON UPDATE CASCADE
  ON DELETE NO ACTION
  NOT VALID
)
WITH (
  OIDS = FALSE
);" )

dbExecute(con2, "CREATE INDEX fki_fruEmp_fk_productores
          ON public.\"fruEmp\"(\"Clave_productor\");")



dbExecute(con,
"ALTER TABLE public.\"fruEmp\"
OWNER to postgres;")

dbExecute(con,
          "ALTER TABLE public.\"fruEmp\"
          ADD COLUMN \"Usuario_creacion\" character(10) ,
          ADD COLUMN \"Fecha_creacion\" date DEFAULT current_date,
          ADD COLUMN \"Cancelado\" boolean DEFAULT FALSE,
          ADD COLUMN \"Usuario_cancel\" character(10),
          ADD COLUMN \"Fecha_cancel\" date,
          ADD COLUMN \"Motivo_cancel\" character(15);")

#PRESENTACIONES
presentaciones <- myfetch2("tbPresentaciones")%>%
  filter(strCan_cel == "NO")%>%
  transmute(Clave_presentacion = intCla_pre, 
            Cantidad = intCan_tid,
            Peso = intPes_o,
            Unidad = strUni_med,
            Activo = "I",
            Peso_lbs = floPes_lbs)
            
dbWriteTable(con2,value = presentaciones, name = "presentaciones")             


dbExecute(con,
          "ALTER TABLE public.presentaciones
          ADD COLUMN \"Id_presentaciones\" serial")

dbExecute(con,
          "ALTER TABLE public.presentaciones
          ADD CONSTRAINT \"Id_presentaciones\" PRIMARY KEY (\"Id_presentaciones\")
          ;")

dbExecute(con,
          "ALTER TABLE public.\"presentaciones\"
          ADD COLUMN \"Usuario_creacion\" character(10) ,
          ADD COLUMN \"Fecha_creacion\" date DEFAULT current_date,
          ADD COLUMN \"Cancelado\" boolean DEFAULT FALSE,
          ADD COLUMN \"Usuario_cancel\" character(10),
          ADD COLUMN \"Fecha_cancel\" date,
          ADD COLUMN \"Motivo_cancel\" character(15);")


dbExecute(con,
          "ALTER TABLE public.\"presentaciones\"
OWNER to postgres;")

dbExecute(con,
          "ALTER TABLE public.presentaciones
          ADD CONSTRAINT \"Unique-Clave_presentacion\" UNIQUE (\"Clave_presentacion\")
          ;")

#PRODUCTOS
productos <- myfetch2("tbProductos")%>%
  filter(strCan_cel == "NO")%>%
  transmute(Clave_producto = str_pad(.$strCla_prod, 6, pad = "0"),
            Clave_grupo = intCla_gpo,
            Clave_presentacion = intCla_pre,
            Clave_fruta = intCla_fru,
            Tipo = strTip_prod,
            Activo = strAct_ivo,
            Nombre = strNom_bre) %>%
  #arrange(Id_productos)%>%
  mutate(Activo = "I")



dbWriteTable(con2,value = productos, name = "productos") 


dbExecute(con,
          "ALTER TABLE public.productos
    ADD COLUMN \"Id_productos\" serial;")

dbExecute(con,
          "ALTER TABLE public.productos
    ADD CONSTRAINT productos_pk PRIMARY KEY (\"Id_productos\");")

dbExecute(con,"
ALTER TABLE public.productos
          ADD CONSTRAINT \"Clave_producto-un\" UNIQUE (\"Clave_producto\");")

dbExecute(con, "
          ALTER TABLE public.productos
          ADD CONSTRAINT \"presentaciones-fk-productos\" FOREIGN KEY (\"Clave_presentacion\")
          REFERENCES public.presentaciones (\"Clave_presentacion\") MATCH FULL
          ON UPDATE CASCADE
          ON DELETE NO ACTION;")


dbExecute(con,
          "ALTER TABLE public.\"productos\"
          ADD COLUMN \"Usuario_creacion\" character(10) ,
          ADD COLUMN \"Fecha_creacion\" date DEFAULT current_date,
          ADD COLUMN \"Cancelado\" boolean DEFAULT FALSE,
          ADD COLUMN \"Usuario_cancel\" character(10),
          ADD COLUMN \"Fecha_cancel\" date,
          ADD COLUMN \"Motivo_cancel\" character(15);")







#fruEmp_reg
dbExecute(con,
"CREATE TABLE public.\"fruEmp_reg\"
(
  \"Id_fruEmp_reg\" serial NOT NULL,
  \"Id_fruEmp\" integer NOT NULL,
  \"Clave_producto\" character(6) NOT NULL,
  \"Cantidad\" integer CHECK (\"Cantidad\" > 0) NOT NULL,
  \"Rechazadas\" integer DEFAULT 0,
  \"Calidad\" character(1),
  \"Observaciones\" character(50),
  \"Precio\" numeric CHECK (\"Precio\" > 0),
  \"Pagado\" boolean DEFAULT TRUE,
  \"Trazabilidad\" character(8),
  PRIMARY KEY (\"Id_fruEmp_reg\"),
  CONSTRAINT \"fruEmp-fk-fruEmp_reg\" FOREIGN KEY (\"Id_fruEmp\")
  REFERENCES public.\"fruEmp\" (\"Id_fruEmp\") MATCH FULL
  ON UPDATE CASCADE
  ON DELETE CASCADE,
  CONSTRAINT \"productos-fk-fruEmp_reg\" FOREIGN KEY (\"Clave_producto\")
  REFERENCES public.productos (\"Clave_producto\") MATCH FULL
  ON UPDATE CASCADE
  ON DELETE CASCADE
)
WITH (
  OIDS = FALSE
);")


dbExecute(con,
"ALTER TABLE public.\"fruEmp_reg\"
OWNER to postgres;")


dbExecute(con2, "CREATE INDEX fki_fruEmp_fk_fruEmp_reg
          ON public.\"fruEmp_reg\"(\"Id_fruEmp\");")


dbExecute(con2, "CREATE INDEX fki_productos_fk_fruEmp_reg
          ON public.\"fruEmp_reg\"(\"Clave_producto\");")

dbExecute(con,
          "ALTER TABLE public.\"fruEmp_reg\"
          ADD COLUMN \"Usuario_creacion\" character(10) ,
          ADD COLUMN \"Fecha_creacion\" date DEFAULT current_date,
          ADD COLUMN \"Cancelado\" boolean DEFAULT FALSE,
          ADD COLUMN \"Usuario_cancel\" character(10),
          ADD COLUMN \"Fecha_cancel\" date,
          ADD COLUMN \"Motivo_cancel\" character(15);")

#salidas

dbExecute(con,
          "CREATE TABLE public.salidas
          (
          \"Id_salidas\" serial,
          \"Fecha_salida\" date,
          PRIMARY KEY (\"Id_salidas\")
          )
          WITH (
          OIDS = FALSE
);"
          )

dbExecute(con,
          "ALTER TABLE public.\"salidas\"
OWNER to postgres;")

dbExecute(con,
          "ALTER TABLE public.\"salidas\"
          ADD COLUMN \"Usuario_creacion\" character(10) ,
          ADD COLUMN \"Fecha_creacion\" date DEFAULT current_date,
          ADD COLUMN \"Cancelado\" boolean DEFAULT FALSE,
          ADD COLUMN \"Usuario_cancel\" character(10),
          ADD COLUMN \"Fecha_cancel\" date,
          ADD COLUMN \"Motivo_cancel\" character(15);")


#Clientes

dbExecute(con,
          "
          CREATE TABLE public.clientes
          (
          \"Id_clientes\" serial,
          \"Nombre\" character(15),
          PRIMARY KEY (\"Id_clientes\")
          )
          WITH(
          OIDS = FALSE
          )")


dbExecute(con,
          "ALTER TABLE public.\"clientes\"
OWNER to postgres;")



dbExecute(con,
          "ALTER TABLE public.\"clientes\"
          ADD COLUMN \"Usuario_creacion\" character(10) ,
          ADD COLUMN \"Fecha_creacion\" date DEFAULT current_date,
          ADD COLUMN \"Cancelado\" boolean DEFAULT FALSE,
          ADD COLUMN \"Usuario_cancel\" character(10),
          ADD COLUMN \"Fecha_cancel\" date,
          ADD COLUMN \"Motivo_cancel\" character(15);")



#pallets

dbExecute(con,
          "CREATE TABLE public.pallets
          (
            \"Id_pallets\" serial,
            \"Id_salidas\" integer,
            \"Id_productos\" integer,
            \"Id_clientes\" integer,
            PRIMARY KEY (\"Id_pallets\"),
            CONSTRAINT \"salidas-fk-pallets\" FOREIGN KEY (\"Id_salidas\")
            REFERENCES public.\"salidas\" (\"Id_salidas\") MATCH SIMPLE
            ON UPDATE CASCADE
            ON DELETE NO ACTION,
            CONSTRAINT \"productos-fk-pallets\" FOREIGN KEY (\"Id_productos\")
            REFERENCES public.productos (\"Id_productos\") MATCH FULL
            ON UPDATE CASCADE
            ON DELETE NO ACTION,
            CONSTRAINT \"clientes-fk-pallets\" FOREIGN KEY (\"Id_clientes\")
            REFERENCES public.clientes (\"Id_clientes\") MATCH SIMPLE
            ON UPDATE CASCADE
            ON DELETE NO ACTION
            
)
WITH (
OIDS = FALSE
);")


dbExecute(con2, "CREATE INDEX fki_salidas_fk_pallets
          ON public.\"pallets\"(\"Id_salidas\");")

dbExecute(con2, "CREATE INDEX fki_productos_fk_pallets
          ON public.\"pallets\"(\"Id_productos\");")


dbExecute(con2, "CREATE INDEX fki_clientes_fk_pallets
          ON public.\"pallets\"(\"Id_clientes\");")




dbExecute(con,
          "ALTER TABLE public.\"pallets\"
OWNER to postgres;")



dbExecute(con,
          "ALTER TABLE public.\"pallets\"
          ADD COLUMN \"Usuario_creacion\" character(10) ,
          ADD COLUMN \"Fecha_creacion\" date DEFAULT current_date,
          ADD COLUMN \"Cancelado\" boolean DEFAULT FALSE,
          ADD COLUMN \"Usuario_cancel\" character(10),
          ADD COLUMN \"Fecha_cancel\" date,
          ADD COLUMN \"Motivo_cancel\" character(15);")






#Pallets_reg
dbExecute(con, 
          "CREATE TABLE public.pallets_reg
          (
          \"Id_pallets_reg\" serial,
          \"Id_fruEmp_reg\" integer NOT NULL,
          \"Cantidad\" integer CHECK (\"Cantidad\" > 0) NOT NULL,
          \"Id_pallets\" integer,
          PRIMARY KEY (\"Id_pallets_reg\"),
          CONSTRAINT \"fruEmp_reg-fk-pallets_reg\" FOREIGN KEY (\"Id_fruEmp_reg\")
          REFERENCES public.\"fruEmp_reg\" (\"Id_fruEmp_reg\") MATCH FULL
          ON UPDATE CASCADE
          ON DELETE CASCADE,
          CONSTRAINT \"pallets-fk-pallets_reg\" FOREIGN KEY (\"Id_pallets\")
          REFERENCES public.\"pallets\" (\"Id_pallets\") MATCH SIMPLE
          ON UPDATE CASCADE
          ON DELETE CASCADE
          )
          WITH (
          OIDS = FALSE
          );")

dbExecute(con, "CREATE INDEX fki_fruEmp_reg_fk_pallets_reg
          ON public.\"pallets_reg\"(\"Id_fruEmp_reg\");")


dbExecute(con, "CREATE INDEX fki_pallets_fk_pallets_reg
          ON public.\"pallets_reg\"(\"Id_pallets\");")









dbExecute(con,
          "ALTER TABLE public.\"pallets_reg\"
          ADD COLUMN \"Usuario_creacion\" character(10) ,
          ADD COLUMN \"Fecha_creacion\" date DEFAULT current_date,
          ADD COLUMN \"Cancelado\" boolean DEFAULT FALSE,
          ADD COLUMN \"Usuario_cancel\" character(10),
          ADD COLUMN \"Fecha_cancel\" date,
          ADD COLUMN \"Motivo_cancel\" character(15);")

dbExecute(con, 
          "ALTER TABLE public.pallets
          OWNER to postgres;")
#fruta
fruta <- readRDS("tbFrutas.RDS")%>%
  filter(strCan_cel == "NO")%>%
  transmute(Id_fruta = intNum_reg, 
            Clave_fruta = intCla_fru,
            Nombre = strNom_bre,
            Nombre_corto = strNom_cto,
            Variedad = strVar_ied,
            Variedad_corto = strVar_cto,
            Tipo = strTip_o,
            Tipo_corto = strTip_cto,
            Activo = strAct_ivo)

dbWriteTable(conn = con, name = "fruta", value = fruta) 

dbExecute(con, "ALTER TABLE public.fruta
          ADD CONSTRAINT \"fruta_pk\" PRIMARY KEY (\"Id_fruta\")")


dbExecute(con,"
ALTER TABLE public.fruta
ADD CONSTRAINT \"Clave_fruta-un\" UNIQUE (\"Clave_fruta\");")

dbExecute(con,
          "ALTER TABLE public.\"fruta\"
          ADD COLUMN \"Usuario_creacion\" character(10) ,
          ADD COLUMN \"Fecha_creacion\" date DEFAULT current_date,
          ADD COLUMN \"Cancelado\" boolean DEFAULT FALSE,
          ADD COLUMN \"Usuario_cancel\" character(10),
          ADD COLUMN \"Fecha_cancel\" date,
          ADD COLUMN \"Motivo_cancel\" character(15);")


dbExecute(con, "
          ALTER TABLE public.productos
          ADD CONSTRAINT \"productos-fk-fruta\" FOREIGN KEY (\"Clave_fruta\")
          REFERENCES public.fruta (\"Clave_fruta\") MATCH FULL
          ON UPDATE CASCADE
          ON DELETE NO ACTION;")




