source("funciones.R")
library(tidyr)

productoresTang <- read.csv("Productores.csv")%>%
  rename(Clave_productor = Clave, NobreDb = Nombre)%>%
  filter(Clave_productor != 11)

#productoresTang[productoresTang$Clave_productor == 11,]$Clave_productor <- 1111

productoresReyes <- readRDS("tbProductores.RDS")%>%
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


dbWriteTable(con2, "productores", productores_alta)

dbExecute(conn = con, "ALTER TABLE public.productores
    ADD CONSTRAINT \"Clave_productor\" PRIMARY KEY (\"Clave_productor\")
          ;")

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



contactos <- productores%>%
  select(Clave_productor, Telefono_celular, Telefono_casa, Telefono, Celular)%>%
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
          REFERENCES public.productores (\"Clave_productor\") MATCH SIMPLE
          ON UPDATE NO ACTION
          ON DELETE NO ACTION;")

dbExecute(con, "CREATE INDEX \"fki_Huertas_fk_Productores\"
    ON public.huertas(\"Clave_productor\");")

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
    REFERENCES public.huertas (\"Clave_productor\", \"Clave_huerta\") 
    ON UPDATE CASCADE
    ON DELETE NO ACTION;")

dbExecute(con2, "CREATE INDEX fki_sectores_fk_huertas
    ON public.sectores(\"Clave_productor\");")




fruta <- readRDS("tbFrutas.RDS")%>%
  filter(strCan_cel == "NO")%>%
  transmute(Id_fruta = intNum_reg, 
            Clave_fruta = intCla_fru,
            Nombre = strNom_bre,
            Nombre_corto = strNom_cto,
            Variedad = strVar_ied,
            Variedad_corto = strVar_cto,
            Tipo = strTip_o,
            Activo = strAct_ivo)

dbWriteTable(conn = con, name = "fruta", value = fruta) 

dbExecute(con, "ALTER TABLE public.fruta
          ADD CONSTRAINT \"fruta_pk\" PRIMARY KEY (\"Id_fruta\")")


dbExecute(con, "CREATE TABLE public.\"frutaEmp\"
(
  \"Id_fruEmp_reg\" serial NOT NULL,
  \"Folio\" integer NOT NULL,
  \"Clave_productor\" integer NOT NULL,
  \"Fecha\" date NOT NULL,
  \"Fecha_creacion\" timestamp without time zone NOT NULL,
  PRIMARY KEY (\"Id_fruEmp_reg\"),
  CONSTRAINT \"uni_Folio_fruEmp\" UNIQUE (\"Folio\")
  ,
  CONSTRAINT \"fruEmpp_fk_productores\" FOREIGN KEY (\"Clave_productor\")
  REFERENCES public.productores (\"Clave_productor\") MATCH FULL
  ON UPDATE CASCADE
  ON DELETE NO ACTION
  NOT VALID
)
          WITH (
          OIDS = FALSE
          )
          TABLESPACE pg_default;")

dbExecute(con, "ALTER TABLE public.\"frutaEmp\"
    OWNER to postgres;")







frutaEmpaque <- data.frame(
  
  Clave_productor = character(),
  Fecha = date(),
  Fecha_creacion = .POSIXct(character(0))
)

frutaEmpaqueReg <- data.frame(

  Folio = integer(),
  Clave_producto = character(),
  Cantidad = integer(),
  Rechazadas = integer(),
  Fecha_creacion = as.POSIXct()
)


