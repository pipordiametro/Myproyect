library(RMySQL)
library(DBI)
library(plyr)
library(dplyr)
library(dbplyr)

tiempos <- function(df, Campos = c("Semana", "Semanats", "Year", "Temporada")){
  
    df_mod <- df%>%
      mutate(Fecha = as.Date(Fecha),
             Semana = as.integer(format(Fecha, "%U")),
             Year = as.integer(format(Fecha , "%Y")),
             Year = ifelse(Semana == 0, Year - 1, Year),
             Semana = ifelse(Semana %in% c(0,53), 52, Semana),
             Semanats = (Year - 2010)*52 + as.integer(Semana),
             Semana = factor(Semana, levels = (c(34:85)%%52 + 1), ordered = TRUE),
             Temporada = ifelse((Fecha >= as.Date("2013-09-01") & Fecha < as.Date("2014-09-01")), "2013-2014",
                                ifelse((Fecha >= as.Date("2014-09-01") & Fecha < as.Date("2015-09-01")), "2014-2015",
                                       ifelse((Fecha >= as.Date("2015-09-01") & Fecha < as.Date("2016-09-01")), "2015-2016",
                                              ifelse((Fecha >= as.Date("2016-09-01") & Fecha < as.Date("2017-09-01")), "2016-2017",
                                                     ifelse((Fecha >= as.Date("2017-09-01") & Fecha < as.Date("2018-09-01")), "2017-2018",
                                                            ifelse((Fecha >= as.Date("2018-09-01") & Fecha < as.Date("2019-09-01")), "2018-2019",NA)))))),
             Temporada = factor(Temporada, levels = c("2013-2014", "2014-2015", "2015-2016", "2016-2017", 
                                                      "2017-2018", "2018-2019")))
    

}



mydb <- dbConnect(MySQL(), 
                  user='francisco', 
                  password='francisco', 
                  dbname='alpasaAcopios', 
                  host='alpasafarms.com',
                  port = 3306)

camposMod <- c("usuario_creo", "fecha_creo", "usuario_modifico",
               "fecha_modifico", "usuario_elimino", "fecha_elimino", "cancelado", "exportado" )


entradas_fruta <- left_join(tbl(mydb, "recepcion_fruta")%>%
                              filter(cancelado == "NO")%>%
                              select( -one_of(camposMod)), 
                            tbl(mydb, "recepcion_fruta_det")%>%
                              filter(cancelado == "NO")%>%
                              select(-id, -one_of(camposMod)) , 
                            by = c("id" = "id_recepcion"))%>%
  filter(clave_acopio == 4)%>%
  collect()%>%
  mutate(fecha = as.Date(fecha))
 



entradas_mat <- left_join(tbl(mydb,"almacen_entrada")%>%
                            filter(cancelado == "NO",
                                   clave_acopio == 4)%>%
                            select(-one_of(camposMod)), 
                          tbl(mydb,"almacen_entrada_det")%>%
                            filter(cancelado == "NO")%>%
                            select(-id, -one_of(camposMod)), 
                          by = c("id" = "id_entrada"))%>%
  collect()

salidas_mat <- left_join(tbl(mydb, "almacen_salida")%>%
                           filter(cancelado == "NO",
                                  clave_acopio == 4)%>%
                           select(-one_of(camposMod)),
                         tbl(mydb, "almacen_salida_det")%>%
                           filter(cancelado == "NO")%>%
                           select(-id, -one_of(camposMod)),
                         by = c("id" = "id_salida"))%>%
  collect()






#relacion <- entradas_fruta%>%
#  filter(clave_producto != "010204",
#         fecha < as.Date("2019-04-04"),
#         fecha > as.Date("2019-03-20"))%>%
#  left_join(tbl(mydb, "productos")%>%
#              select(clave_producto, nombre, clave_fruta)%>%
#            collect(), 
#            by = c("clave_producto" = "clave_producto"))%>%
#  filter(clave_fruta == 1)%>%
#  ddply(.(fecha, clave_productor, clave_producto, nombre), summarize,
#        Total = sum(cantidad_aceptada))%>%
#  left_join(tbl(mydb, "productos")%>%
#              select(clave_producto, nombre)%>%
#              collect(), 
#            by = c("clave_producto" = "clave_producto"))

#sum(relacion$Total)
 

ggplot(entradas_fruta%>%
         rename(Fecha = fecha)%>%
         tiempos("Semana")%>%
         ddply(.(Semana), summarize, Total = sum(cantidad_aceptada)), 
       aes(x = Semana, y  = Total)) + geom_point()

domingo <- entradas_fruta%>%
  filter(fecha == as.Date("2019-04-14"))%>%
  left_join(tbl(mydb, "productos")%>%
                            select(clave_producto, nombre, clave_fruta)%>%
                          collect(), 
                          by = c("clave_producto" = "clave_producto"))%>%
                #filter(clave_fruta == 1)%>%
                #ddply(.(fecha, clave_productor, clave_producto, nombre), summarize,
                 #     Total = sum(cantidad_aceptada))%>%
                left_join(tbl(mydb, "productos")%>%
                            select(clave_producto, nombre)%>%
                            collect(), 
                          by = c("clave_producto" = "clave_producto"))%>%
  #filter(clave_productor == 581)%>%
  select(id, clave_productor, fecha, clave_producto, cantidad_aceptada, nombre.x, clave_fruta)
              
dbDisconnect(mydb)

prueba <- domingo%>%
  ddply(.(id, fecha, clave_productor, clave_producto, nombre.x, clave_fruta), summarize, Total = sum(cantidad_aceptada))
write.csv(prueba, "beto.csv")

entradasPag <- function(desde = Sys.Date(), hasta = Sys.Date(), Clave_productor = NULL){
  
  library(RMySQL)
  library(DBI)
  library(plyr)
  library(dplyr)
  library(dbplyr)
  
  
  
  mydb <- dbConnect(MySQL(), 
                    user='francisco', 
                    password='francisco', 
                    dbname='alpasaAcopios', 
                      host='alpasafarms.com',
                    port = 3306)
  
  camposMod <- c("usuario_creo", "fecha_creacion", "fecha_creo, usuario_modifico",
                 "fecha_modifico", "usuario_elimino", "fecha_elimino", "cancelado", "exportado" )
  
  entradas_fruta <- left_join(tbl(mydb, "recepcion_fruta")%>%
                                filter(cancelado == "NO")%>%
                                select( -one_of(camposMod)), 
                              tbl(mydb, "recepcion_fruta_det")%>%
                                filter(cancelado == "NO")%>%
                                select(-id, -one_of(camposMod)) , 
                              by = c("id" = "id_recepcion"))%>%
    filter(clave_acopio == 4)%>%
    collect()%>%
    mutate(fecha = as.Date(fecha))%>%
    filter(fecha >= desde,
           fecha <= hasta)%>%
    left_join(tbl(mydb, "productos")%>%
                select(clave_producto, nombre, clave_fruta)%>%
                collect(), 
              by = c("clave_producto" = "clave_producto"))%>%
    #filter(clave_fruta == 1)%>%
    #ddply(.(fecha, clave_productor, clave_producto, nombre), summarize,
    #     Total = sum(cantidad_aceptada))%>%
    left_join(tbl(mydb, "productos")%>%
                select(clave_producto, nombre)%>%
                collect(), 
              by = c("clave_producto" = "clave_producto"))%>%
    #filter(clave_productor == 581)%>%
    select(id, clave_productor, fecha, clave_producto, cantidad_aceptada, nombre.x, clave_fruta)
  
  
  dbDisconnect(mydb)
  
  entradas_fruta
  
}

prueba <- entradasPag(desde = as.Date("2019-04-17"), hasta = as.Date("2019-04-21"))%>%
  filter(clave_productor == 581)%>%
  select(cantidad_aceptada)%>%
  mutate(cantidad_aceptada = as.numeric(cantidad_aceptada))%>%
    sum()
