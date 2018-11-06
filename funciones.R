library(RODBC)
library(plyr)
library(dplyr)
library(tidyr)
library(DBI)
library(odbc)
#library(pool)

#pool <- dbPool(drv = odbc::odbc(),
#               dsn = "SQLProyecto08",  uid = "francisco", pwd = "Alpasa2017")

inicio_temporada <- as.Date("2018-09-01")
con <- dbConnect(odbc::odbc(), "SQLProyecto08", uid = "francisco", pwd = "Alpasa2017")


myfetch <- function(nombre){
 # if(unname(Sys.info()["nodename"] == "DESKTOP-LQ3B302") ){
  #  con <- dbConnect(odbc::odbc(), "SQLProyecto08", uid = "francisco", pwd = "Alpasa2017")
    var <- tbl(con, nombre) 
 #   odbcClose(con)
#    write.csv(var, paste0("proyecto/", nombre, ".csv"))
 # }else{
  #  var <- read.csv(paste0("proyecto/", nombre,".csv"))
    
#  }
  return(var)
}



myfetch3 <- function(nombre){
  if(unname(Sys.info()["nodename"] == "DESKTOP-LQ3B302") ){
    var <-tbl(pool, nombre) 
  
  }else{
    var <- read.csv(paste0("proyecto/", nombre,".csv"))
    
  }
  return(var)
  
}





myfetch2 <- function(nombre,base = FALSE){
  if(unname(Sys.info()["nodename"] == "DESKTOP-LQ3B302") ){
    con <- odbcConnect(dsn = "SQLProyecto08", uid = "francisco", pwd = "Alpasa2017")
    var <- sqlFetch(con, nombre, as.is = TRUE) 
    odbcClose(con)
   # write.csv(var, paste0("proyecto/", nombre, ".csv"))
    }else{
      var <- read.csv(paste0("proyecto/", nombre,".csv"))
   
  }
  return(var)
}

myfetch6 <- function(nombre,base = FALSE){
     var <- read.csv(paste0("proyecto/", nombre,".csv"))
    
  return(var)
}

get.precios <- function(){
  library(RCurl)
  library(XML)
  frutas.df <- data.frame(Fruta = c("Blackberries", "Blueberries"), 
                          Url = c("https://www.marketnews.usda.gov/mnp/fv-report-top-filters?&commAbr=BLKBERI-V&region=&repType=shipPriceDaily&portal=fv&locName=&type=shipPrice&navClass=&navClass=&navType=byComm&varName=&locAbr=&volume=&commName=BLACKBERRIES&dr=1&repDate=", 
                                  "http://www.marketnews.usda.gov/mnp/fv-report-top-filters?&commAbr=BLUBY&shipNavClass=&portal=fv&repType=shipPriceDaily&movNavClass=&Go=Go&locName=&type=shipPrice&locAbrAll=&navClass=FRUITS&navType=byComm&organic=&environment=&locAbr=&volume=&stateID=&commName=BLUEBERRIES&termNavClass=&repDate="))
  for(var in frutas.df$Fruta){
    
    fruta <- var
    
    purl <- frutas.df[frutas.df == var,]$Url
      
    fruit <- read.csv(paste0("Precios/",var,".csv"))%>%
      mutate(reportDate = as.Date(reportDate))
    
    dias <- as.Date(c(max(fruit$reportDate):Sys.Date()), origin = "1970-01-01")
    
    fruit <- fruit[,-c(1)]
    
    for(var2 in dias){
      
      tryCatch({    
        #var <- as.Date("2018-01-23")
        var2 <- as.Date(var2, origin= "1970-01-01") 
        
        dia <- format(var2,"%d")
        
        mes <- format(var2,"%m")
        
        year <- format(var2,"%Y")
        
        url <- paste0(purl,mes,"%2F",dia,"%2F",year,"&endDate=",mes,"%2F",dia,"%2F",
                      year,"&format=xml&rebuild=false")
        
        dest <- paste0("Precios/files/",fruta,"/",format(var2,"%Y-%m-%d"),".xml")
        
        if(!file.exists(dest)){
          download.file(url,dest, quiet = TRUE, method="libcurl")
        }
      },error = function(e){})
      
    }
    
    for(var3 in dias){
      tryCatch({  
      #var <- "2018-01-17"
      var3 <- as.Date(var3,origin="1970-01-01") 
      dia <- format(var3,"%d")
      mes <- format(var3,"%m")
      year <- format(var3,"%Y")
      dest <- paste0("Precios/files/",fruta,"/",format(var3,"%Y-%m-%d"),".xml")
      
      if(file.exists(dest)){
        data <- xmlParse(dest)
        #rootnode <- xmlRoot(data)
        table <- xmlToDataFrame(data)
        #table <- table[,-c(1)]
        table$reportDate <- as.Date(table$reportDate,format="%m/%d/%Y")
        fruit <- rbind(fruit,table)
        fruit <- unique(fruit)
      }
    }, error = function(e){})}
    
    write.csv(fruit,paste0("Precios/",fruta,".csv"))
  }
}

dbbackup <- function(){
  con <- odbcConnect(dsn = "SQLProyecto08", uid = "francisco", pwd = "Alpasa2017")
  for (var in sqlTables(con)$TABLE_NAME){
    write.csv(sqlFetch(con,var, as.is = TRUE),paste0("proyecto/",var,".csv"))
  }
  odbcClose(con)
}

ploteos <- function(df, fecha = "Fecha" , cantidad = "Cantidad"){

  df <- data.frame(Fecha = df[,c(fecha)], Cantidad = df[,c(cantidad)])%>%
    ddply(.(Fecha), summarize, Total = sum(Cantidad))
  
  minyear <- min(as.integer(format(df$Fecha, format = "%Y")))
  
  df <- df%>% 
    merge(data.frame(Fecha = as.Date(c(min(df$Fecha):max(df$Fecha)), 
                                     origin = "1970-01-01")), all.y = TRUE)%>%
    tiempos()
  
  df[is.na(df$Total),]$Total <- 0
  
  semanal <- ddply(df,.(Temporada, Semanats, Semana), summarize, Total = sum(Total))%>%
    arrange(Semanats)
  
  semanal.ts <- ts(semanal$Total, c(minyear, min(semanal$Semanats)), frequency = 52) 
  
  semanal.ts <- decompose(semanal.ts, "additive")
  
  semanal.cum <- semanal%>%
    arrange(Semana)%>%
    group_by(Temporada)%>%
    mutate(Acumulado =cumsum(Total))
  
  ts.semanal.cum <- ts(semanal.cum$Acumulado, c(minyear, min(semanal$Semanats)), frequency = 52)
  
  
  
  print(ggseasonplot(semanal.ts))
  
  dev.new()
  plot(dec.semanal)
  dev.new()
  print(ggseasonplot(ts.semanal.cum))
  dev.new()
  plot(dec.semanal.cum)
}

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
  df_mod <- df_mod[,c(names(df), Campos)]   
  
  return(df_mod)
}

plots.productos <- function(df){
  library(plotly)
  productos <- unique(df[df$Temporada =="2017-2018",]$Producto)
  
  graficas <<- new.env()
  
  for (var in productos){
    
    assign(var, df[df$Producto == var,]%>%
             merge(merge(data.frame(Temporada = unique(df$Temporada)), 
                         data.frame(Semana = c(1:52))), 
                   all.y = TRUE)%>%
             mutate(Total = ifelse(is.na(Total), 0, Total))%>%
             ddply(.(Semana, Temporada), summarize, Total = sum(Total)), envir = graficas)
    
    assign(paste0(var,".plot"), ggplotly(ggplot(get(var, envir = graficas), 
                                                aes(x = Semana, y = Total, 
                                                    colour = Temporada, 
                                                    group = Temporada)) + 
                                           geom_line() + labs(title = var)), graficas)
  }
  
}

kable2 = function(...) {
  knitr::kable(..., format.args = list(decimal.mark = '.', big.mark = ","))
}

#dolar
get.dolar <- function(){
  
  hoy <- format(Sys.Date(),format ="%Y-%m-%d")
  dolarurl <- paste0("https://fred.stlouisfed.org/graph/fredgraph.csv?chart_type=line&recession_bars=on&log_scales=&bgcolor=%23e1e9f0&graph_bgcolor=%23ffffff&fo=Open+Sans&ts=12&tts=12&txtcolor=%23444444&show_legend=yes&show_axis_titles=yes&drp=0&cosd=2010-01-01&coed=",
                     hoy,"&height=450&stacking=&range=Custom&mode=fred&id=DEXMXUS&transformation=lin&nd=1993-11-08&ost=-99999&oet=99999&lsv=&lev=&mma=0&fml=a&fgst=lin&fgsnd=2009-06-01&fq=Daily&fam=avg&vintage_date=&revision_date=&line_color=%234572a7&line_style=solid&lw=2&scale=left&mark_type=none&mw=2&width=968")
  
  download.file(dolarurl,"DEXMXUS.csv",quiet = TRUE,method="libcurl")
  
  dolar <- read.csv("DEXMXUS.csv", stringsAsFactors = FALSE)%>%
    mutate(DATE =as.Date(DATE))%>%
    mutate(DEXMXUS = ifelse(DEXMXUS ==".", NA, DEXMXUS),
           DEXMXUS = as.numeric(DEXMXUS))%>%
    repl
  
  
  dolar <- xts(order.by = dolar$DATE, x = dolar$DEXMXUS)
  
  
  dolar <- merge(index(dias), dolar, fill = )%>%
    mutate(DEXMXUS = ifelse(DEXMXUS ==".", NA, DEXMXUS),
           DEXMXUS = as.numeric(DEXMXUS))%>%
    arrange(DATE)%>%
    na.interpolation(DEXMXUS, option = "linear")%>%
    rename(Fecha = DATE, Dolar = DEXMXUS)%>%
    write.csv("dolar.csv", row.names = FALSE)

}

get.productos <- function(df, Campos = c("Clave_fruta", "Clave_presentacion",  "Nombre_producto", "Clams",             
                                              "Peso", "Unidad", "Presentacion", "Nombre_fruta", "Nombre_corto_fruta", "Variedad", "Fruta",             
                                              "Producto", "Fraccion6oz")){
  
  df <- left_join(myfetch("tbProductos")%>%
                       filter(strCan_cel == "NO"),
                     myfetch("tbPresentaciones")%>%
                       filter(strCan_cel == "NO"), 
                     by = "intCla_pre", suffix = c("h", "b"))%>%
    transmute(Clave_presentacion = intCla_pre, intCla_fru = intCla_fru, 
              Clave_producto = strCla_prod, Nombre_producto = strNom_bre,
              Clams = as.integer(intCan_tid), Peso = as.numeric(intPes_o), 
              Unidad = strUni_med)%>%
    collect()%>%
    mutate(Presentacion = paste0(Clams,"X", Peso, Unidad))%>%
    left_join(myfetch("tbFrutas")%>%
                collect(), by = "intCla_fru")%>%
    rename(Clave_fruta = intCla_fru, Nombre_fruta = strNom_bre, Nombre_corto_fruta = strNom_cto, 
              Variedad = strVar_ied, Variedad_corto = strVar_cto, Tipo_fruta = strTip_o,
              Tipo_corto = strTip_cto)%>% 
    mutate(Fruta = paste(Nombre_corto_fruta,Tipo_corto))%>%
    mutate(Producto = paste0(Nombre_producto, "_", Presentacion, "_", Fruta),
           Fraccion6oz = ifelse(Unidad == "G", Clams*Peso*0.035274/72  , Clams * Peso/72),
           Peso_total =  ifelse(Unidad == "OZ", Peso*Clams,
                                                   ifelse(Unidad =="G", Peso*Clams*0.035274, 
                                                          ifelse(Unidad == "KG",Peso*Clams*35.274,
                                                                 ifelse(Unidad == "LB", Peso*Clams*16, NA)))))%>%
    select(Clave_producto, one_of(Campos))%>%
    right_join(df, by = "Clave_producto")
  
  return(df)
  

}

load.productos <- function(){

productos <<- left_join(myfetch("tbProductos")%>%
                     filter(strCan_cel == "NO"),
                   myfetch("tbPresentaciones")%>%
                     filter(strCan_cel == "NO"), 
                   by = "intCla_pre", suffixes = c("h", "b"))%>%
  transmute(Clave_presentacion = intCla_pre, intCla_fru = intCla_fru, 
            Clave_producto = strCla_prod, Nombre_producto = strNom_bre,
            Clams = as.integer(intCan_tid), Peso = as.numeric(intPes_o), 
            Unidad = strUni_med)%>%
  mutate(Presentacion = paste0(Clams,"X", Peso, Unidad))%>%
  left_join(myfetch("tbFrutas"), by = "intCla_fru")%>%
  rename(Clave_fruta = intCla_fru, Nombre_fruta = strNom_bre, Nombre_corto_fruta = strNom_cto, 
         Variedad = strVar_ied, Variedad_corto = strVar_cto, Tipo_fruta = strTip_o,
         Tipo_corto = strTip_cto)%>% 
  mutate(Fruta = paste(Nombre_corto_fruta,Tipo_corto))%>%
  mutate(Producto = paste0(Fruta,"_", Presentacion, "_", Nombre_producto),
         Fraccion6oz = ifelse(Unidad == "G", Clams*Peso*0.035274/72  , Clams * Peso/72))

}



precio.promedio_diario <- function(df){

  precio_diario <- read.csv("csvs/precio.mean_diario.csv", stringsAsFactors = FALSE)%>%
    mutate(Fecha = as.Date(Fecha), Precio_promedio = as.numeric(Precio_promedio))
  
  df <- merge(df,precio_diario, by = c("Fecha", "Clave_producto"))
  
  return(df)
  
}

precio.promedio_semanal <- function(df){
  
  precio_semanal <- read.csv("csvs/precio.mean_semanal.csv", stringsAsFactors = FALSE)%>%
    mutate(Precio_semanal = as.numeric(Precio_semanal))
  
  df <- merge(df, precio_semanal, by = c("Semana", "Temporada", "Clave_producto"), all.x = TRUE)
  
  return(df)
  
  
}    

nombres.productores <- function(df, Campos = c("Nombre", "Apellido_paterno", "Apellido_materno")){
  
  productores <- myfetch2("tbProductores")%>%
    filter(strCan_cel == "NO")%>%
    transmute(Clave_productor = intCla_pro, Nombre = strNom_bre, Apellido_paterno = strApe_pat,
              Apellido_materno = strApe_mat)%>%
    select(Clave_productor, one_of(Campos))%>%
    right_join(df, by ="Clave_productor", all.y =TRUE)
  
  
}

entradas.fruta <- function(Campos = c("Id_entradas","Idb_entradas", "Nota", "Folio", "Fecha", "Clave_productor", "Clave_acopio",   
                                      "Cantidad", "Rechazadas", "Clave_producto", "Pallet", "Precio",
                                      "Numero_pago", "Pagado", "Aceptadas")){

  entradas_fruta <- left_join(myfetch("tbrecFruEmp"), myfetch("tbrecFruEmpReg"), 
                          by = "intNum_reg", suffix = c("h","b"))%>%
    filter(strCan_celb == "NO")%>%
    transmute(Id_entradas = intNum_reg, Idb_entradas = intNum_regA, Folio = intNum_fol, Nota = intNum_not, 
              Fecha = as.Date(fecFec_not), Clave_productor = intCla_pro, 
              Clave_acopio = intCen_aco, Cantidad = as.integer(intCan_tid), Rechazadas = as.integer(intCan_rec), Clave_producto =  intCla_prod,
              Pallet = intNum_pal, Precio = as.numeric(floPre_uni), Numero_pago = intNum_pag, Pagado = strPag_ado)%>%
   # filter(Fecha >= inicio_temporada)%>%
    mutate(Aceptadas = Cantidad - Rechazadas)%>%
    select(one_of(Campos))
  
  
 return(entradas_fruta)
}

get.acopio <- function(df){
  
  df <- left_join(df,
                  myfetch2("tbAlmacenes")%>%
                    filter(strCan_cel == "NO")%>%
                    transmute(Clave_acopio = intCla_alm, Acopio = strNom_bre),
              by = "Clave_acopio", suffix = c("h", "b"))
  
  return(df)
}

entradas.material <- function(){
  
  entradas_material <- left_join(myfetch("tbAlmEnt"),myfetch("tbAlmEntReg"), 
                             by = "intNum_reg", all.x = TRUE, suffix = c("h", "b"))%>%
    filter(strCan_celb == "NO")%>%
    transmute(Clave_acopio = intCla_alm, Tipo_entrada =  strTip_ent, Fecha = as.Date(fecFec_fac), 
              Tipo_proveedor = strTip_prov, Cantidad = intCan_tid , Clave_material = intCla_mat,
              Clave_proveedor = intCla_prov)%>%
    filter(Fecha > inicio_temporada)
    

  
  return(entradas_material)
}

entregas.material <- function(Campos = c("Clave_acopio", "Tipo_salida", "Clave_productor", "Fecha", "Cantidad",
                                         "Clave_material", "Tipo_receptor" )){
  
  entregas_material <- left_join(myfetch("tbAlmSal"),myfetch("tbAlmsalReg"), 
                             by = "intNum_reg", all.x = TRUE, suffix = c("h", "b"))%>%
    filter(strCan_celb == "NO")%>%
    transmute(Clave_acopio = intCla_alm, Tipo_salida = strTip_sal, Clave_productor = intCla_pro,
              Fecha = as.Date(fecFec_not), Cantidad = intCan_tid, Clave_material = intCla_mat, 
              Tipo_receptor = strTip_pro )%>%
    filter(Fecha > inicio_temporada)%>%
    select(one_of(Campos))
  
  return(entregas_material)
}

get.material <- function(df, Campos = c("Clave_material", "Marca", "Tipo_material", "Presentacion",
                                        "Clave_presentacion", "Material", "Clams", "Peso", "Unidad")){
  
  df <- left_join(df, left_join(myfetch("tbMateriales")%>%
                          transmute(Clave_material = intCla_mat, Marca = strMar_ca,
                                    Tipo_material = strTip_o,
                                    Clave_presentacion = intCla_pre), 
                        myfetch("tbPresentaciones")%>%
                          transmute(Clave_presentacion = intCla_pre, Clams = as.integer(intCan_tid), 
                                    Peso = as.numeric(intPes_o), 
                                    Unidad = strUni_med, Cantidad = intCan_tid),
                        all.x = TRUE, by = "Clave_presentacion")%>%
                    collect()%>%
                mutate(Presentacion = ifelse(Tipo_material == "CLAMSHELL", paste0(Peso, Unidad), 
                                             paste0(Cantidad, "X", Peso, Unidad)))%>%
                mutate(Material = paste(Marca, Presentacion, Tipo_material))%>%
                select(Clave_material, one_of(Campos)),
              by = "Clave_material", all.x = TRUE)
              
  return(df)              
   
}


load.materiales <- function( Campos = c("Clave_material", "Marca", "Tipo_material", "Presentacion",
                                        "Clave_presentacion", "Material")){
  
  materiales <<- myfetch2("tbMateriales")%>%
    transmute(Clave_material = intCla_mat, Marca = strMar_ca,
              Tipo_material = strTip_o, Presentacion = strPre_sen,
              Clave_presentacion = intCla_pre)%>%
    collect()%>%
    mutate(Material = paste(Marca, Presentacion, Tipo_material))%>%
    select(Clave_material, one_of(Campos))
 
}

load.presentaciones <- function(){
  
  presentaciones <<- myfetch("tbPresentaciones")
}



get.presentacion <- function(df, Campos = c("Clams", "Peso", "Unidad", "Presentacion")){
  
  df <- left_join(df, myfetch("tbPresentaciones")%>%
                transmute(Clave_presentacion = intCla_pre, Clams = as.integer(intCan_tid), 
                          Peso = as.numeric(intPes_o), 
                          Unidad = strUni_med, Tipo_material = strTip_o)%>%
                  collect()%>%
                mutate_if(Tipo_material == "CLAMSHELL", Presentacion = paste0(Peso, Unidad),
                          Presentacion = paste0(Clams,"X", Peso, Unidad))%>%
                select(Clave_presentacion, one_of(Campos)),
              all.x = TRUE, by = "Clave_presentacion")

}

saldos.material <- function(Clave){
  
  x  <-   entradas.fruta(Campos = c("Fecha", "Clave_productor", "Clave_producto", "Aceptadas"))%>%
    filter(Clave_productor == Clave)%>%
    ddply(.(Fecha, Clave_producto), summarize, Total = sum(Aceptadas))
 
saldos_material <-    rbind(

    #Cajas con fruta
    x%>%
      get.productos(Campos = c("Presentacion"))%>%
      select(Fecha, Presentacion,Total)%>%
      filter(Fecha >= inicio_temporada)%>%
      mutate(Tipo_material = "CAJA EMP.", Tipo = "FRUTA", Total = - Total),   #Cajas con fruta
    
    #clams con fruta
    
    x%>%
      get.productos(Campos = c("Presentacion", "Peso", "Unidad", "Clams"))%>%
      mutate(Total = Total*Clams,
             Presentacion = paste0(Peso,Unidad))%>%
      select(Fecha, Presentacion, Total)%>%
      mutate(Tipo_material = "CLAMSHELL", Tipo = "FRUTA", Total = -Total),    
    
    #prestamos
    entregas.material(Campos = c("Tipo_salida", "Clave_productor", 
                                 "Fecha", "Cantidad", "Clave_material",
                                 "Tipo_receptor", "Clave_acopio"))%>%
      filter(Clave_productor == Clave)%>%
      filter(Tipo_salida == "PRESTAMO", Tipo_receptor == "PRODUCTOR")%>%
      select(Fecha, Clave_material, Cantidad)%>%
      rename(Total = Cantidad)%>%
      get.material(Campos = c("Presentacion", "Tipo_material")) %>%
      mutate(Tipo = "PRESTAMO")%>%
      select(Fecha, Presentacion, Total, Tipo_material, Tipo),
    
    #devoluciones
    
    entradas.material()%>%
      rename(Clave_productor = Clave_proveedor)%>%
      filter(Clave_productor == Clave)%>%
      filter(Tipo_entrada == "DEVOLUCION", Tipo_proveedor == "PRODUCTOR")%>%
      rename(Tipo = Tipo_entrada)%>%
      get.material(Campos = c("Presentacion", "Tipo_material"))%>%
      mutate(Total = -Cantidad)%>%
      select(Fecha, Presentacion, Total, Tipo_material, Tipo))#%>%
  
  #sumas
  #ddply(.(Presentacion), summarize, Saldo = sum(Total))

return(saldos_material)  
}


semanas <- function(df, Campos = list(Total = 0)){
  
  df <- merge(
              data.frame(Semana = factor(c(1:52), levels = (c(34:85)%%52 + 1), ordered = TRUE)),
              df, all.x = TRUE, by = "Semana")%>%
    replace_na(Campos)
  
}

dias <- function(df, Campos = list(Total = 0)){
  
  df <- merge(data.frame(Fecha = as.Date(c(inicio_temporada:Sys.Date()), origin = "1970-01-01")),
              df, all.x = TRUE, by = "Fecha")%>%
    replace_na(Campos)
}

getmode <- function(v) {
  uniqv <- unique(v)
  uniqv[which.max(tabulate(match(v, uniqv)))]
}

myhcsemana <- function(df, name = "Producto", data = "Total"){
  
  df2 <- data.frame(Semana = df$Semana)
  
  
  df2$Producto <- df[,c(name)]
  df2$Total <- df[,c(data)]
  
hc <-  highchart()%>%
#    hc_add_series_list(x)%>%
    hc_chart(type = "column")%>%
    hc_chart(zoomType = "xy", panKey = "ctrl", panning = TRUE)%>%
    hc_xAxis(list(type = "category", crosshair = TRUE, categories =  sort(unique(data.frame(Semana = factor(c(1:52), 
                                                                                                            levels = (c(34:85)%%52 + 1), 
                                                                                                            ordered = TRUE))[,1])),
                  min =  min(as.numeric(df2[df2$Total != 0,]$Semana))-1, 
                  max = max(as.numeric(df2[df2$Total != 0,]$Semana))))%>%
    hc_tooltip(crosshair = TRUE,
               shared = TRUE,
               split = FALSE)%>%
    hc_plotOptions(column = list(tooltip = list( useHTML= TRUE,
                                                 headerFormat =  '<small>Semana:{point.key}</small><table>',
                                                 pointFormat = '<br><tr><td style="color: {point.color}"> {series.name} : </td></br>
                                               <br style="color: #000000"> {point.y} Cajas</br>' ,
                                                 valueDecimals = 2)))
  
  
 # l = list()
  for(var in  unique(df2$Producto)){
    y  <- df2%>%
      filter(Producto == var)%>%
      semanas(Campos = list(Total = NA))%>%
      arrange(Semana)
    
    
    
    serie <- list(list(x = y$Semana, name = var, data = y$Total))
    
    hc <- hc_add_series_list(hc,serie)
  
  }
  
  
    return(hc)
               

    
  
  
  
}

myhcsemana.line <- function(df, name = "Producto", data = "Total"){
  
  df2 <- df%>%
    select(Semana, one_of(c(name,data)))
  
  names(df2) <- c("Semana", "Producto", "Total")
  
  
  x = list()
  for(var in  unique(df2$Producto)){
    y  <- df2%>%
      filter(Producto == var)%>%
      semanas(Campos = list(Total = 0))%>%
      arrange(Semana)%>%
      mutate(Saldo = cumsum(Total))%>%
      ungroup()
    
    
    x[[length(x)+1]] <-  list(name = var, data = y$Saldo)
    
  }
  
  highchart()%>%
    hc_add_series_list(x)%>%
    hc_chart(type = "line")%>%
    hc_chart(zoomType = "xy", panKey = "ctrl", panning = TRUE)%>%
    hc_xAxis(list(type = "category", 
                  categories =  sort(unique(data.frame(Semana = factor(c(1:52), 
                                                                       levels = (c(34:85)%%52 + 1), 
                                                                       ordered = TRUE))[,1])),
                  min =  min(as.numeric(df2[df2$Total != 0,]$Semana))-1, 
                  max = max(as.numeric(df2[df2$Total != 0,]$Semana))))
  
  
  
}

myhcdiario.line <- function(df, name = "Producto", data = "Total"){
  
  df2 <- df%>%
    select(Fecha, one_of(c(name,data)))
  
  names(df2) <- c("Fecha", "Producto", "Total")
  
  
  x = list()
  for(var in  unique(df2$Producto)){
    y  <- df2%>%
      filter(Producto == var)%>%
      dias(Campos = list(Total = 0))%>%
      arrange(Fecha)%>%
      mutate(Saldo = cumsum(Total))%>%
      ungroup()%>%
      mutate(Producto = var)
    
    
    x[[length(x)+1]] <-  list(y)
    
  }
  
y  <- ldply(x, data.frame)
  
  
  
    hchart(y, hcaes(x = Fecha, y = Saldo, group = Producto), type = "line")%>%
    hc_chart(zoomType = "xy", panKey = "ctrl", panning = TRUE)
  
  
}

precios.usda <- function(){
  #library(imputeTS)
  library(xts)
  
  frutas.df <- data.frame(Fruta = c("Blackberries", "Blueberries"), 
                          Nombre_corto = c("ZAR","ARA"))
  
  for (var in frutas.df$Fruta){
    precios.var <- read.csv(paste0("Precios/",var,".csv"))%>%
      rename(Presentacion_usda = packageDesc, Puerto =  cityName , Fecha = reportDate, 
             Precio_menor = lowPriceMin, Precio_mayor = highPriceMax, Frecuente_menor = mostlyLowMin,
             Frecuente_mayor = mostlyHighMax, Suministro = supplyTone, Demanda = demandTone, 
             Mercado = marketTone, Ciudad_reporte = reportingCity)%>%
      mutate(Fecha = as.Date(Fecha))%>%
      select(Puerto, Fecha, Precio_menor, Precio_mayor, Frecuente_menor, Frecuente_mayor,
             Ciudad_reporte, organic)
    
 
    assign(paste0(frutas.df[frutas.df$Fruta == var,]$Nombre_corto,".CN"), 
           precios.var[precios.var$organic != "Organic",]%>%
             mutate(Fruta = paste(frutas.df[frutas.df$Fruta == var,]$Nombre_corto,"CN")))
    
  }
  ara_usda <- merge(ARA.CN%>%
    filter(!Puerto %in% c("CENTRAL & NORTH FLORIDA", 
                          "ARGENTINA IMPORTS - PORT OF ENTRY LOS ANGELES INTERNATIONAL AIRPORT"))%>%
    ddply(.(Fecha), summarize, 
          High = mean(Precio_mayor, na.rm = TRUE), Low = mean(Precio_menor, na.rm = TRUE), 
          Close = ((High+Low)/2))%>%
    xts(order.by = .$Fecha),
    index(xts(order.by = seq(min(ARA.CN$Fecha),max(ARA.CN$Fecha), "day"))), fill  = na.approx)
 
  ara_usda$Open <- lag(ara_usda$Close)
    
  ara_usda <<- ara_usda[,c("Open", "High", "Low", "Close")]
 
  zar <- merge(ZAR.CN%>%
    filter(Puerto %in% c("CARIBBEAN BASIN IMPORTS - PORTS OF ENTRY SOUTH FLORIDA",
                         "CENTRAL AMERICA IMPORTS - PORTS OF ENTRY SOUTH FLORIDA",
                         "MEXICO CROSSINGS THROUGH ARIZONA, CALIFORNIA AND TEXAS"))%>%
    ddply(.(Fecha), summarize, 
          High = mean(Precio_mayor, na.rm = TRUE), Low = mean(Precio_menor, na.rm = TRUE), 
          Close = ((High+Low)/2))%>%
    xts(order.by = .$Fecha),
  index(xts(order.by = seq(min(ARA.CN$Fecha),max(ARA.CN$Fecha), "day"))), fill  = na.approx)
  
  zar$Open <- lag(zar$Close)
  
  return(zar[,c("High","Low" ,"Open" , "Close")])
  

}


get.cliente <- function(df){
  df <- left_join(df, myfetch("tbClientes")%>%
    transmute(Clave_cliente = intCla_cli, Cliente = strNom_bre)%>%
      collect(),
    by = "Clave_cliente")
    return(df)
}

get.destino <- function(df){
  df <- left_join(df, myfetch("tbDestinos")%>%
                    transmute(Pais = strPai_s, Clave_destino= intCla_des)%>%
                    collect(),
                  by = "Clave_destino")
}




salidas.fruta <- function(Campos = c( "Folio", "Fecha", "Clave_cliente",  "Clave_destino",
                                       "Destino", "Pallets", "Fecha_arrivo", "Pallet", 
                                      "Clave_producto", "Cantidad")){
  
  salidas_fruta <- left_join(myfetch("tbSalFruEmp")%>%
    filter(strCan_cel == "NO"),
    myfetch("tbSalFruEmpReg")%>%
      filter(strCan_cel == "NO"),
    by = "intNum_reg", suffix = c("h", "b"))%>%
    transmute(Folio = intNum_fol, Fecha = fecFec_sal,
              Clave_cliente = intCla_cli, Clave_destino =  intCla_des,
              Destino = strDes_tin,Pallets =  strPal_let, Mexican_cus = strMex_cus, 
              American_cus = strAme_cus, strWar_hou =  strWar_hou, Fecha_arrivo = strArr_dat,  
              Pallet = intNum_pal,
              Clave_producto = strCla_prod, Cantidad = intCan_tid)%>%
    mutate(Fecha = as.Date(Fecha), Cantidad = as.integer(Cantidad))%>%
    select(one_of(Campos))
  
}


liquid <- function(Campos = c( "Folio", "Fecha","Clave_producto", "Clave_fruta", "Clave_cliente", "Clave_destino", "Pallets",
                                      "Precio_real", "Comision_aduanal","Cantidad", 
                                      "Manejo", "Handlio", "Comision_ventas", "Caja", "Frio", 
                                      "Mano_obra","Energia")){
  
  df <- left_join(left_join(myfetch("tbSalFruEmp")%>%
                              filter(strCan_cel == "NO"),
                            
                            myfetch("tbSalFruEmpReg")%>%
                              filter(strCan_cel == "NO"),
                            by = "intNum_reg", suffix = c("h", "b"))%>%
                    
                    transmute(Folio = intNum_fol, Fecha = fecFec_sal,
                              Clave_cliente = intCla_cli, Clave_destino =  intCla_des,
                              Destino = strDes_tin,Pallets =  strPal_let, Mexican_cus = strMex_cus, 
                              American_cus = strAme_cus, strWar_hou =  strWar_hou, Fecha_arrivo = strArr_dat,  
                              Pallet = intNum_pal,
                              Clave_producto = strCla_prod, Cantidad = intCan_tid)%>%
                    mutate(Fecha = as.Date(Fecha), Cantidad = as.integer(Cantidad))%>%
                    collect()%>%
                    get.productos(Campos = c("Clave_fruta", "Clave_presentacion")),
                  
                  myfetch("tbPreciosEstimadosSalidas")%>%
                    filter(strCan_cel == "NO")%>%
                    transmute(Folio = intNum_fol, Clave_presentacion = intCla_pre, Clave_fruta = intCla_fru ,
                              Precio_real = floPre_rea, Comision_aduanal = floCus_tom, 
                              Manejo = floHan_dliD, Handlio = floHan_dliO, Comision_ventas = floSal_es,
                              Caja = floCaj_a, Frio = floFri_o, Mano_obra = floMan_obr, Energia = floEne_rgi)%>%
                    collect(), 
                  by = c("Folio", "Clave_presentacion", "Clave_fruta"), suffix = c("_s","_p" ))%>%
    filter(Folio > 1672)%>%
    select(one_of(Campos))
  
  
return(df)
}
  


zar.usda <- function(){
  #library(imputeTS)
  library(xts)
  
  frutas.df <- data.frame(Fruta = c("Blackberries", "Blueberries"), 
                          Nombre_corto = c("ZAR","ARA"))
  
  
    precios.var <- read.csv(paste0("Precios/","Blackberries",".csv"))%>%
      rename(Presentacion_usda = packageDesc, Puerto =  cityName , Fecha = reportDate, 
             Precio_menor = lowPriceMin, Precio_mayor = highPriceMax, Frecuente_menor = mostlyLowMin,
             Frecuente_mayor = mostlyHighMax, Suministro = supplyTone, Demanda = demandTone, 
             Mercado = marketTone, Ciudad_reporte = reportingCity)%>%
      mutate(Fecha = as.Date(Fecha))%>%
      select(Puerto, Fecha, Precio_menor, Precio_mayor, Frecuente_menor, Frecuente_mayor,
             Ciudad_reporte, organic)
    
    
  ZAR.CN <-precios.var[precios.var$organic != "Organic",]
    
  
 
  zar <- merge(ZAR.CN%>%
                 filter(Puerto %in% c("CARIBBEAN BASIN IMPORTS - PORTS OF ENTRY SOUTH FLORIDA",
                                      "CENTRAL AMERICA IMPORTS - PORTS OF ENTRY SOUTH FLORIDA",
                                      "MEXICO CROSSINGS THROUGH ARIZONA, CALIFORNIA AND TEXAS"))%>%
                 ddply(.(Fecha), summarize, 
                       High = mean(Precio_mayor, na.rm = TRUE), Low = mean(Precio_menor, na.rm = TRUE), 
                       Close = ((High+Low)/2))%>%
                 xts(order.by = .$Fecha),
               index(xts(order.by = seq(min(ZAR.CN$Fecha, na.rm = TRUE),max(ZAR.CN$Fecha, na.rm = TRUE), "day"))), fill  = na.approx)
  
  zar$Open <- lag(zar$Close)
  
  return(zar[,c("High","Low" ,"Open" , "Close")])
  
}

ara.usda <- function(){
  
 library(xts)

  frutas.df <- data.frame(Fruta = c("Blackberries", "Blueberries"), 
                          Nombre_corto = c("ZAR","ARA"))
  
precios.var <- read.csv(paste0("Precios/","Blueberries",".csv"))%>%
  rename(Presentacion_usda = packageDesc, Puerto =  cityName , Fecha = reportDate, 
         Precio_menor = lowPriceMin, Precio_mayor = highPriceMax, Frecuente_menor = mostlyLowMin,
         Frecuente_mayor = mostlyHighMax, Suministro = supplyTone, Demanda = demandTone, 
         Mercado = marketTone, Ciudad_reporte = reportingCity)%>%
  mutate(Fecha = as.Date(Fecha))%>%
  select(Puerto, Fecha, Precio_menor, Precio_mayor, Frecuente_menor, Frecuente_mayor,
         Ciudad_reporte, organic)


ARA.CN <-precios.var[precios.var$organic != "Organic",]

ara <- merge(ARA.CN%>%
                    filter(!Puerto %in% c("CENTRAL & NORTH FLORIDA", 
                                          "ARGENTINA IMPORTS - PORT OF ENTRY LOS ANGELES INTERNATIONAL AIRPORT"))%>%
                    ddply(.(Fecha), summarize, 
                          High = mean(Precio_mayor, na.rm = TRUE), Low = mean(Precio_menor, na.rm = TRUE), 
                          Close = ((High+Low)/2))%>%
                    xts(order.by = .$Fecha),
                  index(xts(order.by = seq(min(ARA.CN$Fecha),max(ARA.CN$Fecha), "day"))), fill  = na.approx)

ara$Open <- lag(ara$Close)

return(ara[,c("High","Low" ,"Open" , "Close")])

}

decompose.xts <-  function (x, type = c("additive", "multiplicative"), filter = NULL)   {
    dts <- decompose(as.ts(x), type, filter)
    dts$x <- .xts(dts$x, .index(x))
    dts$seasonal <- .xts(dts$seasonal, .index(x))
    dts$trend <- .xts(dts$trend, .index(x))
    dts$random <- .xts(dts$random, .index(x))
    
    with(dts,
         structure(list(x = x, seasonal = seasonal, trend = trend,
                        random = if (type == "additive") x - seasonal - trend else x/seasonal/trend, 
                        figure = figure, type = type), class = "decomposed.xts"))
  }

plot.decomposed.xts <-  function(x, ...){
    xx <- x$x
    if (is.null(xx))
      xx <- with(x,
                 if (type == "additive") random + trend + seasonal
                 else random * trend * seasonal)
    p <- cbind(observed = xx, trend = x$trend, seasonal = x$seasonal, random = x$random)
    plot(p, main = paste("Decomposition of", x$type, "time series"), multi.panel = 4,
         yaxis.same = FALSE, major.ticks = "days", grid.ticks.on = "days", ...)
  }