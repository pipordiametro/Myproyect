#get.precios <- function(){
library(RCurl)
library(XML)
library(readr)
library(purrr)
library(xts)
library(fpp2)
frutas.df <- data.frame(Fruta = c("Blackberries", "Blueberries"), 
                        Url = c("https://www.marketnews.usda.gov/mnp/fv-report-top-filters?&commAbr=BLKBERI-V&region=&repType=shipPriceDaily&portal=fv&locName=&type=shipPrice&navClass=&navClass=&navType=byComm&varName=&locAbr=&volume=&commName=BLACKBERRIES&dr=1&repDate=", 
                                "http://www.marketnews.usda.gov/mnp/fv-report-top-filters?&commAbr=BLUBY&shipNavClass=&portal=fv&repType=shipPriceDaily&movNavClass=&Go=Go&locName=&type=shipPrice&locAbrAll=&navClass=FRUITS&navType=byComm&organic=&environment=&locAbr=&volume=&stateID=&commName=BLUEBERRIES&termNavClass=&repDate="),
                        Nombre_corto = c("ZAR","ARA"))
for(var in frutas.df$Fruta){
  
  fruta <- var
  
  purl <- frutas.df[frutas.df == var,]$Url
  
  fruit <- read_rds(paste0("Precios/",var,".RDS"))%>%
    mutate(reportDate = as.Date(reportDate))
  
  dias <- as.Date(c(as.Date("2010-01-01"):Sys.Date()), origin = "1970-01-01")
  
  #fruit <- fruit[,-c(1)]
  
  funcion1 <- function(var2){
    
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
  
  walk(dias, funcion1)
  
  
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
        fruit <-  unique(fruit)
      }
    }, error = function(e){})}
  
  write_rds(fruit,paste0("Precios/",fruta,".RDS"))
  
  if (var == "Blackberries"){
    
    
    
    precios.var <- read_rds(paste0("Precios/","Blackberries",".RDS"))%>%
      rename(Presentacion_usda = packageDesc, Puerto =  cityName , Fecha = reportDate, 
             Precio_menor = lowPriceMin, Precio_mayor = highPriceMax, Frecuente_menor = mostlyLowMin,
             Frecuente_mayor = mostlyHighMax, Suministro = supplyTone, Demanda = demandTone, 
             Mercado = marketTone, Ciudad_reporte = reportingCity)%>%
      mutate(Fecha = as.Date(Fecha), Precio_mayor = as.numeric(Precio_mayor),
             Precio_menor = as.numeric(Precio_menor))%>%
      select(Puerto, Fecha, Precio_menor, Precio_mayor, Frecuente_menor, Frecuente_mayor,
             Ciudad_reporte, organic)
    
    
    ZAR.CN <-precios.var[precios.var$organic != "Organic",]
    
    
    
    zar <- merge(ZAR.CN%>%
                   filter(Puerto %in% c(
                     #"CARIBBEAN BASIN IMPORTS - PORTS OF ENTRY SOUTH FLORIDA",
                     #  "CENTRAL AMERICA IMPORTS - PORTS OF ENTRY SOUTH FLORIDA",
                     "MEXICO CROSSINGS THROUGH ARIZONA, CALIFORNIA AND TEXAS"))%>%
                   ddply(.(Fecha), summarize, 
                         High = mean(Precio_mayor, na.rm = TRUE), Low = mean(Precio_menor, na.rm = TRUE), 
                         Close = ((High+Low)/2))%>%
                   xts(order.by = .$Fecha),
                 index(xts(order.by = seq(min(ZAR.CN$Fecha, na.rm = TRUE),max(ZAR.CN$Fecha, na.rm = TRUE), "day"))), fill  = na.approx)
    
    zar$Open <- lag(zar$Close)
    
    write_rds(zar[,c("High","Low" ,"Open" , "Close")], "zar_usda.RDS")
    
    
    zar_usda <- zar
    
    fruta <-  data.frame(Fecha = index(zar_usda$Close), Close = zar_usda$Close)%>%
      tiempos("Semanats")%>%
      ddply(.(Semanats), summarize, Promedio = mean(Close), Fecha = min(Fecha))%>%
      arrange(Semanats)
    
    
    fruta_ts <- ts(fruta$Promedio, 
                   start = c(min(as.integer(format(fruta$Fecha, format = "%Y"))), 
                             min(fruta$Semanats)),
                   frequency = 52)
    
    fit <- tbats(fruta_ts)
    
    write_rds(fit,"zar_fit.RDS")
  }else if(var == "Blueberries"){
    
    precios.var <- read_rds(paste0("Precios/","Blueberries",".RDS"))%>%
      rename(Presentacion_usda = packageDesc, Puerto =  cityName , Fecha = reportDate, 
             Precio_menor = lowPriceMin, Precio_mayor = highPriceMax, Frecuente_menor = mostlyLowMin,
             Frecuente_mayor = mostlyHighMax, Suministro = supplyTone, Demanda = demandTone, 
             Mercado = marketTone, Ciudad_reporte = reportingCity)%>%
      mutate(Fecha = as.Date(Fecha), Precio_mayor = as.numeric(Precio_mayor),
             Precio_menor = as.numeric(Precio_menor))%>%
      select(Puerto, Fecha, Precio_menor, Precio_mayor, Frecuente_menor, Frecuente_mayor,
             Ciudad_reporte, organic)
    
    
    ARA.CN <-precios.var[precios.var$organic != "Organic",]%>%
      filter(!is.na(Puerto))
    
    ara <- merge(ARA.CN%>%
                   filter(!Puerto %in% c("CENTRAL & NORTH FLORIDA",
                                         "MEXICO CROSSINGS THROUGH ARIZONA, CALIFORNIA AND TEXAS",
                                         "ARGENTINA IMPORTS - PORT OF ENTRY LOS ANGELES INTERNATIONAL AIRPORT",
                                         "CHILE IMPORTS - PORT OF ENTRY LOS ANGELES AREA",
                                         "CHILE IMPORTS - PORT OF ENTRY MIAMI AREA",
                                         "CHILE IMPORTS - PORT OF ENTRY PHILADELPHIA AREA ",
                                         "CHILE IMPORTS - PORT OF ENTRY LOS ANGELES INTERNATIONAL AIRPORT ",
                                         "CHILE IMPORTS - PORT OF ENTRY MIAMI INTERNATIONAL AIRPORT",
                                         "ARGENTINA IMPORTS - PORT OF ENTRY MIAMI INTERNATIONAL AIRPORT",
                                         "URUGUAY IMPORTS - PORT OF ENTRY MIAMI INTERNATIONAL AIRPORT",
                                         "ARGENTINA IMPORTS - PORT OF ENTRY LOS ANGELES INTERNATIONAL AIRPORT",
                                         "MEXICO CROSSINGS THROUGH ARIZONA, CALIFORNIA AND TEXAS",
                                         "ARGENTINA/URUGUAY IMPORTS - PORTS OF ENTRY SOUTH FLORIDA",
                                         "ARGENTINA/URUGUAY IMPORTS - PORTS OF ENTRY SOUTHERN CALIFORNIA",
                                         "PERU IMPORTS - PORTS OF ENTRY PHILADELPHIA AREA AND NEW YORK CITY AREA",
                                         "PERU IMPORTS - PORTS OF ENTRY SOUTHERN CALIFORNIA"
                   ))%>%
                   ddply(.(Fecha), summarize, 
                         High = mean(Precio_mayor, na.rm = TRUE), Low = mean(Precio_menor, na.rm = TRUE), 
                         Close = ((High+Low)/2))%>%
                   xts(order.by = .$Fecha),
                 index(xts(order.by = seq(min(ARA.CN$Fecha),max(ARA.CN$Fecha), "day"))), fill  = na.approx)
    
    ara$Open <- lag(ara$Close)
    
    ara_usda <- ara
    
    write_rds(ara[,c("High","Low" ,"Open" , "Close")], "ara_usda.RDS")
    
    fruta <-  data.frame(Fecha = index(ara_usda$Close), Close = ara_usda$Close)%>%
      tiempos("Semanats")%>%
      ddply(.(Semanats), summarize, Promedio = mean(Close), Fecha = min(Fecha))%>%
      arrange(Semanats)
    
    fruta_ts <- ts(fruta$Promedio, 
                   start = c(min(as.integer(format(fruta$Fecha, format = "%Y"))), 
                             min(fruta$Semanats)),
                   frequency = 52)
    
    fit <- tbats(fruta_ts)
    
    write_rds(fit,"ara_fit.RDS")
  }
}
}

#get.precios2 <- function(){
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