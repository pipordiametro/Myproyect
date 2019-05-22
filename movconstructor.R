fruitmov <- data.frame()

fruta <- "Blackberries"

purl <- "https://www.marketnews.usda.gov/mnp/fv-report?&commAbr=BLKBERI-V&repType=movementDaily&repTypeChanger=movementDaily&locAbrPass=ALL%7C%7C&step3date=true&type=movement&locChoose=commodity&refine=false&_environment=1&locAbrlength=1&organic=&environment=&locAbr=&commodityClass=allcommodity&repDate="


for(var in c(2010:2018)){
  
  desde <- as.Date(paste0(var, "-01-01"))
  
  hasta <- as.Date(paste0(var, "-12-31"))
  
  diastart <- format(desde,"%d")
  
  messtart <- format(desde,"%m")
  
  yearstart <- format(desde,"%Y")
  
  diaend <- format(hasta,"%d")
  
  mesend <- format(hasta,"%m")
  
  yearend <- format(hasta,"%Y")
  
  url <- paste0(purl,messtart,"%2F",diastart,"%2F",yearstart,"&endDate=", mesend, 
                "%2F", diaend, "%2F",  yearend, "&format=xml&rebuild=false")
  
  dest <- paste0("Movimientos/files/",fruta,"/",format(Sys.Date()),".xml")
  
  download.file(url,dest, quiet = TRUE, method="libcurl")
  
  data <- xmlParse(dest)
  
  table <- xmlToDataFrame(data, stringsAsFactors = FALSE)
  
  if("reportDate" %in% names(table)){
    
    table <- table%>%
      mutate(reportDate = as.Date(reportDate, format = "%m/%d/%Y"))
    
    
  }else{
    
    if("date" %in% names(table)){
      table <- table%>%
        mutate(date = as.Date(date, format = "%m/%d/%Y"))
      
    }
  }
  fruitmov <- rbind(fruitmov,table)
  
  fruitmov <- unique(fruitmov)
  
  
}
x