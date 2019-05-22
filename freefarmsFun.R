#funcion para salvar formularios
saveReg <- function(mother, child, 
                    motherTable = as.character(bquote(mother)), 
                    childTable = as.character(bquote(hija)),
                    Id_mother = paste0("Id_", motherTable)) {
  
  dbWithTransaction(con, {
    dbWriteTable(con = con, motherTable, mother, append = TRUE)
    
    newId <- myfetch(motherTable, con = con, source = "PGS")%>%
      select(Id_mother)%>%
      collect()%>%
      max()
    
    child[, Id_mother] <-  newId
    
    dbWriteTable(con = con, childTable, child, append = TRUE)})
  
  
}

#kable func
kable3 <- function(...){
  kableExtra::kable(..., row.names = FALSE)%>%
    kable_styling(bootstrap_options = "striped", full_width = F)
}

#funcion para agregar botones a tablas

shinyInput <- function(FUN, len, id, ...) {
  inputs <- character(length(len))
  for (i in 1:length(len)) {
    clave <- len[i]
    inputs[i] <- as.character(FUN(paste0(id, clave), ...))
  }
  inputs
}



#update row

dbUpdateRow  <- function(df, Id, tbl, row, ...){
  
  df$Llave <- df[,c(Id)]
  
  df <- df%>%
    select(-Llave)%>%
    gather("Campo", "Valor")
  
  querry1 <- paste0('"', df$Campo, '\"=\'', df$Valor,"'")
  
  actualizar_datos <- NULL
  
  
  
  for(var in querry1){
    actualizar_datos <- paste(actualizar_datos, var,",")
  }
  
  actualizar_datos <- substr(actualizar_datos, 1, nchar(actualizar_datos)-1)
  
  consulta <- paste0("update ",  "\"", tbl, "\" set", actualizar_datos, 'WHERE "' ,Id, '" = \'', row, "'")
  
  dbExecute(consulta, ...)
}


#etiquetas mandatorias

labelMandatory <- function(label) {
  tagList(
    label,
    span("*", class = "mandatory_star")
  )
}

appCSS <- ".mandatory_star { color: red; }"

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
      #merge(weeks, by = "Fecha", all.x = TRUE)%>%
    
     df_mod[,c(names(df), Campos)]
    
}    
  


zar.usda <- function(){
  library(readr)
  
  read_rds("zar_usda.RDS")
  
}

ara.usda <- function(){
  
  read_rds("ara_usda.RDS")
  
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

semanas <- function(df, Campos = list(Total = 0)){
  
  df <- merge(
    data.frame(Semana = factor(c(1:52), levels = (c(34:85)%%52 + 1), ordered = TRUE)),
    df, all.x = TRUE, by = "Semana")%>%
    replace_na(Campos)
  
}

jalarinputs <- function(campos, input){
  
  data <- sapply(campos, function(x) input[[x]])
  
  as.data.frame(t(data),
                stringsAsFactors = FALSE)
  
  
}
