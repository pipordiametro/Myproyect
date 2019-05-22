library(plyr)
library(dplyr)
library(pool)

pool <- dbPool(
  drv = RMySQL::MySQL(),
  user='francisco', 
  password='francisco', 
  dbname='alpasaAcopios', 
  host='alpasafarms.com',
  port = 3306,
  minSize = 3,
  maxSize = 8,
  idleTimeout = 15,
  validationInterval = 20
  
  
  
)


pagFetch = function(tabla, cancelado = TRUE, acopio = NULL, detalles = FALSE){
  
  camposMod <- c("usuario_creo", "fecha_creacion", "usuario_modifico",
                 "fecha_modifico", "usuario_elimino", "fecha_elimino", "cancelado","fecha_creo")
  
  if(cancelado){
    
    if(is.null(acopio)){
      tbl(pool, tabla, stringsAsFactors = FALSE)%>%
        filter(cancelado == "NO")%>%
        select(-one_of(camposMod))
    }else{
      tbl(pool, tabla, stringsAsFactors = FALSE)%>%
        filter(cancelado == "NO")%>%
        select(-one_of(camposMod))%>%
        filter(clave_acopio == acopio)
    }
  }else{
    if(is.null(acopio)){
      tbl(pool, tabla)%>%
        #filter(cancelado == "NO")%>%
        select(-one_of(camposMod))
    }else{
      tbl(pool, tabla)%>%
        # filter(cancelado == "NO")%>%
        select(-one_of(camposMod))%>%
        filter(clave_acopio == acopio)
    }
  }
  
}

acopio = 4



pagEnv <- list2env(
  list(
    
    recepcion_fruta =   left_join(
      pagFetch("recepcion_fruta", acopio = acopio),
      pagFetch("recepcion_fruta_det")%>%select(-id),
      by = c("id" = "id_recepcion")),
    
    productores = pagFetch("productores")%>%
      rename(nombre_productor = nombre),
    
    productos = pagFetch("productos")%>%
      #filter(cancelado == "NO")%>%
      select(clave_producto, nombre, clave_fruta, clave_presentacion)%>%
      rename(nombre_producto = nombre),
    
    fruta = pagFetch("fruta")%>%
      #filter(cancelado == "NO")%>%
      select(clave_fruta, nombre, nombre_corto, variedad, variedad_corto, tipo,
             tipo_corto, activo)%>%
      rename(nombre_fruta = nombre),
    
    presentaciones = pagFetch("presentacion")%>%
      #filter(cancelado == "NO")%>%
      select(clave_presentacion, cantidad, peso, medida, activo),
    
    almacen_salida = left_join(
      pagFetch("almacen_salida", acopio = acopio),
      pagFetch("almacen_salida_det")%>%select(-id),
      by = c("id" = "id_salida")),
    
    
    
    almacen_entrada = left_join(
      pagFetch("almacen_entrada", acopio = acopio),
      pagFetch("almacen_entrada_det")%>%select(-id),
      by = c("id" = "id_entrada")
    ),
    
    material = pagFetch("material"),
    
    pallet = left_join(
      pagFetch("pallet"),
      pagFetch("pallet_det")%>%select(-id), 
      by = c("id" = "id_pallet")
    ),
    
    salida_acopio = left_join(
      left_join(
        pagFetch("salida_acopio", acopio = acopio),
        pagFetch("salida_acopio_det")%>%select(-id),
        by = c("id" = "id_salida")
      )%>%
        rename(id_salida = tipo),
      pagFetch("tipo_salida", cancelado = FALSE),
      by = c("id_salida" = "id")
    )%>%
      select(-id_salida)
    
    
    
    
    #telefonos = myfetch("productoresContactos", con = con, source = "PGS"),
    
    #pallets = myfetch("pallets", con = con, source = "PGS"),
    
    #pallets_reg = myfetch("pallets_reg", con = con, source = "PGS")
    
  )#, parent = baseenv()
)


getProducto <- function( df, 
                         productos = pagEnv$productos, 
                         campos = c("nombre_producto", "clave_fruta", "clave_presentacion"),
                         copy = FALSE,
                         clave_producto = "clave_producto"){
  
  nombres <- names(df)
  campos <- setdiff(campos, nombres)
  
  if(copy){
    left_join(
      df,
      productos%>%
        collect(),
      by = c("clave_producto" = "clave_producto")
    )%>%
      select(one_of(c(nombres, campos)))
    
  }else{
    left_join(
      df,
      productos,
      by = c("clave_producto" = "clave_producto")
    )%>%
      select(one_of(c(nombres, campos)))
  }
  
  
}


getPresentacion <- function( df, envir = pagEnv, 
                             campos = c("cantidad", "peso", "medida", "presentacion")){
  
  nombres <- names(df)
  campos <- setdiff(campos, nombres)
  
  
    left_join(
      df,
      envir$presentaciones%>%
        collect(),
      by = c("clave_presentacion" = "clave_presentacion")
    )%>%
      mutate(presentacion = paste0(cantidad, "X", peso, medida))%>%
      select(one_of(c(nombres, campos)))
  
}

getFruta <- function( df, envir = pagEnv, 
                      campos = c("nombre_fruta", "nombre_corto", "variedad",
                                 "variedad_corto", "tipo", "tipo_corto"),
                      copy = FALSE){
  
  nombres <- names(df)
  campos <- setdiff(campos, nombres)
  
  if(copy){
    left_join(
      df,
      envir$fruta%>%
        collect(),
      by = c("clave_fruta" = "clave_fruta")
    )%>%
      select(one_of(c(nombres, campos)))
    
  }else{
    left_join(
      df,
      envir$fruta,
      by = c("clave_fruta" = "clave_fruta")
    )%>%
      select(one_of(c(nombres, campos)))
  }
  
  
}

saldosClaveProducto <- function(df, 
                                   Clave_producto = "clave_producto", 
                                   Cantidad = "cantidad"){
  df <- as.data.frame(df)
  totales  <- data.frame(clave_producto = df[,c(Clave_producto)], Cantidad = as.numeric(df[,c(Cantidad)]))%>%
    ddply(.(clave_producto), summarize, Total = sum(Cantidad))
  
  if(length(totales[[1]]) == 0){
    
    return(data.frame(presentacion = "6OZ", Total = 0))
    
  } else{
    
    rbind(
      totales%>%
        getProducto(campos = c("clave_presentacion"), copy = TRUE)%>%
        getPresentacion()%>%
        #collect()%>%
        mutate(presentacion = paste0(cantidad,"X",peso, medida))%>%
        mutate(presentacion = ifelse(presentacion == "12X125G", "12X4.4OZ", presentacion))%>%
        ddply(.(presentacion), summarize, Total = sum(Total)),
      
      totales%>%
        getProducto(campos = c("clave_presentacion"), copy = TRUE)%>%
        getPresentacion()%>%
        mutate(Cantidad = Total*cantidad,
               presentacion = paste0(peso, medida))%>%
        select(presentacion, Cantidad)%>%
        mutate(presentacion = ifelse(presentacion == "125G", "4.4OZ", presentacion))%>%
        ddply(.(presentacion), summarize, Total = sum(Cantidad, na.rm = TRUE))
    )
  }
}  



getMaterial <- function( df, envir = pagEnv, 
                      campos = c("clave_material", "tipo", "marca", "descripcion",
                                 "unidad_medida", "clave_presentacion", "precio_venta"),
                      copy = FALSE){
  nombres <- names(df)
  campos <- setdiff(campos, nombres)
  
  if(copy){
    left_join(df, 
              envir$material%>%
                collect(),
              by = c("clave_material" = "clave_material")
    )%>%
      select(one_of(nombres, campos))
  }else{
    left_join(df, 
              envir$material,
              by = c("clave_material" = "clave_material")
    )%>%
      select(one_of(c(nombres, campos)))
  }
  
}

saldosClaveMaterial <- function(df, clave_material = "clave_material", Cantidad = "cantidad",
                                copy = TRUE){
  df <- as.data.frame(df)
  totales <- data.frame(clave_material = df[,c(clave_material)], 
                        Cantidad = c(df[,c(Cantidad)]))%>%
    getMaterial(campos = c("clave_presentacion", "tipo"), copy = copy)%>%
    filter(tipo %in% c("CAJA EMP.","CLAMSHELL" ))%>%
    getPresentacion()%>%
    mutate(presentacion = ifelse(tipo == "CLAMSHELL", paste0(peso, medida), 
                                 paste0(cantidad, "X", peso, medida)))%>%
    mutate(presentacion = ifelse(presentacion == "12X125G", "12X4.4OZ", 
                                 ifelse(presentacion == "125G", "4.4OZ", presentacion)))%>%
    ddply(.(presentacion), summarize, Total = sum(as.numeric(Cantidad), na.rm = TRUE))
  
  if(length(totales[[1]]) == 0){
    return(data.frame(presentacion = "6OZ", Total = 0))
  } else{
    totales
  }
}


saldosMaterial <- function(clave = NULL,
                           desde = inicioTemporada, hasta = Sys.Date(),
                           clave_productor = NULL,
                           recepcionFruta = pagEnv$recepcion_fruta,
                           salidasMaterial = pagEnv$almacen_salida,
                           entradasMaterial = pagEnv$almacen_entrada,
                           contadoProducto = NULL){
  library(purrr)
  library(tidyr)
  
  precios <- readRDS("precios_ref.RDS")%>%
    rename(presentacion = Presentacion)
  
  thismerge <-  function(x){
    df <- x[[1]]
    for(i in 2:length(x)){
      df <- merge(df,x[[i]], by = "presentacion", all = TRUE)
    }
    df[is.na(df)] <- 0
    df
  }
  sumas <- function(clave){
    datos <- list(
      
        prestamos = salidasMaterial%>%
          as.data.frame()%>%
          rename(clave_productor = clave_prov)%>%
          filter(clave_productor == clave)%>%
          collect()%>%
          saldosClaveMaterial()%>%
          rename(Prestamos = Total),
      
      entradas_fruta = recepcionFruta%>%
        as.data.frame()%>%
        filter(clave_productor == clave)%>%
        collect()%>%
        rename(cantidad = cantidad_aceptada)%>%
        saldosClaveProducto()%>%
        rename(Recepcion = Total),
      
      contadoProducto = {
        if(is.null(contadoProducto)  || length(contadoProducto[,1]) == 0){
          data.frame(presentacion = "6OZ", Devoluciones_extras = 0)
        }else{
          
          contadoProducto%>%
            filter(clave_productor == clave)%>%
            collect()%>%
            #rename(cantidad = cantidad_aceptada)%>%
            saldosClaveProducto()%>%
            rename(Devoluciones_extras = Total)
        }
      },
      
      devoluciones = {
        
        
        dev <-   entradasMaterial%>%
          #filter(tipo_entrada == "DEVOLUCION")%>%
          rename(clave_productor = clave_prov)%>%
          filter(clave_productor == clave)%>%
          collect()
        
        if(length(data.frame(dev)[,1]) == 0){
          
          data.frame(presentacion = "6OZ", Devoluciones = 0)
          
        }else{
          
          dev%>%
            saldosClaveMaterial()%>%
            mutate(Devoluciones = Total)%>%
            select(-Total)
        }
      })
    
    
    x <-  thismerge(datos)%>%
      filter(!presentacion %in% c("2LB", "6X2LB"))%>%
      mutate(Saldo = Prestamos - Recepcion - Devoluciones - Devoluciones_extras)%>%
      merge(precios, by = "presentacion", all = TRUE)%>%
      mutate(Imp = Saldo * Precio)
    
    x[is.na(x)] <- 0
    
    x
  }
  if(is.null(clave)){
    
    recepcionFruta = pagEnv$recepcion_fruta%>%
      collect()%>%
      ddply(.(clave_producto, clave_productor), 
            summarize, cantidad_aceptada = sum(cantidad_aceptada))
    
    salidasMaterial = pagEnv$almacen_salida%>%
      collect()%>%
      ddply(.(clave_prov, clave_material),
             summarize, cantidad = sum(cantidad))
    
    entradasMaterial = pagEnv$almacen_entrada%>%
      collect()%>%
      ddply(.(clave_prov, clave_material),
            summarize, cantidad = sum(cantidad))
    
    productores <- unique(
      bind_rows(
      entradasMaterial%>%transmute(clave_productor = clave_prov)%>%collect()%>%unique(),
      salidasMaterial%>%transmute(clave_productor = clave_prov)%>%collect()%>%unique(),
      recepcionFruta%>%select(clave_productor)%>%collect()%>%unique(),
      contadoProducto%>%select(clave_productor)%>%unique()
    ))%>%
      filter(clave_productor !=0)
    
    sumas2 <- function(x){
      
      suma <- sumas(x)
      
      imp <- sum(suma$Imp, na.rm = TRUE)
      
      suma%>%
        select(presentacion, Saldo )%>%
        rbind(data.frame(presentacion = "Imp", Saldo = imp))%>%
        mutate(Clave_productor = x)
    }
    
     map(productores$clave_productor, sumas2)%>%
       map_df(rbind)%>%
       spread(value = Saldo, fill = 0, key = presentacion)
     
         
     }else{
    sumas(clave)
  }
}





