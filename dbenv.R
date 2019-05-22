con <- DBI::dbConnect(drv=RPostgres::Postgres(),
                      host = "localhost", 
                      dbname = "ALFIN", 
                      user = "postgres", 
                      password = "magadan13", 
                      port = 5432)
 
con2 <- DBI::dbConnect(odbc::odbc(),
                       Driver   = "PostgreSQL Unicode(x64)",
                       Server   = "localhost",
                       Database = "ALFIN",
                       UID      = "postgres",
                       PWD      = "magadan13",
                       Port     = 5432)

myfetch <- function(nombre, con = con, source = "RDS"){
  
  if(source == "RDS"){
    var <- read_rds(paste0("proyecto/", nombre,".RDS"))
  }else{
    tbl(con, nombre)
  }
  
}


source <- "RDS"

dbenv <- list2env(
  list(
    fruEmp =   myfetch("tbRecFruEmp")%>%
      filter(strCan_cel == "NO")%>%
      transmute(Id_entradas = intNum_reg, Folio = intNum_fol, Nota = intNum_not, 
                Fecha = fecFec_not,  Clave_productor = intCla_pro, 
                Clave_acopio = intCen_aco),
    
    fruEmp_reg = myfetch("tbRecFruEmpReg")%>%
      filter(strCan_cel == "NO")%>%
      transmute(Id_entradas = intNum_reg, Idb_entradas = intNum_regA, Semana_db = intNum_sem,
                Year_db = intNum_ano,  Cantidad = as.integer(intCan_tid), Rechazadas = as.integer(intCan_rec), Clave_producto =  intCla_prod,
                Pallet = intNum_pal, Precio = as.numeric(floPre_uni), Numero_pago = intNum_pag, 
                Pagado = strPag_ado),
    
    asignaciones = myfetch("asignaciones",con = con, source = "PGS"),
    
    alias = myfetch("alias", con = con, source = "PGS"),
    
    pedidos = myfetch("pedidos", con = con, source  = "PGS") ,
    
    pedidos_reg = myfetch("pedidosReg", con = con , source = "PGS"),
    
    #productores = myfetch("tbProductores")%>%
     # filter(strCan_cel == "NO")%>%
      #transmute(Id_productor = intNum_reg,
       #         Clave_productor = intCla_pro, Nombre = strNom_bre, Apellido_paterno = strApe_pat,
        #        Apellido_materno = strApe_mat),
    
    productores = myfetch("productores", con = con, source = "PGS"),
    
    productos = myfetch("tbProductos")%>%
      filter(strCan_cel == "NO")%>%
      transmute(Clave_producto = str_pad(.$strCla_prod, 6, pad = "0"),
                Clave_grupo = intCla_gpo,
                Clave_presentacion = intCla_pre,
                Clave_fruta = intCla_fru,
                Tipo = strTip_prod,
                Activo = strAct_ivo,
                Nombre = strNom_bre),
    
    fruta = myfetch("tbFrutas")%>%
      filter(strCan_cel == "NO")%>%
      transmute(Id_fruta = intNum_reg, 
                Clave_fruta = intCla_fru,
                Nombre = strNom_bre,
                Nombre_corto = strNom_cto,
                Variedad = strVar_ied,
                Variedad_corto = strVar_cto,
                Tipo = strTip_o,
                Tipo_corto = strTip_cto,
                Activo = strAct_ivo),
    
    presentaciones = myfetch("tbPresentaciones")%>%
      filter(strCan_cel == "NO")%>%
      transmute(Clave_presentacion = intCla_pre, 
                Cantidad = as.integer(intCan_tid),
                Peso = as.numeric(intPes_o),
                Unidad = strUni_med,
                Activo = "I",
                Peso_lbs = floPes_lbs),
    
    telefonos = myfetch("productoresContactos", con = con, source = "PGS"),
    
    pallets = myfetch("pallets", con = con, source = "PGS"),
    
    pallets_reg = myfetch("pallets_reg", con = con, source = "PGS")
    
  )#, parent = baseenv()
)

dbenv$entradas_fruta <-  left_join(dbenv$fruEmp, dbenv$fruEmp_reg, 
                           by = "Id_entradas", suffix = c("h","b"))%>%
  mutate(Aceptadas = Cantidad - Rechazadas)

getProductos <- function(df, Campos = c("Clave_fruta", "Clave_presentacion",  "Nombre", "Cantidad",             
                                         "Peso", "Unidad", "Presentacion", "Nombre_fruta", "Nombre_corto", "Variedad", "Fruta",             
                                         "Producto", "Fraccion6oz")){
  
  df <- left_join(dbenv$productos%>%
                    select(-Tipo, -Activo), 
                  dbenv$presentaciones%>%
                    select(-Activo), 
                  by = "Clave_presentacion", suffix = c("h", "b"))%>%
    left_join(dbenv$fruta%>%
                select(-Nombre), by = "Clave_fruta")%>%
    collect()%>%
    mutate(Presentacion = paste0(Cantidad,"X", Peso, Unidad))%>%
    mutate(Fruta = paste(Nombre_corto,Tipo_corto))%>%
    mutate(Producto = paste0(Nombre, "_", Presentacion, "_", Fruta),
           Fraccion6oz = ifelse(Unidad == "G", Cantidad*Peso*0.035274/72  , Cantidad * Peso/72),
           Peso_total =  ifelse(Unidad == "OZ", Peso*Cantidad,
                                ifelse(Unidad =="G", Peso*Cantidad*0.035274, 
                                       ifelse(Unidad == "KG",Peso*Cantidad*35.274,
                                              ifelse(Unidad == "LB", Peso*Cantidad*16, NA)))))%>%
    right_join(df, by = "Clave_producto")
  
  return(df)
  
  
}






















