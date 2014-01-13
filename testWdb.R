source("readVerifWdb.R")

# Examples of use
#
# find all models(shortmodelname) for station 1493, 1/3-2010, parameter wind speed(FF):
# findAvailableModels(1493,20100301,"FF")
#
# find all parameters(miopdb_par) for station 1493, 1/3-2010, model Hirlam 8
# findAvailableParameters(1493,20100301,"H8")
#
# find all stations(wmo_no) for  1/3-2010, parameter wind speed(FF), model Hirlam 8
# findAvailablePlaces(20100301,"FF","H8")
#
# find all referencetimes for station 1493, parameter wind speed(FF), model Hirlam 8
# findAvailableReferencetimes(1493,"FF","H8")
#
# find all proglenghts for station 1493,1/3-2010, parameter wind speed(FF), model Hirlam 8
# findAvailableProglengths(1493,20100301,"FF","H8")

findAvailableModels<-function(wmo_no=NULL,date=NULL,prm=NULL){
# returns vector of available models
  startup()
  query <- "SELECT dataprovidername FROM wci.browse( NULL,WMO_NO ,TIME,NULL, PARAMETER,NULL, NULL,NULL::wci.browsedataprovider )"
  query <- sub("WMO_NO",wmostring(wmo_no) ,query)
  query <- sub("TIME",timestring(date),query)
  query <- sub("PARAMETER",parameterstring(prm),query)
  print(query)
  rs <- dbSendQuery(con, query)
  results<<-fetch(rs,n=-1)
  if (nrow(results)==0){
    return()
  }
  dbClearResult(rs)
  vmodels<<-vector(length=0)
  if (nrow(results)==0)
    return(vmodels)
  for (i in 1:nrow(results)){
    dp <- results$dataprovidername[i]
    dpdef <- dataproviderDefinitions[dataproviderDefinitions$dataprovider==dp,]
    if (nrow(dpdef)==1){
      mod<-dpdef$shortmodelname
      vmodels<<-c(vmodels,as.character(mod))
    } else {
      vmodels<<-c(vmodels,as.character(dp))
    }
  }
  return(vmodels)
 }


findAvailableParameters<-function(wmo_no=NULL,date=NULL,model=NULL){
  startup()
 # returns vector of available parameters
  query <- "SELECT valueparametername FROM wci.browse(DATAPROVIDER,WMO_NO ,TIME,NULL, NULL,NULL, NULL,NULL::wci.browsevalueparameter )"
  query <- sub("WMO_NO",wmostring(wmo_no) ,query)
  query <- sub("TIME",timestring(date),query)
  query <- sub("DATAPROVIDER",dataproviderstring(model),query)
  print(query)
  rs <- dbSendQuery(con, query)
  results<<-fetch(rs,n=-1)
  dbClearResult(rs)
  vparams<<-vector(length=0)
  if (nrow(results)==0)
    return(vparams)
  for (i in 1:nrow(results)){
    vp <- results$valueparametername[i]
    vpdef <- parameterDefinitions[parameterDefinitions$valueparametername==vp,]
    if (nrow(vpdef)>0){
      for (j in 1:nrow(vpdef)){
        par<-vpdef[j,]$miopdb_par
        vparams <<- c(vparams,as.character(par))
      }
    } else {
      vparams<<-c(vparams,as.character(vp))
    }
  }
  return(vparams)
}


findAvailablePlaces<-function(date=NULL,prm=NULL,model=NULL){
 # returns vector of available places
  startup()
  query <- "SELECT placename FROM wci.browse( DATAPROVIDER,NULL ,TIME,NULL, PARAMETER,NULL, NULL,NULL::wci.browseplace ) order by placename"  
  query <- sub("TIME",timestring(date),query)
  query <- sub("DATAPROVIDER",dataproviderstring(model),query)
  query <- sub("PARAMETER",parameterstring(prm),query)  
  print(query)
  rs <- dbSendQuery(con, query)
  results<-fetch(rs,n=-1)
  dbClearResult(rs)
  vplaces<-vector(length=nrow(results))
  for (i in 1:nrow(results)){
    vplaces[i]<-results$placename[i]
  }
  return(vplaces)
 }


findAvailableReferencetimes<-function(wmo_no=NULL,prm=NULL,model=NULL){
 # returns vector of available reftimes
  query <- "SELECT distinct(to_char(referencetime,'YYYYMMDDHH24')) as time FROM wci.browse( DATAPROVIDER,WMO_NO ,NULL,NULL, PARAMETER,NULL, NULL,NULL::wci.browsereferencetime) order by time"        
  query <- sub("DATAPROVIDER",dataproviderstring(model),query)
  query <- sub("PARAMETER",parameterstring(prm),query)
  query <- sub("WMO_NO",wmostring(wmo_no) ,query)
  print(query)
  rs <- dbSendQuery(con, query)
  results<-fetch(rs,n=-1)
  dbClearResult(rs)
  vreftimes<-vector(length=nrow(results))
  for (i in 1:nrow(results)){
    vreftimes[i]<-results$time[i]
  }
  vreftimes<-sort(vreftimes)
  return(vreftimes)

}


findAvailableProglengths<-function(wmo_no=NULL,date=NULL,prm=NULL,model=NULL){
 # returns vector of available progtimes
  query<-"SELECT distinct(wci.prognosishour(referencetime, validtimefrom)) as prog FROM wci.read(DATAPROVIDER,WMO_NO,TIME,NULL, PARAMETER,NULL, NULL,NULL::wci.returnfloat) order by prog"
  query <- sub("WMO_NO",wmostring(wmo_no) ,query)
  query <- sub("TIME",timestring(date),query)
  query <- sub("DATAPROVIDER",dataproviderstring(model),query)
  query <- sub("PARAMETER",parameterstring(prm),query)
  print(query)
  rs <- dbSendQuery(con, query)
  results<-fetch(rs,n=-1)
  dbClearResult(rs)
  vprogs<-vector(length=nrow(results))
  for (i in 1:nrow(results)){
    vprogs[i]<-results$prog[i]
  }

  return(sort(vprogs))
 }



# utility functions
wmostring <- function(wmo_no){
  if (is.null(wmo_no)){
    wmostring <- "NULL"    
  } else{
    wmostring <- paste("'",wmo_no,"'",sep="")
  }
  return(wmostring)
}

timestring <- function(date){
  if (is.null(date)){
    timestring <- "NULL"
  } else{
    time <- getFormattedTime(date)
    timestring <- paste("'",time,"'",sep="")
  }
  return(timestring)
}

parameterstring <- function(prm){
  if (is.null(prm)){
    parameterstring <- "NULL"    
  } else{
    pdef <- parameterDefinitions[parameterDefinitions$miopdb_par==prm,]
    parameter <- as.character(pdef$valueparametername)
    parameterstring <- paste("'{",parameter,"}'",sep="")
  }
  return(parameterstring)
}

dataproviderstring <- function(model){  
  if (is.null(model)){
    dataproviderstring <- "NULL";
  } else {  
    dpdef <- dataproviderDefinitions[dataproviderDefinitions$shortmodelname==model,]
    dataprovider <- dpdef$dataprovider
    dataproviderstring <- paste("ARRAY['",dataprovider,"']",sep="")
  }
  return(dataproviderstring)
}
