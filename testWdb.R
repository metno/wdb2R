foundAvailable<-FALSE

findAvailableModels<-function(){
 # returns vector of available models
 # query<-"SELECT dataprovidername FROM wci.browse( NULL::wci.browsedataprovider )"
  query<-"SELECT distinct(dataprovidername) FROM wci.read(NULL,NULL, NULL,NULL, NULL,NULL, NULL,NULL::wci.returnfloat)"
  rs <- dbSendQuery(con, query)
  results<-fetch(rs,n=-1)
  dbClearResult(rs)
  vmodels<-vector(length=nrow(results))
  for (i in 1:nrow(results)){
    mod<-models[results$dataprovidername[i]]
    vmodels[i]<-mod
  }
  return(vmodels[!is.na(vmodels)])
 }


findAvailableParameters<-function(){
 # returns vector of available models
 # query<-"SELECT valueparametername FROM wci.browse( NULL::wci.browsevalueparameter )"
  query<-"SELECT distinct(valueparametername) FROM wci.read(NULL,NULL, NULL,NULL, NULL,NULL, NULL,NULL::wci.returnfloat)"
  rs <- dbSendQuery(con, query)
  results<-fetch(rs,n=-1)
  dbClearResult(rs)
  vparams<-vector(length=nrow(results))
  for (i in 1:nrow(results)){
    par<-prm[results$valueparametername[i]]
    vparams[i]<-par
  }
  return(vparams[!is.na(vparams)])
}


findAvailablePlaces<-function(){
 # returns vector of available places
   query<-"SELECT distinct(placename) FROM wci.read(NULL,NULL, NULL,NULL, NULL,NULL, NULL,NULL::wci.returnfloat)"
  rs <- dbSendQuery(con, query)
  results<-fetch(rs,n=-1)
  dbClearResult(rs)
  vplaces<-vector(length=nrow(results))
  for (i in 1:nrow(results)){
    vplaces[i]<-results$placename[i]
  }
  return(vplaces)
 }


findAvailableReferencetimes<-function(){
 # returns vector of available reftimes
   query<-"SELECT distinct(to_char(referencetime,'YYYYMMDDHH12')) as time FROM wci.read(NULL,NULL, NULL,NULL, NULL,NULL, NULL,NULL::wci.returnfloat)"
  rs <- dbSendQuery(con, query)
  results<-fetch(rs,n=-1)
  dbClearResult(rs)
  vreftimes<-vector(length=nrow(results))
  for (i in 1:nrow(results)){
    vreftimes[i]<-results$time[i]
  }
  vreftimes<-sort(vreftimes)
#      vreftimes<-as.POSIXlt(vreftimes,origin="1970-01-01")
  return(vreftimes)

}


findAvailableProglengths<-function(){
 # returns vector of available progtimes
   query<-"SELECT distinct(EXTRACT(HOUR FROM (validtimeto - referencetime)) + ( EXTRACT(DAY FROM (validtimeto - referencetime) )*24 )  ) as prog FROM wci.read(NULL,NULL, NULL,NULL, NULL,NULL, NULL,NULL::wci.returnfloat)"
  rs <- dbSendQuery(con, query)
  results<-fetch(rs,n=-1)
  dbClearResult(rs)
  vprogs<-vector(length=nrow(results))
  for (i in 1:nrow(results)){
    vprogs[i]<-results$prog[i]
  }

  return(sort(vprogs))
 }


findAvailable<-function(){
  timemodels<-system.time(availableModels<<-findAvailableModels())
  timeparams<-system.time(availableParams<<-findAvailableParameters())
  timeplaces<-system.time(availablePlaces<<-findAvailablePlaces())
  timereftimes<-system.time(allreftimes<<-findAvailableReferencetimes())
  timeprogs<-system.time(availableProgs<<-findAvailableProglengths())
  cat("Time in seconds to find available models, user:",  timemodels[1],"system:",timemodels[2], "elapsed:", timemodels[3],"\n")
  cat("Time in seconds to find available parameters, user:",  timeparams[1],"system:",timeparams[2], "elapsed:", timeparams[3],"\n")
  cat("Time in seconds to find available places, user:",  timeplaces[1],"system:",timeplaces[2], "elapsed:", timeplaces[3],"\n")
  cat("Time in seconds to find available reference times, user:",  timereftimes[1],"system:",timereftimes[2], "elapsed:", timereftimes[3],"\n")
  cat("Time in seconds to find available prognosis lengths, user:",  timeprogs[1],"system:",timeprogs[2], "elapsed:", timeprogs[3],"\n")
  foundAvailable<<-TRUE
}


#get everything from database or number of weeks etc
testLatency<-function(nplaces,nweeks,nmodels,nparams,nprogs){
  if (!foundAvailable)
    findAvailable()
  models<-getSelectedVector(nmodels,availableModels)
  places<-getSelectedVector(nplaces,availablePlaces)
  params<-getSelectedVector(nparams,availableParams)
  progs<-getSelectedVector(nprogs,availableProgs)  
  reftimes<-vector(length=2)
  if (is.null(nweeks)){
    reftimes[1]=allreftimes[1]
    reftimes[2]=allreftimes[length(allreftimes)]
  }else{
    reftimes[1]<-allreftimes[1]
    tdiff<-as.difftime(nweeks,unit="weeks")
    newtime<-strptime(allreftimes[1],"%Y%m%d%H")+tdiff
    reftimes[2]<-format(newtime,"%Y%m%d%H")
  }
  timeReadVerif<-system.time(mydf<-readVerifWdb(places,reftimes,models,params,progs))
  cat("========= Test latency readVerifWdb ================ \n")

  cat("Time in seconds to read data(readVerifWdb), user:",  timeReadVerif[1],"system:",timeReadVerif[2], "elapsed:", timeReadVerif[3],"\n")
  cat("Number of rows in resulting dataframe:", nrow(mydf),"\n")
  cat("Number of columns in resulting dataframe:", ncol(mydf),"\n")
  cat("Names of columns in resulting dataframe:",names(mydf),"\n")
  cat("Number of places selected:",length(places),"\n")
  cat("Number of models selected:",length(models),"\n")
  cat("Number of parameters selected:",length(params),"\n")
  cat("Number of proglengths selected:",length(progs),"\n")

  cat("Selected places:",places,"\n") 
  cat("Selected referencetimes:", reftimes[1],"to", reftimes[2],"\n")
  cat("Selected models:",models,"\n")
  cat("Selected parameters:",params,"\n")
  cat("Selected proglengths:",progs,"\n")

  return(mydf)
  
# 

}


getSelectedVector<-function(nEntries,availableVector){
  nAv<-length(availableVector)
  if(is.null(nEntries) || nEntries>nAv){
    selectedVector<-availableVector
  } else{
    selectedVector<-availableVector[1:nEntries]
  }  
  
  return(selectedVector)
}
