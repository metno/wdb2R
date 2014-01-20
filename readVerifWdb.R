# before running this script install packages from CRAN
# in R:
# > install.packages("DBI")
# > install.packages("RPostgreSQL")
# > install.packages("udunits2")
#

# Example of use:
# connect to database:
# > startup(user, host, database)
# run queries:
# > mydf<-readVerifWdb(c(1003),c(20120131,20120203),c("EC"),c("TT","P"), c(18,36))
# > mydf<-readVerifWdb(c(1493),c(20100131,20100203),c("EC","UM4"),c("FF","DD"), NULL)
# > mydf<-readVerifWdb(c(1317),c(1990128,19990130),c("H50","H10"),c("DD","FF"),prg=seq(3,21,3))
# ... more calls to readVerifWdb ...
# close connecion when finished
# > finish()



# define parameters
DD<-c("DD","degrees","wind from direction","air pressure","height above ground","10","radians")
FF<-c("FF","m/s","wind speed","air pressure","height above ground","10","m/s")
TT<-c("TT","celsius","air temperature","air pressure","height above ground","2","kelvin")
TT_K<-c("TT_K","celsius","kalman air temperature","air pressure","height above ground","2","kelvin")
TT.K<-c("TT.K","celsius","kalman air temperature","air pressure","height above ground","2","kelvin")
TD<-c("TD","celsius","dew point temperature","air pressure","height above ground","2","kelvin")
RR<-c("RR","mm","lwe thickness of precipitation amount","air pressure","height above ground","0","m")
P<-c("P","hPa","air pressure at sea level",NA,"height above ground","0","Pa")
N<-c("N","%","cloud area fraction",NA,"air pressure","100000","%")
CH<-c("CH","%","cloud area fraction in atmosphere layer",NA,"air pressure","30000","%")
FG<-c("FG","%","cloud area fraction in atmosphere layer",NA,"air pressure","100000","%")
CL<-c("CL","%","cloud area fraction in atmosphere layer",NA,"air pressure","85000","%")
CM<-c("CM","%","cloud area fraction in atmosphere layer",NA,"air pressure","50000","%")
UU<-c("UU","%","relative humidity","air pressure",NA,NA,"%")
Z<-c("Z",NA,"atmosphere sigma coordinate","air pressure",NA,NA,NA)
parameterDefinitions<-data.frame(rbind(DD,FF,TT,TT_K,TT.K,TD,RR,P,N,CH,FG,CL,CM,UU,Z))
colnames(parameterDefinitions)<-c("miopdb_par","miopdb_unit","valueparametername","levelparametername","groundlevelparametername","groundlevel","unit")

NIVA<-c("NIVA","hPa","air pressure","Pa")
levelparameterDefinitions <-data.frame(rbind(NIVA))
colnames(levelparameterDefinitions)<-c("miopdb_par","miopdb_unit","levelparametername","unit")


# definitions of dataproviders
hirlam50<-c("hirlam50km_interpolated_v0","H50")
hirlam10<-c("hirlam10km_interpolated_v0","H10")
hirlam20<-c("hirlam20km_interpolated_v0","H20")
hirlam4<-c("hirlam4km_interpolated_v0","H4")
hirlam8<-c("hirlam8km_interpolated_v0","H8")
hirlam12<-c("hirlam12km_interpolated_v0","H12")
EC<-c("ecmwf15_interpolated_v0","EC1_LOW")
ECMWF<-c("ecmwf_interpolated_v0","EC")
UK<-c("uk_global_interpolated_v0","UK")
UM4KM1<-c("unified_model4km_interpolated_v0","UM4")
dataproviderDefinitions<-data.frame(rbind(hirlam50,hirlam10,hirlam20,hirlam4,hirlam8,hirlam12, EC,ECMWF,UK,UM4KM1))
colnames(dataproviderDefinitions)<-c("dataprovider","shortmodelname")

started <<-FALSE 

startup<-function(user, host, dbname){
  if (started)
    return(TRUE)
  if (!require(udunits2)) {
    cat("The udunits2 package is not available.\n")
    return(FALSE)
  }   
  if (!require(DBI)) {
    cat("The DBI package is not available.\n")
    return(FALSE)
  }       
  if (!require(RPostgreSQL)) {
    cat("The RpostgreSQL package is not available.\n")
    return(FALSE)
  }
 #wdb stuff  
  namespace <- "88,42,88"
 #startup; load driver, connect to wdb etc
  drv<<-dbDriver("PostgreSQL")
  con<<-dbConnect(drv,  dbname=dbname,  user=user, host=host)
  sendquery <- "select wci.begin('wdb',88,42,88)"
  rs<-suppressWarnings(dbSendQuery(con,sendquery))
  dbClearResult(rs)
  started<<-TRUE
  return(TRUE)
}


finish<-function(){
 # finish; release wci, disconnect, unload driver
  rs<-suppressWarnings(dbSendQuery(con,"select wci.end()"))
  dbClearResult(rs)
  dbDisconnect(con);
  dbUnloadDriver(drv)
}


readVerifWdb<-function(wmo_no,period,model,prm,prg,lev=NULL,init.time=0,useReftime=FALSE){

  if (!started){
    cat("Please call startup(user, host, database) before using this function.\n")
    return(FALSE)
  }
  
  if (length(wmo_no)!=1){
    cat("Length of wmo_no!=1\n")
    return()
  }

  queryPart1<-"select * from (select placename as WMO_NO, to_char(validtimefrom,'YYYYMMDDHH24') as TIME, wci.prognosishour(referencetime, validtimefrom)  AS PROG"
  if (!is.null(lev))
    queryPart1 <- paste(queryPart1,",levelfrom as LEV")

  
  #create empty data frame to hold all models
  allmodels<-data.frame()
# loop over models
  for (mod in model){
    for (par  in prm){

    # each model/parameter combo results in a dataframe with rows like this
    # WMO_NO, TIME,PROG, TT.EC
    # which are then merged together to something like
    # WMO_NO, TIME,PROG, TT.EC TT.UM FF.EC FF.UM        
      modelString<-getDataProviderString(mod)
      locationString <-paste("'",wmo_no,"'",sep="")
      if (useReftime){
        reftimeString<-getInsideTimeString(period)
        validtimeString<-"NULL"
      } else{
        reftimeString<-getInsideTimeString(period,subtracthours=300)
        validtimeString<-getInsideTimeString(period)
      }
      parameterString<-getParameterString(par)
      levelString<-getLevelString(par,lev)
      versionString<-"NULL"
      returnString<-"NULL::wci.returnfloat)"
      init.timestring <- sprintf("%02i", init.time)
      terminString <- paste("where to_char(referencetime,'HH24') in ('",init.timestring,"')",sep="")
      whereString<-")AS wciquery "
      if(!is.null(prg)){
        progString<-paste("where (PROG in(",paste(prg,collapse=","),") )",sep="")      
      } else {
        progString <- ""
      }
      parmod <- paste(par,mod,sep=".")
      parmodel<-paste("\"", parmod  ,"\"",sep="")
      queryPart2<- paste(",value as",parmodel, "from wci.read(")
      queryPart3<-paste(modelString,locationString,reftimeString,validtimeString,parameterString,levelString,versionString,returnString,sep=",")
      queryPart4<-paste(terminString,whereString,progString,"order by TIME")
      
      query<-paste(queryPart1,queryPart2,queryPart3,queryPart4)

      cat(query,"\n") 
      rs <- dbSendQuery(con, query)
      results<-fetch(rs,n=-1)
 
      cat("Number of rows in results", nrow(results),"\n")
      cat("Size of results", object.size(results),"bytes \n")

      dbClearResult(rs)
     
      if (nrow(results)!=0){
        values<-as.double(unlist(results[parmod]))
        #convert values to other units
        values <- convertValues(values,par)
        results[parmod] <- values
        levs <- results$lev
        if (length(levs)!=0){
          levs <- convertLevels(levs,par)
          results$lev <- levs
        }
      }
          
      if (nrow(allmodels)==0){      
        allmodels<-results
      }
      else{
        if (nrow(results)!=0){      
          allmodels<-merge(allmodels,results)
        }
        
      }
      
    }
  }
  
    
  return(allmodels)  
}



readVerifWdbMultipleStations<-function(wmo_no,period,model,prm,prg,lev=NULL,init.time=0,useReftime=FALSE){

  if (!started){
    cat("Please call startup(user, host, database) before using this function.\n")
    return(FALSE)
  }
  
  #create empty data frame to hold all models
  allmodels<-data.frame()
  

  for (mod in model){
    for (par  in prm){

  
      query <- "
SELECT
        placename as WMO_NO, to_char(validtimeto,'YYYYMMDDHH24') as TIME, wci.prognosishour(referencetime, validtimeto) AS PROG, value as PARMODELSTRING
FROM
        wci.floatvalue,
        wci.dataprovidergroups g
WHERE
        validtimeto-referencetime in (PROGSTRING) AND
        extract(hour from referencetime) in (0) AND
        placename in (WMOSTRING) AND
        valueparametername in (PARAMETERSTRING) AND
        referencetime >= 'REFSTARTTIME' AND
        referencetime <= 'REFENDTIME' AND
        validtimeto >= 'VALIDSTARTTIME' AND
        validtimefrom <= 'VALIDENDTIME' AND
        dataprovidername=g.child AND
        g.parent in (DATAPROVIDERSTRING) order by wmo_no,validtimeto"
      

      parmod <- paste(par,mod,sep=".")
      parmodelString<-paste("\"", parmod  ,"\"",sep="")
      query <- sub("PARMODELSTRING",parmodelString,query)
      progString<-paste(paste("'",prg,"hours'",collapse=","))
      query <- sub("PROGSTRING",progString,query)
      wmoString<-paste(paste("'",wmo_no,"'",sep=""),collapse=",")
      query <- sub("WMOSTRING",wmoString,query)
      fstarttime<-getFormattedTime(period=period[1],subtracthours=max(prg))
      fendtime<-getFormattedTime(period=period[2],addDay=TRUE)
      query <- sub("REFSTARTTIME",fstarttime,query)
      query <- sub("REFENDTIME",fendtime,query)
      validstarttime<-getFormattedTime(period=period[1])
      validendtime<-getFormattedTime(period=period[2],addDay=TRUE)
      query <- sub("VALIDSTARTTIME",validstarttime,query)
      query <- sub("VALIDENDTIME",validendtime,query)
      dataprovider<-dataproviderDefinitions[dataproviderDefinitions$shortmodelname==mod,]$dataprovider
      dataproviderstring<-paste("'",dataprovider,"'",sep="")
      query <- sub("DATAPROVIDERSTRING",dataproviderstring,query)
      pdef <- parameterDefinitions[parameterDefinitions$miopdb_par==par,]
      valueparametername <- as.character(pdef$valueparametername)
      parameterstring<-paste("'",valueparametername,"'",sep="")
      query <- sub("PARAMETERSTRING",parameterstring,query)
      
      
      cat(query,"\n") 
      rs <- dbSendQuery(con, query)
      results<-fetch(rs,n=-1)
      dbClearResult(rs)

      if (nrow(results)!=0){
        values<-as.double(unlist(results[parmod]))
        #convert values to other units
        values <- convertValues(values,par)
        results[parmod] <- values
        levs <- results$lev
        if (length(levs)!=0){
          levs <- convertLevels(levs,par)
          results$lev <- levs
        }
      }
      
      if (nrow(allmodels)==0){      
        allmodels<-results
      }
      else{
        if (nrow(results)!=0){      
          allmodels<-merge(allmodels,results)
        }
        
      }

    }
  }
      
  return(allmodels)  

  
}


readEnsembleWdb<-function(wmo_no,period,prm,model,prg,lev=NULL,members=NULL){
  # example call
  # readEnsembleWdb(wmo_no=c(10380,98790),period=c(20120310,20120312),model=c("EPS"),parameters=c("TT"),prg=c(12,18))->mydf
  # readEnsembleWdb(wmo_no=c(18700),period=c(20120310,20120312),model=c("EPS"),parameters=c("TCC"),prg=c(12),members=c(0:10))-> mydf
  
  # selection of ref and valid time, same for all queries
  reftimeString<-gsub("'","''",getInsideTimeString(period))
  validtimeString<-"NULL"
  locationString <-"NULL"
  
  # selection of stations and progs, same for all queries
  whereString<-" AS wciquery where "
  orderString <-  " order by rowid,dataversion'"
  stationString<-paste("wmo_no in(''",paste(wmo_no,collapse="'',''"),"'')",sep="")
  progString<-paste("AND prog in(",paste(prg,collapse=","),")",sep="")
  queryPart4<-paste(whereString,stationString,progString,orderString)

  if (is.null(members))
    # select all versions
    versionString="NULL"
  else
    versionString<-paste("ARRAY[", paste(members,collapse=","),"]",collapse="")
 
  
  allmodels<-data.frame()
  
  for (mod in model){
    for (par  in prm){
    # each model/parameter combo results in a dataframe with rows like this
    # WMO_NO, TIME,PROG, TT.EC.1 TT.EC.2 TT.EC.3 ........       TT.EC.50
    # which are then merged together to something like
    # WMO_NO, TIME,PROG, TT.EC.1 TT.EC.T2....  TCC.EC.1 TCC.EC.2 TCC.EC3...


      # create source_sql string from wmo_no, period, models, prm, prg
      # query which gives all the data + an extra row id column
      # the row id is for classification
      queryPart1 <-"'select * from (select (placename||'',''||to_char(validtimefrom,''YYYYMMDDHH24'')||'',''||wci.prognosishour(referencetime, validtimefrom)||'',''||levelfrom) as rowid , placename as wmo_no, to_char(validtimefrom,''YYYYMMDDHH24'') as time, wci.prognosishour(referencetime, validtimefrom) as prog,
  dataversion,  to_char(value,''9999D99'') "
      queryPart2<- "from wci.read("
      modelString<-gsub("'","''",getDataProviderString(mod))
      parameterString<-gsub("'","''",getParameterString(par))
      levelString <- gsub("'","''",getLevelString(par,lev))
      returnString<-"NULL::wci.returnfloat)"
      queryPart3<-paste(modelString,locationString,reftimeString,validtimeString,parameterString,levelString,versionString,returnString,sep=",")
      source_sql<-paste(queryPart1,queryPart2,queryPart3,")",queryPart4)
      
      # query which gives the version
      queryCategoryPart1 <- "' SELECT distinct(dataversion)";
      category_sql <-paste(queryCategoryPart1,queryPart2,queryPart3,"order by dataversion'")


      if (is.null(members)){
        # find number of dataversions=ensemble members 
        queryCountPart1 <- "SELECT count(distinct(dataversion))"
        count_sql <-gsub("''","'",paste(queryCountPart1,queryPart2,queryPart3))
        cat("\n",count_sql,"\n")
        rs <- dbSendQuery(con, count_sql )
        results<-fetch(rs,n=-1)
        nversion<- results[1,1]-1
        members<-c(0:nversion)
      }
        
      # columns to sort into, depends on how many ensembleversions we have    
      parmod <- paste(par,mod,sep=".")
      parmodel<- paste("\"",parmod,".",sep="")
      floatparmodel<-paste("\" float,", parmodel)
      colstring <- paste(members,collapse=floatparmodel)
      colstring <- paste(parmodel,colstring,"\" float",sep="")
      asct_sql <- paste("as ct(rowid text,wmo_no text, time text, prog int,",colstring,  ")")

      # the final crosstab query
      query<-paste("select * from crosstab(",source_sql, ",", category_sql, ")", asct_sql,sep="")

      cat("\n",query,"\n") 
      rs <- dbSendQuery(con, query)
      results<-fetch(rs,n=-1)
      results<-results[,-1]



      if (nrow(results)!=0){
        for (i in 1:ncol(results)){
          #check if this is a parameter column
          if (length(grep(parmod,names(results)[i],TRUE,TRUE))>0) {
            values<-as.double(unlist(results[,i]))
            #convert values to other units
            values <- convertValues(values,par)
            results[,i] <- values
          }
        }
      }
      
      if (nrow(allmodels)==0){      
        allmodels<-results
      }
      else{
        allmodels<-merge(allmodels,results)
      }
      
      
    }
  }


  return(allmodels)
 #remove first column which is just to sort by
 #merge models, parameters
}




getInsideTimeString<-function(period,subtracthours=0){
  if (is.null(period)){
    #use last week
    today<-Sys.Date()
    lastweek<-today-6
    period<-c(format(lastweek,"%Y%m%d"),format(today,"%Y%m%d"))
  }
  if (length(period)==1) period=c(period,period)	
  # now length of period is at least two (use only the first two entries)
  fstarttime<-getFormattedTime(period=period[1],subtracthours=subtracthours)
  fendtime<-getFormattedTime(period=period[2],addDay=TRUE)
  rtstring<-paste("'inside ",fstarttime," TO ",fendtime,"'",sep="")
  return(rtstring)
}


getFormattedTime<-function(period,addDay=FALSE,subtracthours=0){
  # get formatted time ie 2012-01-28 00:00:00  from period ie 20120128
  # if addDay=true, add one day
  timestring<-paste(period,00)
  time<-strptime(timestring,"%Y%m%d%H")
  if (addDay)time<-time+23*3600
  if (subtracthours!=0) time <- time-subtracthours*3600
  ftime<-format(time,"%Y-%m-%d %H:%M:%S")
  return(ftime)
}

getDataProviderString<-function(model){
  dataprovider<-dataproviderDefinitions[dataproviderDefinitions$shortmodelname==model,]$dataprovider
  dps<-paste("ARRAY['",dataprovider,"']",sep="")
  return (dps)	
}


getParameterString<-function(param){
  pdef <- parameterDefinitions[parameterDefinitions$miopdb_par==param,]
  valueparametername <- as.character(pdef$valueparametername)
  parameterstring<-paste("ARRAY['",valueparametername,"']",sep="")
  return (parameterstring)	
}


convertValues <- function(values,param){
  pdef <- parameterDefinitions[parameterDefinitions$miopdb_par==param,]
  miopdb_unit<- as.character(pdef$miopdb_unit)
  unit<- as.character(pdef$unit)
  if (!is.na(unit)&&!miopdb_unit==unit)
    values <- ud.convert(values,unit,miopdb_unit)
  return(values)
}
  
getLevelString<-function(param,lev){
  levelstring <- "NULL"
  return(levelstring)
  pdef <- parameterDefinitions[parameterDefinitions$miopdb_par==param,]

  if (nrow(pdef)!=0) {
    if (is.null(lev)){
      levelparametername <- as.character(pdef$groundlevelparametername)
      level <- as.character(pdef$groundlevel)
    } else{
      levelparametername <- as.character(pdef$levelparametername)
      levpdef <- levelParameterDefinitions[levelParameterDefinitions$levelparametername==levelparametername,]
      #check if we need to convert level from miopdbunit
      if (nrow(levpdef)!=0) {
        miopdb_levelunit<- as.character(levpdef$miopdb_unit)
        levelunit<- as.character(levpdef$unit)
        if (!is.na(levelunit)&&!miopdb_levelunit==levelunit){
          lev <- ud.convert(lev,miopdb_levelunit,levelunit)
        }
      }
      level <- format(lev,scientific=FALSE)
      
    }

    
    levelstring <- paste("'",level," ",levelparametername,"'",sep="")  
  }
  return(levelstring)
}



convertLevels <- function(levs,param){
  pdef <- parameterDefinitions[parameterDefinitions$miopdb_par==param,]
  if (nrow(pdef)!=0) {
    levelparametername <- as.character(pdef$levelparametername)
    levpdef <- levelParameterDefinitions[levelParameterDefinitions$levelparametername==levelparametername,]
    #check if we need to convert level from miopdbunit
    if (nrow(levpdef)!=0) {
      miopdb_levelunit<- as.character(levpdef$miopdb_unit)
      levelunit<- as.character(levpdef$unit)
      if (!is.na(levelunit)&&!miopdb_levelunit==levelunit){
        levs <- ud.convert(levs,levelunit,miopdb_levelunit)
      }

    }
  }
  return(levs)
}
