# before running this script install packages from CRAN
# in R:
# > install.packages("RPostgreSQL","/path/to/library")
# > library(DBI,lib.loc="/path/to/library")
# > library(RPostgreSQL,lib.loc="path/to/library")
#

# Example of use:
# connect to database
# startup(dev3)
# mydf<-readVerifWdb(c(10800,10380,17250),c(20120131,20120203),c("DEF","APP","LOC"),c("TT","P"), c(18,36))
# mydf<-readVerifWdb(c(1317,1492),c(20120128,20120130),c("DEF","APP"),c("TT","P"),NULL)
# mydf<-readVerifWdb(wmo_no=c(1317,1492),period=NULL,model=c("DEF","RAW","H12"),parameters=c("TT","P"),prg=c(2,4,6))
# more calls to readVerifWdb
# ....
# close connecion when finished
# finish()

library(udunits2)
library(DBI)
library(RPostgreSQL)

startup<-function(db){
 #startup; load driver, connect to wdb etc
  drv<<-dbDriver("PostgreSQL")
  if (db=="dev3"){
    con<<-dbConnect(drv,  dbname="wdb",  user="wdb", host="wdb-dev3")
    rs<-suppressWarnings(dbSendQuery(con,"select wci.begin('wdb',88,456,88)"))
  }
  dbClearResult(rs)
 }


finish<-function(){
# finish; release wci, disconnect, unload driver
  rs<-suppressWarnings(dbSendQuery(con,"select wci.end()"))
  dbClearResult(rs)
  dbDisconnect(con);
  dbUnloadDriver(drv)
}


readVerifWdb<-function(wmo_no,period,model,prm,prg,lev=NULL,init.time=0,useReftime=FALSE,testMemory=FALSE,testLatency=FALSE){

  queryPart1<-"select * from (select placename as WMO_NO, to_char(validtimefrom,'YYYYMMDDHH24') as TIME, wci.prognosishour(referencetime, validtimefrom)  AS PROG"

  maxmemory<<-0
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
      locationString <-"NULL"
      if (useReftime){
        reftimeString<-getInsideTimeString(period)
        validtimeString<-"NULL"
      } else{
        reftimeString<-getInsideTimeString(period,subtracthours=300)
        validtimeString<-getInsideTimeString(period)
      }
      parameterString<-getParameterString(par)
      levelString<-getLevelString(par)
      versionString<-"NULL"
      returnString<-"NULL::wci.returnfloat)"
      init.timestring <- sprintf("%02i", init.time)
      terminString <- paste("where to_char(referencetime,'HH24') in ('",init.timestring,"')",sep="")
      whereString<-")AS wciquery where("
      stationString<-paste("WMO_NO in('",paste(wmo_no,collapse="','"),"')",sep="")      
      progString<-paste("AND PROG in(",paste(prg,collapse=","),") )",sep="")      
      
      parmod <- paste(par,mod,sep=".")
      parmodel<-paste("\"", parmod  ,"\"",sep="")
      queryPart2<- paste(",value as",parmodel, "from wci.read(")
      queryPart3<-paste(modelString,locationString,reftimeString,validtimeString,parameterString,levelString,versionString,returnString,sep=",")
      queryPart4<-paste(terminString,whereString,stationString,progString,"order by TIME")

      query<-paste(queryPart1,queryPart2,queryPart3,queryPart4)

 
      cat(query,"\n") 
      if (testLatency){
        ts<-system.time(rs <- dbSendQuery(con, query))
        tr<-system.time(results<-fetch(rs,n=-1))
        cat("Time for rs <- dbSendQuery(con, query)\n")
        print(ts)
        cat("Time for results<-fetch(rs,n=-1)\n")
        print(tr)
      }
      else{
        rs <- dbSendQuery(con, query)
        results<-fetch(rs,n=-1)
      }

      cat("Number of rows in results", nrow(results),"\n")
      cat("Size of results", object.size(results),"bytes \n")

      dbClearResult(rs)


      if (nrow(results)!=0){
        values<-as.double(unlist(results[parmod]))
        #convert values to other units
        values <- convertValues(values,par)
        results[parmod] <- values
      }
          
      if (nrow(allmodels)==0){      
        allmodels<-results
      }
      else{
        if (testLatency){
          tm<-system.time(allmodels<-merge(allmodels,results))
          cat("Time for allmodels<-merge(allmodels,results) \n")
          print(tm)
        }
        else
          if (nrow(results)!=0)      
          allmodels<-merge(allmodels,results)
      }


      #monitor memory usage
      if (testMemory){
        myobj<-sort( sapply(ls(),function(x){object.size(get(x))}), decreasing=TRUE)
        totalmem<-sum(myobj)
        cat("Total memory", totalmem,"bytes \n")
        myobj<-head(myobj,5)
        cat("5 largest objects:\n")
        print(myobj)
        if (totalmem > maxmemory)
          maxmemory<<-totalmem
      }
      
      
    }
 
  }

  
  return(allmodels)  
}


readEnsembleWdb<-function(wmo_no,period,prm,model,prg,members=NULL){
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
      levelString="NULL"
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


# Mapping from miopdb parameter name to proffdb parameter name
parameterDefinitions <- read.table("parameters.conf",sep=",",header=TRUE,stringsAsFactors=FALSE)

# Mapping from miopdb model name to proffdb dataprovider name
dataproviders<-c("proff.default","proff.approved","proff.raw","proff.h12","locationforecast","h8","um4","pgen_percentile yr")
models<-c("DEF","APP","RAW","H12","LOC","H8","UM4","EPS")

names(dataproviders)<-models
names(models)<-dataproviders





getDataProviderString<-function(model){
  modelstring<-paste(dataproviders[model],collapse="','")
  dps<-paste("ARRAY['",modelstring,"']",sep="")
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
  if (!miopdb_unit==unit)
    values <- ud.convert(values,unit,miopdb_unit)
  return(values)
}


getLevelString<-function(param){
  pdef <- parameterDefinitions[parameterDefinitions$miopdb_par==param,]
  levelparametername <- as.character(pdef$levelparametername)
  defaultlevel <- as.character(pdef$defaultlevel)
  levelstring <- paste("'",defaultlevel," ",levelparametername,"'",sep="")
  return(levelstring)
}
