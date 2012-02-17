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

library(DBI,lib.loc="/home/helenk/local/lib/R")
library(RPostgreSQL,lib.loc="/home/helenk/local/lib/R")


startup<-function(db){
 #startup; load driver, connect to wdb etc
  drv<<-dbDriver("PostgreSQL")
  if (db=="dev3"){
    con<<-dbConnect(drv,  dbname="wdb",  user="wdb", host="wdb-dev3")
    rs<-suppressWarnings(dbSendQuery(con,"select wci.begin('wdb')"))
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


readVerifWdb<-function(wmo_no,period,models,parameters,prg,testMemory=false,testLatency=false){

  queryPart1<-"select * from (select placename as WMO_NO, to_char(validtimefrom,'YYYYMMDDHH24') as TIME, wci.prognosishour(referencetime, validtimefrom)  AS PROG"

  maxmemory<<-0
  #create empty data frame to hold all models
  allmodels<-data.frame()
# loop over models
  for (model in models){
    for (par  in parameters){

    # each model/parameter combo results in a dataframe with rows like this
    # WMO_NO, TIME,PROG, TT.EC
    # which are then merged together to something like
    # WMO_NO, TIME,PROG, TT.EC TT.UM FF.EC FF.UM      
   
      modelString<-getDataProviderString(model)
      locationString <-"NULL"
      reftimeString<-getInsideTimeString(period)
      validtimeString<-"NULL"
      parameterString<-getParameterString(par)
      levelString<-"NULL"
      dataProviderString<-"ARRAY[-1]"
      returnString<-"NULL::wci.returnfloat))"
      whereString<-"AS wciquery where("
      stationString<-paste("WMO_NO in('",paste(wmo_no,collapse="','"),"')",sep="")      
      progString<-paste("AND PROG in(",paste(prg,collapse=","),") )",sep="")      


      parmodel<-paste("\"", paste(par,model,sep=".")  ,"\"",sep="")
      queryPart2<- paste(",value as",parmodel, "from wci.read(")
      queryPart3<-paste(modelString,locationString,reftimeString,validtimeString,parameterString,levelString,dataProviderString,returnString,sep=",")
      queryPart4<-paste(whereString,stationString,progString)

 
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


getInsideTimeString<-function(period){
  if (is.null(period)){
    #use last week
    today<-Sys.Date()
    lastweek<-today-6
    period<-c(format(lastweek,"%Y%m%d"),format(today,"%Y%m%d"))
  }
  if (length(period)==1) period=c(period,period)	
  # now length of period is at least two (use only the first two entries)
  fstarttime<-getFormattedTime(period[1])
  fendtime<-getFormattedTime(period[2],TRUE)
  rtstring<-paste("'inside ",fstarttime," TO ",fendtime,"'",sep="")
  return(rtstring)
}


getFormattedTime<-function(period,addDay=FALSE){
  # get formatted time ie 2012-01-28 00:00:00  from period ie 20120128
  # if addDay=true, add one day
  timestring<-paste(period,00)
  time<-strptime(timestring,"%Y%m%d%H")
  if (addDay)time<-time+24*3600
  ftime<-format(time,"%Y-%m-%d %H:%M:%S")
  return(ftime)
}


# Mapping from miopdb parameter name to proffdb parameter name
parameters<-c("air temperature","air pressure at sea level","wind speed",
              "wind from direction")
prm<-c("TT","P","FF","DD")
names(parameters)<-prm
names(prm)<-parameters

# Mapping from miopdb model name to proffdb dataprovider name
dataproviders<-c("proff.default","proff.approved","proff.raw","proff.h12","locationforecast","h8","um4")
models<-c("DEF","APP","RAW","H12","LOC","H8","UM4")
names(dataproviders)<-models
names(models)<-dataproviders

getDataProviderString<-function(model){
  modelstring<-paste(dataproviders[model],collapse="','")
  dps<-paste("ARRAY['",modelstring,"']",sep="")
  return (dps)	
}


getParameterString<-function(param){
  parameterstring<-paste(parameters[param],collapse="','")
  ps<-paste("ARRAY['",parameterstring,"']",sep="")
  return (ps)	
}




