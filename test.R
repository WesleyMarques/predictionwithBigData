library(ggplot2)
require(dplyr)
require(GGally)
require(reshape2)
library(caret)
library(SparkR, lib.loc = "/usr/local/spark/R/lib")

sc <- sparkR.init(master = "", appName = "SparkR",
            sparkHome = "/usr/local/spark", sparkEnvir = list(),
            sparkExecutorEnv = list(), sparkJars = "", sparkPackages = "")
sqlContext <- sparkRSQL.init(sc)
people <- read.df(sqlContext, "data/dados_2.json", "json")
#
people <- withColumn(people, "DIST", people$LON*0)
people <- withColumn(people, "TIME", people$LON*0)
people <- SparkR::arrange(people, asc(people$COD_LINHA), asc(people$VEIC), asc(people$DTHR))
aux <-collect(people)

aux <-transform(aux,COD_LINHA = as.integer(COD_LINHA),DTHR = as.POSIXct(DTHR, format = "%d/%m/%Y %H:%M:%S"), DIST = as.numeric(DIST), TIME = as.numeric(TIME), LAT = as.numeric(gsub( ",", ".", aux$LAT)), LON = as.numeric(gsub( ",", ".", aux$LON)))
aux<-setDistAndTime()

linha222 <- dplyr::filter(aux, aux$COD_LINHA=="222")
ggpairs(linha222)
df <- melt(linha222)
trainIndex <- createDataPartition(linha222$TIME, p = .75, list = FALSE)
train <- linha222[trainIndex,]
test <- linha222[-trainIndex,]
str(train)
model <- glm(TIME ~ ., data = train, family="gaussian")
summary(model, signif.stars=TRUE)
preds <- SparkR::predict(model, dplyr::select(test,DIST))
residuals <- collect(agg(preds,SS_res=sum(preds$)))

linha812 <- filter(aux, aux$COD_LINHA=="812")
linha822 <- filter(aux, aux$COD_LINHA=="822")
linha827 <- filter(aux, aux$COD_LINHA=="827")
dist_pontos <- function(x1,y1,x2,y2){
  return(sqrt((((x2-x1)**2)+((y2-y1)**2))))
}

time_diff <- function(date1, date2){
  return(as.numeric(difftime(date1,date2)))
}

setDistAndTime <- function(){
  dist_init <- 0
  reset <- TRUE
  veic_atual <- 0
  cod_lin_atual <- 0
  first_LAT <- 0
  first_LON <- 0
  for (variable in 1:nrow(aux)) {
    if(variable > 1 && (aux$COD_LINHA[variable] != aux$COD_LINHA[variable-1] || aux$VEIC[variable] != aux$VEIC[variable-1])){
      reset <- TRUE
    }else if(variable > 1){
      aux$DIST[variable] <- aux$DIST[variable-1]+dist_pontos(aux$LON[variable],aux$LAT[variable],aux$LON[variable-1],aux$LAT[variable-1])
      aux$TIME[variable] <- aux$TIME[variable-1]+time_diff(aux$DTHR[variable], aux$DTHR[variable-1])
      reset <- FALSE
    }
    if(reset || (first_LON == aux$LON[variable] && first_LAT == aux$LON[variable])){
      aux$DIST[variable] <- 0
      aux$TIME[variable] <- 0
      veic_atual <- aux$VEIC[variable]
      cod_lin_atual <- aux$COD_LINHA[variable]
      first_LAT <- aux$LAT[variable]
      first_LON <- aux$LON[variable]
      reset <- FALSE
    }
  }
  return(aux)
}


