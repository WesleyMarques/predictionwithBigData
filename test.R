library(SparkR, lib.loc = "/usr/local/spark/R/lib")
sc <- sparkR.init(master = "", appName = "SparkR",
            sparkHome = "/usr/local/spark", sparkEnvir = list(),
            sparkExecutorEnv = list(), sparkJars = "", sparkPackages = "")
sqlContext <- sparkRSQL.init(sc)
people <- read.df(sqlContext, "./data/dados_2.txt", "json")
#model <- glm( ~ , data = people, family = "gaussian")
summary(people)

for (variable in people) {
  variable
}
summarize()