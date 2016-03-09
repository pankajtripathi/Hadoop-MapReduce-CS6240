#install.packages('randomForest')
library(randomForest, quietly=TRUE)
require("plyr", quietly=TRUE)
args = commandArgs(trailingOnly=TRUE)
if (length(args) < 3) {
  stop("At least three arguments must be supplied (testData, trainedData)", call.=FALSE)
}
print(args)
headers <- c("year","month","origin", "destination","carrier","crsDepTime","flightDate","flightNumber","dayOfMonth", "dayOfWeek","delay","daysTillNearestHoliday")
headersVal <- c("year","month", "joinColumn", "newDelay")
testData <- read.table(args[1], header = FALSE, sep = "\t", col.names = headers)
testData["joinColumn"] <- paste(testData$flightNumber,"_",testData$flightDate,"_",testData$crsDepTime, sep="")

validateData <- read.table(args[2], header = FALSE, sep = "\t", col.names = headersVal)
validateData <- validateData[,-c(1:2)]
testData <- join(x=testData, y=validateData, type = "inner")
trainData <-""
for (variable in args[3:length(args)]) {
  tmpfile <- read.table(variable, header = FALSE, sep = "\t", col.names = headers)
  trainData <- rbind(trainData, tmpfile)
}
trainData["newDelay"] <- (trainData$delay > 0)
trainData["joinColumn"] <- paste(trainData$flightNumber,"_",trainData$flightDate,"_",trainData$crsDepTime, sep="")
popular <- c("ATL", "ORD", "DFW", "LAX", "DEN", "IAH", "PHX", "SFO", "CLT", "DTW", "MSP", "LAS", "MCO", "EWR", "JFK", "LGA", "BOS", "SLC", "SEA", "BWI", "MIA", "MDW", "PHL", "SAN", "FLL", "TPA", "DCA", "IAD", "HOU","??")
carriers <- c( "9E","AA","AS","B6","DL","US","UA","VX","WN","YV", "EV" ,"F9", "FL", "HA", "MQ", "OO")
swapAirport <- function ( a )  if ( a %in% popular ) a else "??"
dropAll<-function(d) {
  d <- d[ !is.na(d$newDelay) ,]
  d <- d[ !is.na(d$crsDepTime) ,]
  d <- d[ !is.na(d$month) ,]
  d <- d[ !is.na(d$dayOfMonth) ,]
  d <- d[ !is.na(d$dayOfWeek) ,]
  d <- d[ !is.na(d$daysTillNearestHoliday) ,]
  
  d["R"]<- as.factor(d$newDelay)
  d["CSR"] <- as.integer(as.numeric(as.character(d$crsDepTime))/100)
  d["MONTH"] <- d$month
  d["DOM"] <- d$dayOfMonth
  d["DOW"] <- d$dayOfWeek
  d["HOL"] <- ifelse(d$daysTillNearestHoliday>=14,14,d$daysTillNearestHoliday) 
  d["ORG"] <- factor(sapply(as.character(d$origin),swapAirport),popular)
  d["DEST"] <- factor(sapply(as.character(d$destination),swapAirport),popular)
  d["UNIQUE_CARRIER"] <- factor(as.character(d$carrier),carriers)
  
  d <- d[ !is.na(d$UNIQUE_CARRIER),]
  d <- d[ !is.na(d$DOW) ,]
  d <- d[ !is.na(d$DOM) ,]
  d <- d[ !is.na(d$MONTH) ,]
  d <- d[ !is.na(d$CSR) ,]
  d <- d[ !is.na(d$HOL) ,]
  d <- d[ !is.na(d$R) ,]
  d[,-c(1:14)]
}
y <- dropAll(trainData)
ytest <- dropAll(testData)
levels(ytest$UNIQUE_CARRIER) <- levels(y$UNIQUE_CARRIER)
levels(ytest$ORG) <- levels(y$ORG) 
levels(ytest$DEST) <- levels(y$DEST) 
levels(ytest$R) <- levels(y$R) 
m <- randomForest(R ~ ., data=y, ntree=10, nodesize=1000)
p <- predict(m, ytest)

data<-ytest
ac <- length(data$R)
tc <- length(data[data$R==TRUE,]$R)
fc <- length(data[data$R==FALSE,]$R)
err <- length(data[ p == TRUE & data$R == FALSE,]$R)
err2 <-length(data[ p == FALSE & data$R == TRUE,]$R)
print(paste("ac=", ac))
print(paste("tc=", tc))
print(paste("fc=", fc))
print(paste("true to false err=", err))
print(paste("false to true err=", err2))

print(paste("pred Ontime act Late", round(err/tc,2), " pred Late act Ontime",round(err2/fc,2), " total ", round((err+err2)/ac,2)))
varImpPlot(m,main="RandomForest")

x<- paste(testData[1,2], "\tac=", ac, "\ttc=", tc ,"\tfc=", fc, "\ttrue to false err=", err, "\tfalse to true err=", err2, "\tpred Ontime act Late", round(err/tc,2), "\tpred Late act Ontime",round(err2/fc,2), "\ttotal ", round((err+err2)/ac,2),"\n")
print (x)
write(x, "predictionResults", append = TRUE)
file.show("predictionResults")

#cat(ytest$fl,  predict(m, ytest) 
#write(predict(m, ytest) == ytest$R, file = "sss" , append = TRUE)