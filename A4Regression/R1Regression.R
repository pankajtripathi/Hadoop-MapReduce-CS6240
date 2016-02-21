library(plyr)
N<-1
all_data <- read.table("hadoop-output", sep="\t",  col.names=c("airline", "year", "time", "price"), strip.white=TRUE) 
data_year_ar <- split(all_data, list(all_data$year, all_data$airline))
dataList <- NULL
for(dy in data_year_ar){
  if(nrow(dy) > 0) {
    # Retrieve each value
    time<-dy$time
    price<-dy$price
    airline<-unique(unlist(dy$airline))
    year<-unique(unlist(dy$year))
    # Calculate linear regression
    timelr<-lm(price~time)
    # Predictionsx
    time_fit<-predict(timelr, data.frame(time=(c(N))), interval="predict")
    time_diff<-as.data.frame(time_fit)
    time_predict<-sum(time_diff$fit)
    dataList <- rbind(dataList, c(year, as.character(airline), time_predict))
    #print(as.character(airline))
  }
  
}
colnames(dataList) = c("year", "airline", "fit")
df <- data.frame(dataList)

yr_split <- split(df, list(df$year))
min_list = NULL;
for(yr in yr_split){
  fit<-yr$fit
  airline<-yr$airline
  year<-unique(unlist(yr$year))
  c<- yr[which.min(yr[,3]),2]
  min_car<-as.data.frame(c)
  min_list <- rbind(min_list, c(as.character(c)))
}

print(min_list)
c<- count(min_list)

#print(data.frame(dataList, col.names=FALSE))
write.table(as.character(c[which.max(c[,2]),1]), file = "R.txt" , row.names=FALSE, col.names=FALSE)
