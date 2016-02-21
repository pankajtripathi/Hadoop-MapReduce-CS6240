install.packages('ggplot2')
library(ggplot2)
data_set <- read.csv("/Users/kavya/Documents/workspace/JavaToR/336.csv" ,header=TRUE);
p <- ggplot(data = data_set, aes(x = DAY_OF_MONTH , y = AVG_TICKET_PRICE,group=1)) 
p + geom_point() + geom_line() + stat_smooth(method = "lm", col = "red")