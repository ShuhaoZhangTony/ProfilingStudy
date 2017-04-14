getwd()
library(xlsx)
require(ggplot2)
require(reshape)

get_opt <- function(opt){
  y <- as.character(opt)
  switch (y, 
          "1" = "1core",
          "2" = "2cores",
          "3" = "4cores",
          "4" = "1socket",
          "5" = "2sockets",
          "6" = "4sockets",
          "7" = "batch",
          "8" = "hugePage"
  )
}

workload_path<- function(app){
  y <- as.character(app)
  switch (y, 
          "1" = paste("C:/Users/I309939/Documents/Profile-experiments/storm/1/output_stream-greping/%s_%s_%s", sep="")
  )
}

create_report<- function(app){
  y <- as.character(app)
  switch (y, 
          "1" = "sg"
  )
}


myseq<-c(1,2,4,8,10,12,14,16,18,20,32,34,36,38,40)
for(app in 1:1){
  # List attributes.
  argument<-NULL
  Duration<-NULL
  Results_generated<-NULL
  Average_Throughput<-NULL
  Average_Latency<-NULL
  
  # Point attributes.
  High_throughput<-0
  best_argument<-NULL
  
  s<-workload_path(app)
  if(app=="1"){
    for(opt in 1:6){
     for(bt in c(1)){
      for(ct1 in myseq){
        mypath <-sprintf(s,opt,bt,ct1)
          if(file.exists(mypath)) {
           setwd(mypath)
           print("Read raw information.")
           elapsed_time <- scan("elapsed_time.txt",nlines=1, quiet=TRUE)
           tuple_processed <- scan("tuple_processed.txt",nlines=1, quiet=TRUE)
           Throughput <- tuple_processed / elapsed_time * 10^6
           Latency <- 1/Throughput
           current_arg<-sprintf("app:%s, opt:%s, batch:%s, Ct:%s",app,opt,bt,ct1)
           
           #Update Point attributes.
           if(High_throughput < Throughput){
             High_throughput<-Throughput
             best_argument<-current_arg
           }
           
           #Update list attributes.
           argument<-c(argument,current_arg)
           Duration<-c(Duration,elapsed_time)
           Results_generated<-c(Results_generated,tuple_processed)
           Average_Throughput<-c(Average_Throughput,Throughput)
           Average_Latency<-c(Average_Latency,Latency)
        } 
      }
    }
  }
  
  setwd("C:/Users/I309939/Documents/Profile-experiments/storm")
  execution_frame <-data.frame(argument,Average_Throughput,Average_Latency)
  colnames(execution_frame) <- c("argument", "Average_Throughput","Average_Latency")
  sname<-sprintf("%s",create_report(app))
  print(sname)
  #ggplot(execution_frame,aes(Duration,Results_generated,Average_Throughput,Average_Latency))
  plot.ts(execution_frame)
 # write.xlsx2(execution_frame, file="storm.xlsx", sheetName=sname, append=TRUE)
  }  
}