gg.charts.PerformanceSummary <- function(rtn.obj, geometric=TRUE, main="",plot=TRUE){
  
  # load libraries
  suppressPackageStartupMessages(require(ggplot2))
  suppressPackageStartupMessages(require(scales))
  suppressPackageStartupMessages(require(reshape))
  suppressPackageStartupMessages(require(ggthemes))
  suppressPackageStartupMessages(require(PerformanceAnalytics))
  # create function to clean returns if having NAs in data
  clean.rtn.xts <- function(univ.rtn.xts.obj,na.replace=0){
    univ.rtn.xts.obj[is.na(univ.rtn.xts.obj)]<- na.replace
    univ.rtn.xts.obj
  }
  # Create cumulative return function
  cum.rtn <- function(clean.xts.obj, g=TRUE){
    x <- clean.xts.obj
    if(g==TRUE){y <- cumprod(x+1)-1} else {y <- cumsum(x)}
    y
  }
  # Create function to calculate drawdowns
  dd.xts <- function(clean.xts.obj, g=TRUE){
    x <- clean.xts.obj
    if(g==TRUE){y <- Drawdowns(x)} else {y <- Drawdowns(x,geometric=FALSE)}
    y
  }
  # create a function to create a dataframe to be usable in ggplot to replicate charts.PerformanceSummary
  cps.df <- function(xts.obj,geometric){
    x <- clean.rtn.xts(xts.obj)
    series.name <- colnames(xts.obj)[1]
    tmp <- cum.rtn(x,geometric)
    tmp$rtn <- x
    tmp$dd <- dd.xts(x,geometric)
    colnames(tmp) <- c("Cumulative_Return","Daily_Return","Drawdown")
    tmp.df <- as.data.frame(coredata(tmp))
    tmp.df$Date <- as.POSIXct(index(tmp))
    tmp.df.long <- melt(tmp.df,id.var="Date")
    tmp.df.long$asset <- rep(series.name,nrow(tmp.df.long))
    tmp.df.long
  }
  # A conditional statement altering the plot according to the number of assets
  if(ncol(rtn.obj)==1){
    # using the cps.df function
    df <- cps.df(rtn.obj,geometric)
    # adding in a title string if need be
    if(main==""){
      title.string <- paste0(df$asset[1]," Performance")
    } else {
      title.string <- main
    }
    # generating the ggplot output with all the added extras....
    gg.xts <- ggplot(df, aes_string(x="Date",y="value",group="variable"))+
      facet_grid(variable ~ ., scales="free", space="free")+
      geom_line(data=subset(df,variable=="Cumulative_Return"))+
      geom_bar(data=subset(df,variable=="Daily_Return"),stat="identity")+
      geom_line(data=subset(df,variable=="Drawdown"))+
      ylab("")+
      geom_abline(intercept=0,slope=0,alpha=0.3)+
      ggtitle(title.string)+
#       theme_economist() + #if you want to play try theme_wsj() or theme_few()
#       scale_colour_economist() +
      theme(axis.text.x = element_text(angle = 45, hjust = 1))+
      scale_x_datetime(breaks = date_breaks("6 months"), labels = date_format("%d/%m/%Y"))
    
  } else {
    # a few extra bits to deal with the added rtn columns
    no.of.assets <- ncol(rtn.obj)
    asset.names <- colnames(rtn.obj)
    df <- do.call(rbind,lapply(1:no.of.assets, function(x){cps.df(rtn.obj[,x],geometric)}))
    df$asset <- ordered(df$asset, levels=asset.names)
    if(main==""){
      title.string <- paste0(df$asset[1]," Performance")
    } else {
      title.string <- main
    }
    if(no.of.assets>5){legend.rows <- 5} else {legend.rows <- no.of.assets}
    gg.xts <- ggplot(df, aes_string(x="Date", y="value",group="asset"))+
      facet_grid(variable~.,scales="free",space="free")+
      geom_line(data=subset(df,variable=="Cumulative_Return"),aes(colour=factor(asset)))+
      geom_bar(data=subset(df,variable=="Daily_Return"),stat="identity",aes(fill=factor(asset),colour=factor(asset)),position="dodge")+
      geom_line(data=subset(df,variable=="Drawdown"),aes(colour=factor(asset)))+
      ylab("")+
      geom_abline(intercept=0,slope=0,alpha=0.3)+
      ggtitle(title.string)+
      theme(legend.title=element_blank(), legend.position=c(0,1), legend.justification=c(0,1),
            axis.text.x = element_text(angle = 45, hjust = 1))+
      guides(col=guide_legend(nrow=legend.rows))+
      scale_x_datetime(breaks = date_breaks("6 months"), labels = date_format("%d/%m/%Y"))
    
  }
  
  assign("gg.xts", gg.xts,envir=.GlobalEnv)
  if(plot==TRUE){
    plot(gg.xts)
  } else {}
  
}



# 
# # advanced charts.PerforanceSummary based on ggplot
# gg.charts.PerformanceSummary <- function(rtn.obj, geometric = TRUE, main = "", plot = TRUE)
# {
#   
#   # load libraries
#   suppressPackageStartupMessages(require(ggplot2))
#   suppressPackageStartupMessages(require(scales))
#   suppressPackageStartupMessages(require(reshape))
#   suppressPackageStartupMessages(require(PerformanceAnalytics))
#   
#   # create function to clean returns if having NAs in data
#   clean.rtn.xts <- function(univ.rtn.xts.obj,na.replace=0){
#     univ.rtn.xts.obj[is.na(univ.rtn.xts.obj)]<- na.replace
#     univ.rtn.xts.obj  
#   }
#   
#   # Create cumulative return function
#   cum.rtn <- function(clean.xts.obj, g = TRUE)
#   {
#     x <- clean.xts.obj
#     if(g == TRUE){y <- cumprod(x+1)-1} else {y <- cumsum(x)}
#     y
#   }
#   
#   # Create function to calculate drawdowns
#   dd.xts <- function(clean.xts.obj, g = TRUE)
#   {
#     x <- clean.xts.obj
#     if(g == TRUE){y <- Drawdowns(x)} else {y <- Drawdowns(x,geometric = FALSE)}
#     y
#   }
#   
#   # create a function to create a dataframe to be usable in ggplot to replicate charts.PerformanceSummary
#   cps.df <- function(xts.obj,geometric)
#   {
#     x <- clean.rtn.xts(xts.obj)
#     series.name <- colnames(xts.obj)[1]
#     tmp <- cum.rtn(x,geometric)
#     tmp$rtn <- x
#     tmp$dd <- dd.xts(x,geometric)
#     colnames(tmp) <- c("Index","Return","Drawdown") # names with space
#     tmp.df <- as.data.frame(coredata(tmp))
#     tmp.df$Date <- as.POSIXct(index(tmp))
#     tmp.df.long <- melt(tmp.df,id.var="Date")
#     tmp.df.long$asset <- rep(series.name,nrow(tmp.df.long))
#     tmp.df.long
#   }
#   
#   # A conditional statement altering the plot according to the number of assets
#   if(ncol(rtn.obj)==1)
#   {
#     # using the cps.df function
#     df <- cps.df(rtn.obj,geometric)
#     # adding in a title string if need be
#     if(main == ""){
#       title.string <- paste("Asset Performance")
#     } else {
#       title.string <- main
#     }
#     
#     gg.xts <- ggplot(df, aes_string( x = "Date", y = "value", group = "variable" )) +
#       facet_grid(variable ~ ., scales = "free_y", space = "fixed") +
#       geom_line(data = subset(df, variable == "Index")) +
#       geom_bar(data = subset(df, variable == "Return"), stat = "identity") +
#       geom_line(data = subset(df, variable == "Drawdown")) +
#       geom_hline(yintercept = 0, size = 0.5, colour = "black") +
#       ggtitle(title.string) +
#       theme(axis.text.x = element_text(angle = 0, hjust = 1)) +
#       scale_x_datetime(breaks = date_breaks("6 months"), labels = date_format("%m/%Y")) +
#       ylab("") +
#       xlab("")
#     
#   } 
#   else 
#   {
#     # a few extra bits to deal with the added rtn columns
#     no.of.assets <- ncol(rtn.obj)
#     asset.names <- colnames(rtn.obj)
#     df <- do.call(rbind,lapply(1:no.of.assets, function(x){cps.df(rtn.obj[,x],geometric)}))
#     df$asset <- ordered(df$asset, levels=asset.names)
#     if(main == ""){
#       title.string <- paste("Asset",asset.names[1],asset.names[2],asset.names[3],"Performance")
#     } else {
#       title.string <- main
#     }
#     
#     if(no.of.assets>5){legend.rows <- 5} else {legend.rows <- no.of.assets}
#     
#     gg.xts <- ggplot(df, aes_string(x = "Date", y = "value" )) +
#       
#       # panel layout
#       facet_grid(variable~., scales = "free_y", space = "fixed", shrink = TRUE, drop = TRUE, margin = 
#                    , labeller = label_value) + # label_value is default
#       
#       # display points for Index and Drawdown, but not for Return
#       geom_point(data = subset(df, variable == c("Index","Drawdown"))
#                  , aes(colour = factor(asset), shape = factor(asset)), size = 1.2, show_guide = TRUE) + 
#       
#       # manually select shape of geom_point
#       scale_shape_manual(values = c(1,2,3)) + 
#       
#       # line colours for the Index
#       geom_line(data = subset(df, variable == "Index"), aes(colour = factor(asset)), show_guide = FALSE) +
#       
#       # bar colours for the Return
#       geom_bar(data = subset(df,variable == "Return"), stat = "identity"
#                , aes(fill = factor(asset), colour = factor(asset)), position = "dodge", show_guide = FALSE) +
#       
#       # line colours for the Drawdown
#       geom_line(data = subset(df, variable == "Drawdown"), aes(colour = factor(asset)), show_guide = FALSE) +
#       
#       # horizontal line to indicate zero values
#       geom_hline(yintercept = 0, size = 0.5, colour = "black") +
#       
#       # horizontal ticks
#       scale_x_datetime(breaks = date_breaks("6 months"), labels = date_format("%m/%Y")) +
#       
#       # main y-axis title
#       ylab("") +
#       
#       # main x-axis title
#       xlab("") +
#       
#       # main chart title
#       ggtitle(title.string)
#     
#     # legend 
#     
#     gglegend <- guide_legend(override.aes = list(size = 3))
#     
#     gg.xts <- gg.xts + guides(colour = gglegend, size = "none") +
#       
#       # gglegend <- guide_legend(override.aes = list(size = 3), direction = "horizontal") # direction overwritten by legend.box?
#       # gg.xts <- gg.xts + guides(colour = gglegend, size = "none", shape = gglegend) + # Warning: "Duplicated override.aes is ignored"
#       
#       theme( legend.title = element_blank()
#              , legend.position = c(0,1)
#              , legend.justification = c(0,1)
#              , legend.background = element_rect()
#              , legend.box = "horizontal" # not working?
#              , axis.text.x = element_text(angle = 0, hjust = 1)
#       )
#     
#   }
#   assign("gg.xts", gg.xts,envir=.GlobalEnv)
#   if(plot == TRUE){
#     plot(gg.xts)
#   } else {}
#   
# }
