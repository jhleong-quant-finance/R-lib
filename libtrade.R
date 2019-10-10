##############################################################################################
#
#    trading library
#
#    version : 1.01
#    modified on : 22 Nov 2013
#
#    version 1.01 - fix defect on gen_sig_trade_rule1 when upperbound is value
#
##############################################################################################
#
#    TODO:
#    
#    1) Once the matched stock found, observation found it can continue going down 
#       substantially. A monitoring procedure should be implement to only buy 
#       when there is 10% reverse from the open price to the buttom, and the current 
#       price is lower then the permited gap. eg. EXEP on 20131014
#
#    2) set max trade to 10 and min trade to 4. The 
#       the max allocation should 25% for each stock.
#
#    3) check the daily volumn of the stock before placing order. Avoid those very 
#       low volumne stock especially in SP600. eg. PCTI
#    4) put a horizontal line at the trade report chart to indicate  avg buying price
#
##############################################################################################

source(Sys.getenv('R_USER_DIR_CFG_FILE'));
source(cfg_file_libtrade);

#########################################################################
#
# Date 24 AUG 2012
# 
# my library 
#
##########################################################################
library('quantmod');
library("PerformanceAnalytics");

convert_number_scale_to_numeric <- function(strValue){
  
  if (is.na(strValue)) {
    return(NA);
  }
  
  # remove whitespace
  strTemp <- gsub("^\\s+|\\s+$", "", strValue);
  # remove $ and comma
  strTemp <- toupper(gsub(",","",(gsub("\\$", "", strTemp))));
  
  strScale <- substr(strTemp, nchar(strTemp), nchar(strTemp));

  if(strScale == 'B' || strScale == 'M' || strScale == 'K'){
    strNum <- substr(strTemp, 1, nchar(strTemp)-1);
    } else {
    strNum <- strTemp;
  }
  
  if(strScale == 'B'){
    num <- as.numeric(strNum) * 1000000000;
  } else if(strScale == 'M'){
    num <- as.numeric(strNum) * 1000000;
  } else if(strScale == 'K'){
    num <- as.numeric(strNum) * 1000;
  } else{
    num <- as.numeric(strNum);
  }
  
  return(num);
}

# search my downloaded price and set symbols to local lookup from the 
# csv file
create_local_symbol_lookup <- function
(
  SymLookUpFile = 'SymLookUpFile.dat',
  SymLookUpDir = '../data/finance/'
)
{
  library('quantmod')
  
  US_stock_dir = '../data/finance/US/price'
  stock_date_format = '%Y-%m-%d'
  #find the symbols of the list of historical price file in drive.
  downloaded_sym <- list.files(path=US_stock_dir)
  downloaded_sym <- toupper(gsub("\\.csv","",downloaded_sym))
  
  
  for(s in downloaded_sym) {
    temp = list()
    temp[[ s ]] = list(src='csv', format=stock_date_format, dir=US_stock_dir)
    setSymbolLookup(temp)
  }
  
  KL_stock_dir = '../data/finance/MY/price'
  stock_date_format = '%Y-%m-%d'
  #find the symbols of the list of historical price file in drive.
  downloaded_sym <- list.files(path=KL_stock_dir)
  downloaded_sym <- toupper(gsub("\\.csv","",downloaded_sym))
  
  for(s in downloaded_sym) {
    temp = list()
    temp[[ s ]] = list(src='csv', format=stock_date_format, dir=KL_stock_dir)
    setSymbolLookup(temp)
  }
  
  saveSymbolLookup(SymLookUpFile,dir=SymLookUpDir)
}


# init the local symbol lookup
set_local_symbol_lookup <- function()
{
  SymLookUpFile = 'SymLookUpFile.dat'
  SymLookUpDir = '../data/finance/'
  
  loadSymbolLookup(SymLookUpFile,dir=SymLookUpDir)
}



# used in calculating seasonality
subset_full_yr <- function(x)
{
  if(format(index(x[1]), '%m') != '01'){
    yr <- as.numeric(format(index(x[1]), '%Y')) + 1
    subset <- paste(yr, '/' ,sep='')
    x <- x[subset]
  }
  
  if(format(index(x[nrow(x)]), '%m') != '12'){
    yr <- as.numeric(format(index(x[nrow(x)]), '%Y')) -1
    subset <- paste('/',yr ,sep='')
    x <- x[subset]
  }
  
  return(x)
}

#################################################################################
# RSI weight
#################################################################################
create_weight <- function(long_cond, long_exit_cond, 
                          short_cond, short_exit_cond)
{  
  long_weight <- ifelse(long_cond, 1, (ifelse(long_exit_cond, NA, 0)))
  short_weight <- ifelse(short_cond, -1, (ifelse(short_exit_cond, NA, 0)))
  
  if(is.na(long_weight[1])) long_weight[1] = 0 #set to 0 if not a buy long level signal
  long_weight <- na.locf(long_weight)
  
  if(is.na(short_weight[1])) short_weight[1] = 0 #set to 0 if not a buy short level signal
  short_weight <- na.locf(short_weight)
  
  long_short_weight <- long_weight + short_weight
  long_short_weight <- lag(long_short_weight)
  long_short_weight[is.na(long_short_weight)] = 0
  
  return(long_short_weight)
}

# allow the short thresold lower then long level and long thresold higher then short level
create_weight_1 <- function(long_cond, long_exit_cond, 
                            short_cond, short_exit_cond)
{  
  long_weight <- ifelse(long_cond, 1, (ifelse(long_exit_cond, NA, 0)))
  short_weight <- ifelse(short_cond, -1, (ifelse(short_exit_cond, NA, 0)))
  
  if(is.na(long_weight[1])) long_weight[1] = 0 #set to 0 if not a buy long level signal
  long_weight <- na.locf(long_weight)
  
  if(is.na(short_weight[1])) short_weight[1] = 0 #set to 0 if not a buy short level signal
  short_weight <- na.locf(short_weight)
  
  # if both long and short have non-zero, its value is depend on the previous value.
  
  # if both previous value is 0, then 1, < longlevel and 0, > shortlevel
  for(i in 2:length(long_weight))
  {
    if(long_weight[i] == 1 && short_weight == -1)
    {
      # if both previous value is 0, then 1, < longlevel and 0, > shortlevel
      if(long_weight[i-1] == 0 && short_weight[i-1] == -1){
        long_weight[i] = 0
        short_weight[i] = -1
      }else if(long_weight[i-1] == 1 && short_weight[i-1] == 0){
        long_weight[i] = 1
        short_weight[i] = 0
      }
      else if(long_weight[i-1] == 0 && short_weight[i-1] == 0)
      {
        if(long_cond[i]){
          long_weight[i] = 1
          short_weight[i] = 0
        }else if(short_cond[i]){
          long_weight[i] = 0
          short_weight[i] = -1
        }
      }
    }
  }
  
  long_short_weight <- long_weight + short_weight
  long_short_weight <- lag(long_short_weight)
  long_short_weight[is.na(long_short_weight)] = 0
  
  return(long_short_weight)
}

create_weight_2 <- function(long_cond, long_exit_cond, 
                            short_cond, short_exit_cond)
{  
  long_weight <- ifelse(long_cond, 1, (ifelse(long_exit_cond, NA, 0)))
  short_weight <- ifelse(short_cond, -1, (ifelse(short_exit_cond, NA, 0)))
  
  if(is.na(long_weight[1])) long_weight[1] = 0 #set to 0 if not a buy long level signal
  long_weight <- na.locf(long_weight)
  
  if(is.na(short_weight[1])) short_weight[1] = 0 #set to 0 if not a buy short level signal
  short_weight <- na.locf(short_weight)
  
  # if both long and short have non-zero, its value is depend on the previous value.
  idx <- which(long_weight==1 & short_weight==-1)
  
  # if both previous value is 0, then 1, < longlevel and 0, > shortlevel
  for(i in idx)
  {
    # if both previous value is 0, then 1, < longlevel and 0, > shortlevel
    if(long_weight[i-1] == 0 && short_weight[i-1] == -1){
      long_weight[i] = 0
      short_weight[i] = -1
    }else if(long_weight[i-1] == 1 && short_weight[i-1] == 0){
      long_weight[i] = 1
      short_weight[i] = 0
    }
    else if(long_weight[i-1] == 0 && short_weight[i-1] == 0)
    {
      if(long_cond[i]){
        long_weight[i] = 1
        short_weight[i] = 0
      }else if(short_cond[i]){
        long_weight[i] = 0
        short_weight[i] = -1
      }
    }
  }
  
  long_short_weight <- long_weight + short_weight
  long_short_weight <- lag(long_short_weight)
  long_short_weight[is.na(long_short_weight)] = 0
  
  return(long_short_weight)
}

rsi_weight <- function(indlonglevel=10, indlongthresold=20, 
                       indshortlevel=90, indshortthresold=80, rsi)
{
  weight <- create_weight(rsi < indlonglevel, rsi < indlongthresold,
                          rsi > indshortlevel, rsi > indshortthresold)
  
  return(weight)
}

create_rsi_sig <- function(R, indlonglevel, indlongthresold, 
                           indshortlevel, indshortthresold, rsi_period)
{
  rsi <- RSI(R, rsi_period);
  rsi[is.na(rsi)] = 50
  
  sig <- create_weight_2(rsi < indlonglevel, rsi < indlongthresold, 
                         rsi > indshortlevel, rsi > indshortthresold);
  
  return(sig)
}

print_stat <- function(idvSta)
{
  cat(paste('Num of non-trading day : ', nrow(idvSta[idvSta==0]) ,'\n'))
  
  cat(paste('Num of ret > 0 : ', nrow(idvSta[idvSta>0]),'\n'))
  cat(paste('Num of ret < 0 : ', nrow(idvSta[idvSta<0]),'\n'))
  cat(paste('Winning Rate : ', round(nrow(idvSta[idvSta>0]) / ( nrow(idvSta[idvSta>0]) + nrow(idvSta[idvSta<0])) , digits=2),'\n'))
  
  cat(paste('SUM of ret > 0 : ', round(sum(idvSta[idvSta>0]), digits=2),'\n'))
  cat(paste('SUM of ret < 0 : ', round(sum(idvSta[idvSta<0]), digits=2),'\n'))
  cat(paste('Mean Return : ', round(mean(idvSta, na.rm=TRUE), digits=2),'\n'))
  cat('\n')
  
  idvSta_wo_zero <- idvSta[idvSta !=0]
  
  cat('Return Distribution:\n')
  print(table(cut(idvSta_wo_zero, c(-Inf,seq(-10, 10, by=1)/100,Inf))))
  cat('\n')
  
  cat('Return greater then 10% :\n')
  print(idvSta[idvSta>0.1])
  
  cat('\n')
  print(Return.annualized(idvSta))
  
  cat('\n Top 5 Max Drawdraw')
  
  print(table.Drawdowns(idvSta))
  #  par(no.readonly = T)
  #  chart.Histogram(idvSta, methods=c('add.centered', 'add.density', 'add.rug'))
  
  layout(rbind(c(1,2),c(3,4)))
  chart.Histogram(idvSta_wo_zero, main = "Plain", methods = NULL)
  chart.Histogram(idvSta_wo_zero, main = "Density", breaks=40, methods = c("add.density", "add.normal"))
  chart.Histogram(idvSta_wo_zero, main = "Skew and Kurt", methods = c("add.centered", "add.rug"))
  chart.Histogram(idvSta_wo_zero, main = "Risk Measures", methods = c("add.risk"))
}

#############################################################################
#
#
#    Combine all results from all strategies, prepare data for analysis.
#    
#
#############################################################################


generate_BuyOnGap_result <- function(nStockTradeVec, strategy_tag)
{
  
  
  nskip <- 0
  
  yr_ShRate <- data.frame();
  idx <- 0
  
  for( yr in 2003:2012){
    for(stdLookback in c(132, 66, 44, 22, 10, 5 )){
      for(stdMultiple in c(0,0.5,0.7,1)){
        for(nStocksBuy in c(1,2,4,5,8,10,20,40,60)){
          
          nStocksBuy_full <- paste("BuyOnGap.",nStocksBuy,sep="");
          
          cmd <- paste("(nStockTradeVec[[\'",nskip,"\']][[\'",stdLookback,
                       "\']][[\'",stdMultiple,"\']])[\'",yr,"\'][,\'",nStocksBuy_full,"\']",sep="");
          print(cmd);
          tradeVec <- eval(parse(text=cmd));
          
          #ShpRate <- SharpeRatio.annualized(tradeVec);
          result <- table.AnnualizedReturns(tradeVec)
          AnnRet <- result[1,]
          AnnSD <- result[2,]
          AnnSR <- result[3,]
          
          maxDD <- maxDrawdown(tradeVec);
          
          RowName <- paste(nskip,"_",stdLookback,"_",stdMultiple,"_",nStocksBuy_full,"_",
                           strategy_tag,"_",yr,sep="");
          dt <- data.frame(AnnRet,AnnSD,AnnSR, maxDD, nskip,stdLookback,
                           stdMultiple,nStocksBuy, yr, RowName, strategy_tag)
          colnames(dt) <- c( 'AnnRet', 'AnnSD','AnnSR', 'MaxDD',
                             'nskip','stdLookback','stdMultiple','nStocksBuy','Year','long_name','strategy');
          
          if(idx == 0){
            yr_ShRate <- dt
            idx <- idx + 1;
          }else{
            yr_ShRate <- rbind(yr_ShRate, dt);
          }
          
        }         
      }      
    }
    
    # colnames(yr_ShRate) <- as.character(yr);
    #SharpeRateList[[as.character(yr)]] <- yr_ShRate
  }
  return(yr_ShRate)
}


print_AnnualizedReturns <- function(nStockTradeVec)
{
  for(yr in 2003:2012)
  {
    cat('YEAR ', yr,'\n');
    print(round(table.AnnualizedReturns(
      as.xts((nStockTradeVec[['0']][['132']][['0.5']]))[as.character(yr)]), digits=2))
  }
}

print_maxDD <- function(nStockTradeVec)
{
  for(yr in 2003:2012)
  {
    cat('YEAR ', yr,'\n');
    print(round(maxDrawdown(
      as.xts((nStockTradeVec[['0']][['132']][['0.5']]))[as.character(yr)]), digits=2))
  }
}

print_daily_trade_Matrix <- function(conditionsMet, tradeMat, nStockTradeVec, month_yr)
{
  buy_on_gap.numTradePerDay((conditionsMet[['0']][['132']][['0.5']])[month_yr],
                            (tradeMat[['0']][['132']][['0.5']])[month_yr], 
                            (nStockTradeVec[['0']][['132']][['0.5']])[month_yr]);
}

#########################################################
#  BuyOnGap
#  number of trade per stock
#########################################################

buy_on_gap.numTradePerStock <- function(conditionsMet, TradeMat)
{
  count <- apply(conditionsMet, 2, function(x) sum(x, na.rm=TRUE))
  totalRet <- round(apply(TradeMat, 2, function(x) sum(x, na.rm=TRUE)), digits=3)
  numPosTrade <- apply(TradeMat, 2, function(x) length(x[x>0]))
  winRate <- round(numPosTrade/count , digits=2);
  meanRet <- round(totalRet/count, digits=3);
  meanRet <- ifelse(is.nan(meanRet), 0, meanRet)
  e2 <- data.frame(gsub('.LowOpenRet','',names(count)), count, totalRet, meanRet, winRate, stringsAsFactors=FALSE)
  colnames(e2) <- c('Symbol', 'Count', 'TotalRet', 'MeanRet', 'WinRate')
  rownames(e2) <- NULL
  
  numTrade <- e2[order(-e2$Count),]
  return(numTrade)
}

#########################################################
#  BuyOnGap
#  number of trade per day
#########################################################

buy_on_gap.numTradePerDay <- function(conditionsMet, tradeMat, nStockTradeVec)
{
  ff <- as.xts(apply(conditionsMet, 1, sum))
  colnames(ff) <- 'numOfTrade'
  f2 <- apply(conditionsMet, 1, function(x) paste(names(sort(which(x>0))), collapse=','))
  names(f2) <- 'Stock';
  
  numTradePerDay <- as.xts(data.frame(ff,f2, stringsAsFactors = FALSE));
  
  #   get_stock_return <- function(TradeMat, matched_sym, sym_date)
  #   {
  #     #sym_date <- index(matched_sym);
  #     
  #     sym_ret <- lapply(unlist(strsplit(matched_sym,',')), 
  #                       function(x) paste(round(TradeMat[sym_date][,x], digits=3), collapse=','));
  #     
  #     return(sym_ret);
  #   }
  
  #f3 <- lapply(index(numTradePerDay$f2),  function(x) get_stock_return(tradeMat, numTradePerDay$f2[x], x));
  f3 <- vector();
  
  for (i in 1:length(numTradePerDay[,1]))
  {
    sym_date <- index(numTradePerDay[i,]);
    sym_ret <- sapply(unlist(strsplit(numTradePerDay[i,]$f2,',')), 
                      function(x) round(tradeMat[as.character(sym_date)][,x] * 100, digits=1));
    sym_ret <- paste(sym_ret, collapse=',');
    f3 <- append(f3, sym_ret);
  } 
  
  f3 <- as.data.frame(f3)
  names(f3) <- 'Return';
  
  f4 <- round(nStockTradeVec[,c(1:3)] * 100, digits=1);
  f5 <- round(cumprod(nStockTradeVec[,c(1:3)] + 1), digits=2);
  
  numTradePerDay <- data.frame(numTradePerDay,f3, f4, f5);
  
  return(numTradePerDay)
}



sp400mid.components <- function()
{
  library('XML');
  #  sp400 <- xmlToList('http://www.bowgett.com/Markets/SP400_Components.xml')
  sp400 <- xmlToList('../data/finance/US/index/SP400_Components.xml', simplify =T)
  sp400 <- unlist(sp400[1,])
  sp400 <- gsub("/","-",sp400)
  names(sp400) <- NULL
  return(sp400)
}

sp500index.components <- function()
{
  library('XML');
  #  sp500 <- xmlToList('http://www.bowgett.com/Markets/SP500_Components.xml')
  sp500 <- xmlToList('../data/finance/US/index/SP500_Components.xml', simplify =T)
  sp500 <- unlist(sp500[1,])
  sp500 <- gsub("/","-",sp500)
  
  # replace VIA-B with VIAB
  sp500 <- gsub("VIA-B", "VIAB", sp500);
  
  names(sp500) <- NULL
  return(sp500)
}

sp600small.components <- function()
{
  library('XML');
  #  sp600 <- xmlToList('http://www.bowgett.com/Markets/SP600_Components.xml')
  sp600 <- xmlToList('../data/finance/US/index/SP600_Components.xml', simplify =T)
  sp600 <- unlist(sp600[1,])
  sp600 <- gsub("/","-",sp600)
  names(sp600) <- NULL
  return(sp600)
}

russell2000.components <- function()
{
  r2k <- read.csv("../data/finance/US/index/russell2000_stock_list.csv", 
                  blank.lines.skip=TRUE, header=FALSE, as.is=TRUE);
  r2k <- unique(r2k[,1]);
  return(r2k);
}

#############################################################################
#       stock historical data selection and preparation.
#############################################################################
#############################################################################
#       SP500 stock historical data
#############################################################################
make_sp500_stockData <- function()
{
  set_local_symbol_lookup();
  data <- new.env()
  
  symbols = sp500index.components();
  
  try(getSymbols(symbols, from = '1970-01-01', env = data, auto.assign = TRUE));
  
  print(paste(symbols, collapse=' '))
  print(paste('Number of stock : ', length(symbols), sep=''));
  
  save(list = ls(data), file='../data/finance/stockData/SP500_stockData.Rdata', envir=data)
}

load_sp500_stockData <- function(stockData_envir)
{
  load(file=paste0(cfg_stockData_path,'SP500_stockData.Rdata'), envir=stockData_envir)
}


#############################################################################
#       Prepare SP400 MidCap stock historical data
#############################################################################
make_sp400_stockData <- function()
{
  set_local_symbol_lookup();
  data <- new.env()
  
  symbols = sp400mid.components();
  
  try(getSymbols(symbols, from = '1970-01-01', env = data, auto.assign = TRUE));
  
  print(paste(symbols, collapse=' '))
  print(paste('Number of stock : ', length(symbols), sep=''));
  
  save(list = ls(data), file='../data/finance/stockData/SP400_stockData.Rdata', envir=data)
}

load_sp400_stockData <- function(stockData_envir)
{
  load(file=paste0(cfg_stockData_path,'SP400_stockData.Rdata'), envir=stockData_envir)
}


#############################################################################
#       Prepare SP600 SmallCap stock historical data
#############################################################################
make_sp600_stockData <- function()
{
  set_local_symbol_lookup();
  data <- new.env()
  
  symbols = sp600small.components();
  
  try(getSymbols(symbols, from = '1970-01-01', env = data, auto.assign = TRUE));
  
  print(paste(symbols, collapse=' '))
  print(paste('Number of stock : ', length(symbols), sep=''));
  
  save(list = ls(data), file='../data/finance/stockData/SP600_stockData.Rdata', envir=data)
}

load_sp600_stockData <- function(stockData_envir)
{
  load(file=paste0(cfg_stockData_path,'SP600_stockData.Rdata'), envir=stockData_envir)
}

#############################################################################
#       Prepare Russell 2000 stock historical data
#############################################################################
make_russell2000_stockData <- function()
{
  set_local_symbol_lookup();
  data <- new.env()
  
  sym = russell2000.components();
  downloaded_sym <- list.files(path="../data/finance/US/price")
  downloaded_sym <- toupper(gsub("\\.csv","",downloaded_sym))
  
  # remove Symbol which not downloaded or failed to download
  sym <- sym[sym %in% downloaded_sym]
  
  try(getSymbols(sym, from = '1970-01-01', env = data, auto.assign = TRUE));
  
  print(paste(sym, collapse=' '))
  print(paste('Number of stock : ', length(sym), sep=''));
  
  save(list = ls(data), file='../data/finance/stockData/Russell2000_stockData.Rdata', envir=data)
}

load_russell2000_stockData <- function(stockData_envir)
{
  load(file=paste0(cfg_stockData_path,'Russell2000_stockData.Rdata'), envir=stockData_envir)
}

#############################################################################
#       Prepare Russell 2000 stock historical data
#############################################################################
make_top100_etf_stockData <- function()
{
  set_local_symbol_lookup();
  data <- new.env()
  
  sym = top100_etf.components();
  downloaded_sym <- list.files(path="../data/finance/US/price")
  downloaded_sym <- toupper(gsub("\\.csv","",downloaded_sym))
  
  # remove Symbol which not downloaded or failed to download
  sym <- sym[sym %in% downloaded_sym]
  
  try(getSymbols(sym, from = '1970-01-01', env = data, auto.assign = TRUE));
  
  print(paste(sym, collapse=' '))
  print(paste('Number of stock : ', length(sym), sep=''));
  
  save(list = ls(data), file='../data/finance/stockData/top100_etf_stockData.Rdata', envir=data)
  
}

load_top100_etf_stockData <- function(stockData_envir)
{
  load(file=paste0(cfg_stockData_path,'top100_etf_stockData.Rdata'), envir=stockData_envir)
}

#############################################################################
#       convert the time series back to Date class, after bt.prep
#############################################################################
convert_index_to_date <- function(data){
  tmp <- sapply(ls(data), function(x) {  if(is.xts(data[[x]])) { indexClass(data[[x]]) <- 'Date';}});
}

# opposite of adjustOHLC, adjust the price from "from" till the last date in the series 
# back to the "unsplit" price as at "from" date. including dividend.
reverseAdj <-
  function (x, from, ratio = NULL, symbol.name) 
  {
    if(is.null(from))
    {
      from = as.Date(index(x[1,]));  
    }
    
    if (is.null(ratio)) {
        if (!has.Ad(x)) 
          stop("no Adjusted column in 'x'")
        ratio <- Ad(x)/Cl(x)
    }
  
    # get ratio on the last before from date
    inverse_ratio = as.numeric(1/last(ratio[index(ratio) < as.Date(from),1]));
    ratio <- ratio * inverse_ratio;
    
    Adjusted <- Cl(x) * ratio
    structure(cbind((ratio * (Op(x) - Cl(x)) + Adjusted), 
                    (ratio * (Hi(x) - Cl(x)) + Adjusted), 
                    (ratio * (Lo(x) - Cl(x)) + Adjusted), 
                    Adjusted), .Dimnames = list(NULL, colnames(x)[1:4]))
  }


##################################################################################
getNews_GoogleFinance <- function(symbol, number){
  
  # Warn about length
  if (number>300) {
    warning("May only get 300 stories from google")
  }
  
  # load libraries
  require(XML); require(plyr); require(stringr); require(lubridate);
  require(xts);
  
  # construct url to news feed rss and encode it correctly
  url.b1 = 'http://www.google.com/finance/company_news?q='
  url = paste(url.b1, symbol, '&output=rss', "&start=", 1,
              "&num=", number, sep = '')
  url = URLencode(url)
  
  # parse xml tree, get item nodes, extract data and return data frame
  doc = xmlTreeParse(url, useInternalNodes = TRUE)
  nodes = getNodeSet(doc, "//item")
  mydf = ldply(nodes, as.data.frame(xmlToList))
  
  # clean up names of data frame
  names(mydf) = str_replace_all(names(mydf), "value\\.", "")
  
  # convert pubDate to date-time object and convert time zone
  pubDate = strptime(mydf$pubDate,
                     format = '%a, %d %b %Y %H:%M:%S', tz = 'GMT')
  pubDate = with_tz(pubDate, tz = 'America/New_york')
  mydf$pubDate = NULL
  #Parse the description field
  mydf$description <- as.character(mydf$description)
  parseDescription <- function(x) {
    #out <- html2text(x)$text
    doc2 = htmlParse(x, asText=TRUE)
    plain.text <- xpathSApply(doc2, "//text()[not(ancestor::script)][not(ancestor::style)][not(ancestor::noscript)][not(ancestor::form)]", xmlValue)
    out <- paste(plain.text, collapse = " ");
    
    out <- strsplit(out,'\n|--')[[1]]
    #Find Lead
    TextLength <- sapply(out,nchar)
    Lead <- out[TextLength==max(TextLength)]
    #Find Site
    Site <- out[3]
    
    #Return cleaned fields
    out <- c(Site,Lead)
    names(out) <- c('Site','Lead')
    out
  }
  description <- lapply(mydf$description,parseDescription)
  description <- do.call(rbind,description)
  mydf <- cbind(mydf,description)
  
  #Format as XTS object
  mydf = xts(mydf,order.by=pubDate)
  # drop Extra attributes that we don't use yet
  mydf$guid.text = mydf$guid..attrs = mydf$description = mydf$link = NULL
  return(mydf)
  
}

getSymbols_par <- function(sym_list, stockEnv)
{
  library(quantmod);
  require(doMC);
  
  registerDoMC(10);
  
  from_date <- as.Date(trunc(Sys.time(),"days")) - 365;  # 1 year
  
  sym_data <- foreach(c_sym=sym_list) %dopar%
{
  #try(Sys.sleep(abs(rnorm(1, mean = 0.5, sd =0.2))))
  try(getSymbols(c_sym, from=from_date, src="yahoo", auto.assign=F))
}
  
  for(i in 1:length(sym_data))
  {
    if(is.xts(sym_data[[i]]))
    {
      sym_name <- gsub(".Open","", names(sym_data[[i]])[1]);
      eval(parse(text=paste("stockEnv$\'",sym_name,"\' <- sym_data[[i]]",sep="")));
    }
  }
  registerDoMC();
}


#  Most Popular ETFs: Top 100 ETFs By Trading Volume
# http://etfdb.com/compare/volume/

top100_etf.components <- function()
{
  library(sit)
  #url = 'http://etfdb.com/compare/volume/'
  # download the page source from url above
  url='../data/finance/US/index/top100_etf_volume.htm'
  txt = join(readLines(url))
  temp = extract.table.from.webpage(txt, 'Symbol', hasHeader = T)
  tickers = temp[, 'Symbol']
  return(tickers);
}


#############################################################################
#       Function to calculate MTP (minimum total profit) for
#       upperbound value, using AR(1) process to model spread
#############################################################################

pairs_perf_stat <- function(price.pair, signal)
{
  p_daily_ret <- pairs_ret_rebalance(price.pair, signal)
  
  p_tmp <- na.omit(p_daily_ret);
  p_cum_ret <- (100* cumprod(1 + p_tmp));
  
  if(NROW(p_daily_ret) > 1){
    p_ret <- as.numeric(Return.annualized(p_daily_ret));
    p_sharpe <- as.numeric(SharpeRatio.annualized(p_daily_ret)); 
  } else { 
    p_ret <- NA; p_sharpe <- NA;  }
  
  res <- list(daily_ret = p_daily_ret, cum_ret=p_cum_ret, ret=p_ret,
              sharpe = p_sharpe);
  
  return(res);
}

# pairs_ret <- function(price.pair, signal, beta)
# {
#   p_daily_ret <- lag(signal) * 
#     ( (1 / (1 + abs(beta)) * ROC(price.pair[,1])) - ( (beta / ( 1 + abs(beta))) * ROC(price.pair[,2]) ) );
#   
#   res <- as.numeric(Return.annualized(p_daily_ret));
#   return(res);
# }



pairs_ret_rebalance <- function(price.pair, signal)
{
  sig <- na.omit(lag(signal));
  stock1_ret <- ROC(price.pair[,1], type='discrete');
  stock2_ret <- ROC(price.pair[,2], type='discrete');
  stock_ret <- merge(stock1_ret, -stock2_ret) * drop(sig);
  
  weight <- sig;
  weight[]<- NA;
  
  for(i in 2:NROW(sig)){
    if( (sig[i]!=0) && 
          (as.numeric(sig[i-1]) != as.numeric(sig[i]))){
      #rebalance at prev day
      weight[i-1,] <- 0.5;
    }
  }
  weight <- weight[(!is.na(weight[,1])),]
  #   # if the last row of weight is at the last 2 position of the stock_ret, 
  #   # remove it. it will cause problem for Return.rebalancing.
  #   if(index(last(weight)) >= index(last(stock_ret, n=2)[1,]))
  #   {
  #     weight <- weight[1:(NROW(weight)-1),];
  #   }
  
  if(NROW(weight)!=0){
    weight <- merge(weight, weight)
    colnames(weight) <- colnames(stock_ret);
    
    ret <- Return.rebalancing_mod(stock_ret, weight, wealth.index=F, contribution = F);
    
    return(ret$portfolio.returns);
  } else {
    return(stock_ret[,1]/2 + stock_ret[,2]/2);
  }
}

# Return.portfolio and Return.rebalancing can't handle single row of return
Return.rebalancing_mod <- function (R, weights, ...) 
{
  if (is.vector(weights)) {
    stop("Use Return.portfolio for single weighting vector.  This function is for building portfolios over rebalancing periods.")
  }
  weights = checkData(weights, method = "xts")
  R = checkData(R, method = "xts")
  if (as.Date(first(index(R))) > (as.Date(index(weights[1, 
                                                        ])) + 1)) {
    warning(paste("data series starts on", as.Date(first(index(R))), 
                  ", which is after the first rebalancing period", 
                  as.Date(first(index(weights))) + 1))
  }
  if (as.Date(last(index(R))) < (as.Date(index(weights[1, ])) + 
                                   1)) {
    stop(paste("last date in series", as.Date(last(index(R))), 
               "occurs before beginning of first rebalancing period", 
               as.Date(first(index(weights))) + 1))
  }
  for (row in 1:nrow(weights)) {
    from = as.Date(index(weights[row, ])) + 1
    if (row == nrow(weights)) {
      to = as.Date(index(last(R)))
    }
    else {
      to = as.Date(index(weights[(row + 1), ]))
    }
    if (row == 1) {
      startingwealth = 1
    }
    tmpR <- R[paste(from, to, sep = "/"), ]
    if (nrow(tmpR) >= 1) {
      resultreturns = Return.portfolio_mod(tmpR, weights = weights[row, 
                                                               ], ... = ...)
      if (row == 1) {
        result = resultreturns
      }
      else {
        result = rbind(result, resultreturns)
      }
    }
    startingwealth = last(cumprod(1 + result) * startingwealth)
  }
  result <- reclass(result, R)
  result
}


Return.portfolio_mod <- function (R, weights = NULL, wealth.index = FALSE, contribution = FALSE, 
          geometric = TRUE, ...) 
{
  R = checkData(R, method = "xts")
  if (!nrow(R) >= 1) {
    warning("no data passed for R(eturns)")
    return(NULL)
  }
  if (hasArg(method) & !is.null(list(...)$method)) 
    method = list(...)$method[1]
  else if (!isTRUE(geometric)) 
    method = "simple"
  else method = FALSE
  if (is.null(weights)) {
    weights = t(rep(1/ncol(R), ncol(R)))
    warning("weighting vector is null, calulating an equal weighted portfolio")
    colnames(weights) <- colnames(R)
  }
  else {
    weights = checkData(weights, method = "matrix")
  }
  if (nrow(weights) > 1) {
    if ((nrow(weights) == ncol(R) | nrow(weights) == ncol(R[, 
                                                            names(weights)])) & (ncol(weights) == 1)) {
      weights = t(weights)
    }
    else {
      stop("Use Return.rebalancing for multiple weighting periods.  This function is for portfolios with a single set of weights.")
    }
  }
  if (is.null(colnames(weights))) {
    colnames(weights) <- colnames(R)
  }
  if (method == "simple") {
    weightedreturns = R[, colnames(weights)] * as.vector(weights)
    returns = R[, colnames(weights)] %*% as.vector(weights)
    if (wealth.index) {
      wealthindex = as.matrix(cumsum(returns), ncol = 1)
    }
    else {
      result = returns
    }
  }
  else {
    wealthindex.assets = cumprod(1 + R[, colnames(weights)])
    wealthindex.weighted = matrix(nrow = nrow(R), ncol = ncol(R[, 
                                                                colnames(weights)]))
    colnames(wealthindex.weighted) = colnames(wealthindex.assets)
    rownames(wealthindex.weighted) = as.character(index(wealthindex.assets))
    for (col in colnames(weights)) {
      wealthindex.weighted[, col] = weights[, col] * wealthindex.assets[, 
                                                                        col]
    }
    wealthindex = as.xts(apply(wealthindex.weighted, 1, sum))
    result = wealthindex
    if(length(result) > 1){
    result[2:length(result)] = result[2:length(result)]/lag(result)[2:length(result)] - 
      1 }
    result[1] = result[1] - 1
    w = matrix(rep(NA), ncol(wealthindex.assets) * nrow(wealthindex.assets), 
               ncol = ncol(wealthindex.assets), nrow = nrow(wealthindex.assets))
    w[1, ] = weights
    if(length(wealthindex) > 1){
    w[2:length(wealthindex), ] = (wealthindex.weighted/rep(wealthindex, 
                                                           ncol(wealthindex.weighted)))[1:(length(wealthindex) - 
                                                                                             1), ] }
    weightedreturns = R[, colnames(weights)] * w
  }
  if (!wealth.index) {
    colnames(result) = "portfolio.returns"
  }
  else {
    wealthindex = reclass(wealthindex, match.to = R)
    result = wealthindex
    colnames(result) = "portfolio.wealthindex"
  }
  if (contribution == TRUE) {
    result = cbind(weightedreturns, coredata(result))
  }
  rownames(result) <- NULL
  result <- reclass(result, R)
  result
}

# The trade is open when the spread hit upperbound or lowerbound till 
# spread crossing the zero. The spread is centered around mean.
gen_sig_trade_rule1 <- function(x , upperbound, lowerbound)
{
  # centered the spread around mean
  #x <- x[] - mean(x);
  
  # if trade lvl has no value with NA, stop the signal
  # U trade. test the upperbound is a value or xts array.
  if(is.null(dim(upperbound))){
    sig_u <- ifelse(x >= upperbound, -1, NA)
  } else {
    sig_u <- ifelse(is.na(upperbound), 0, ifelse(x >= upperbound, -1, NA));
  }
  sig_u <- ifelse(x <= 0, 0, sig_u);
  sig_u <- na.locf(sig_u, na.rm=F);
  
  # L trade
  if(is.null(dim(lowerbound))){
    sig_l <- ifelse(x <= lowerbound, 1, NA)
  } else {
    sig_l <- ifelse(is.na(lowerbound), 0, ifelse(x <= lowerbound, 1, NA));
  }
  sig_l <- ifelse(x > 0, 0, sig_l);
  sig_l <- na.locf(sig_l, na.rm=F);
  
  sig <- sig_u + sig_l
  return(sig);
}

# the crossing have to be zero
num_crossing_zero <- function(tsobject)
{
  count <- 0
  sign_series <- as.vector(na.omit(sign(tsobject)));
  
  for(i in 2:length(sign_series)){
    if(sign_series[i] != sign_series[i-1] )
      count <- count + 1;
  }
  return(count);
}

# number of trade
get_num_trade_rule1 <- function(signal)
{
  count <- 0
  sig_series <- as.vector(signal);
  
  for(i in 2:length(sig_series)){
    if((sig_series[i] != sig_series[i-1]) && (sig_series[i-1] != 0)){
      count <- count + 1;
    }
  }
  return(count);
}

# calculate sample trading stat
sample_trade_stat_rule1<- function(signal)
{  
  if(!all(is.na(signal))){
    sig <- na.omit(signal);
    
    num_trade <- get_num_trade_rule1(sig);
    total_trading_day <- sum(ifelse((sig == 1) | (sig == -1), 1, 0));
    TradingDuration <- total_trading_day / num_trade; 
    total_interval_day <- sum(ifelse(sig == 0, 1, 0));
    TradeInterval <- total_interval_day / num_trade;
  } else {
    num_trade <- NA;
    total_trading_day <- NA;
    TradingDuration <- NA;
    total_interval_day <- NA;
    TradeInterval <- NA;
  }
  
  return(c(TradingDuration, TradeInterval, num_trade));
}

# AR1 sim only do upperbound trading
ar1_trade_stat_sim<- function(x, upperbound)
{  
  
  sig_u <- ifelse(x >= upperbound, 1, NA);
  sig_u <- ifelse(x <= 0, 0, sig_u);
  
  if(!all(is.na(sig_u))){
    sig <- na.locf(sig_u);
    
    num_trade <- ceiling(num_crossing_zero(sig)/2);
    
    if(num_trade > 0){
      total_trading_day <- sum(ifelse(sig == 1, 1, 0));
      TradingDuration <- total_trading_day / num_trade; 
      total_interval_day <- sum(ifelse(sig == 0, 1, 0));
      TradeInterval <- total_interval_day / num_trade;
      
      return(c(TradingDuration, TradeInterval, num_trade));
    }
  } 
  
  num_trade <- NA;
  total_trading_day <- NA;
  TradingDuration <- NA;
  total_interval_day <- NA;
  TradeInterval <- NA;
  
  
  return(c(TradingDuration, TradeInterval, num_trade));
}

ar1_MTP_sim <- function(spread, upperbound, total_interval, spread_AR1_stat,
                        num_sim=getOption('ar1_MTP_sim.num_sim')){
  # number of simulation, default 50
  if(is.null(num_sim))
    num_sim <- 50
  
  stat <- c(0,0,0);
  
  set.seed(168)      # so you can reproduce these results
  stat <- NULL;
  for(i in 1:num_sim)
  {
    AR1_sim <- arima.sim(list(order=c(1,0,0), ar=spread_AR1_stat$coef[1]), 
                         sd=sqrt(spread_AR1_stat$sigma2) ,n=total_interval);
    x <- ar1_trade_stat_sim(AR1_sim, upperbound);  
    stat <- rbind(stat, data.frame(TD=x[1],I=x[2],NumTrade=x[3]));
  }
  
  # remove sim that without trade
  stat <- stat[!is.na(stat$NumTrade),];
  
  if(nrow(stat) > 0){
    stat_avg <- colMeans(stat, na.rm=T)
    #stat_avg <- stat / n;
    TD_u <- stat_avg[1];
    I_u <- stat_avg[2];
    
    mtp <- ((total_interval / ( TD_u + I_u )) -1) * upperbound;
    
  } else {
    mtp <- 0;
    TD_u <- 0;
    I_u <- 0;
  }
  
  res <- list(MTP = mtp, TD = TD_u, I = I_u);
  return(res);
}

get_top_ranked_pair <- function(m, n)
{
  num_crossing_rank <- rank(m$num_crossing, na.last=T);
  coe_rank <- rank(-m$ar1_coe, na.last=T);
  TD_rank <- rank(-m$sim_TD, na.last=T)
  I_rank <- rank(-m$sim_I, na.last=T)
  sample_ret_rank <- rank(m$sample_ret / m$sample_len, na.last=T);
  
  total_rank <- coe_rank + TD_rank + I_rank + (0.5 * sample_ret_rank) + (0.3 * num_crossing_rank)
  m_top_rank <- order(-total_rank)[1:n];
  
  return(m_top_rank);
}


#############################################################################
#       Function for searching for pairs                                    #       
#############################################################################
#library(PairTrading)
# assume the mean is 0
cal_tradability <- function(price1, price2, hedge_ratio, spread)
{
  examine_interval <- 252;
  
  price.pair <- merge(price1, price2);
  # test for trade leve of sd 0.5, 0.75, 1, 1.5 and 2
  spread_sd <- apply(spread, 2, sd);
  
  #   sig1 <- Simple(spread, spread_sd * 0.5);
  #   sig2 <- Simple(spread, spread_sd * 0.75);
  #   sig3 <- Simple(spread, spread_sd * 1);
  #   sig4 <- Simple(spread, spread_sd * 1.5);
  #   sig5 <- Simple(spread, spread_sd * 2);
  
  #   ret1 <- NA
  #   ret2 <- NA
  #   ret3 <- NA
  #   ret4 <- NA
  #   ret5 <- NA
  
  #   if(any(!is.na(sig1)))
  #     ret1 <- pairs_ret(price.pair, sig1, hedge_ratio);
  #   
  #   if(any(!is.na(sig2)))
  #     ret2 <- pairs_ret(price.pair, sig2, hedge_ratio );
  #   
  #   if(any(!is.na(sig3)))
  #     ret3 <- pairs_ret(price.pair, sig3, hedge_ratio );
  #   
  #   if(any(!is.na(sig4)))
  #     ret4 <- pairs_ret(price.pair, sig4, hedge_ratio );
  #   
  #   if(any(!is.na(sig5)))
  #     ret5 <- pairs_ret(price.pair, sig5, hedge_ratio );
  
  
  #try(suppressWarnings(spread_coe <- arima(spread, order=c(1,0,0))$coef[1]), silent=T);
  # calculate AR1 coe
  
  spread_AR1_stat <- NULL;
  try(suppressWarnings(spread_AR1_stat <- arima(spread, order=c(1,0,0), include.mean=T)));
  spread_coe <- NA;
  if(!is.null(spread_AR1_stat))
    spread_coe <- spread_AR1_stat$coef[1];
  
  wrap <- function(upperbound) {
    return(ar1_MTP_sim(spread,upperbound, examine_interval, spread_AR1_stat)$MTP);
  }
  
  opt <- NULL;
  # filter out the pairs with coe > 0.98. Note, a very close to 1 coe caused the
  # arima.sim execute extreme long time and huge memory.
  
  # get default value
  v_tol <- getOption('cal_tradability.optimize.tol');
  if(is.null(v_tol))
    v_tol = 0.01;
  
  v_upper_interval_sd <- getOption('cal_tradability.optimize.upper_interval_sd');
  if(is.null(v_upper_interval_sd))
    v_upper_interval_sd = 3;
  
  v_lower_interval_sd <- getOption('cal_tradability.optimize.lower_interval_sd');
  if(is.null(v_lower_interval_sd))
    v_lower_interval_sd = 0.5;
  
  if(!is.na(spread_coe) && spread_coe <= 0.98) {
    try(suppressWarnings(opt <- optimize(wrap, 
                                         interval=c(v_lower_interval_sd * spread_sd, 
                                                    v_upper_interval_sd * spread_sd), 
                                         tol=v_tol, 
                                         maximum=TRUE)));
  }
  
  
  
  
  if(is.null(opt)){
    opt_trade_lvl <- NA;
    opt_mtp <- NA;
    sample_TD <- NA;
    sample_I <- NA;
    sample_num_trade <- NA;
    sample_ret <- NA;
    sample_sharpe <- NA;
    sim_TD <- NA;
    sim_I <- NA;
    
  }else{
    opt_trade_lvl <- opt$maximum;
    opt_mtp <- opt$objective;
    
    #spread_mu_centered <- spread[] - mean(spread);
    sample_sig_rule1 <- gen_sig_trade_rule1(spread, opt_trade_lvl, -opt_trade_lvl);
    #print(str(sample_sig_rule1));
    #sample_stat <- c(0,0,0) #sample_trade_stat_rule1(sample_sig_rule1);
    sample_stat <- sample_trade_stat_rule1(sample_sig_rule1);
    sample_TD <- sample_stat[1];
    sample_I <- sample_stat[2];
    sample_num_trade <- sample_stat[3];
    
    ret_stat <- pairs_perf_stat(price.pair, sample_sig_rule1 );
    sample_ret <- ret_stat$ret;
    sample_sharpe <- ret_stat$sharpe;
    sim_stat <- ar1_MTP_sim(spread, opt_trade_lvl, examine_interval, spread_AR1_stat);
    sim_TD <- sim_stat$TD;
    sim_I <- sim_stat$I;
  }
  
  return(data.frame(ar1_coe=spread_coe,
                    opt_trade_lvl=opt_trade_lvl, opt_mtp=opt_mtp,
                    sample_TD=sample_TD, sample_I=sample_I,
                    sample_num_trade=sample_num_trade,
                    sample_ret=sample_ret, sample_sharpe=sample_sharpe,
                    sim_TD=sim_TD, sim_I=sim_I
  )
  );
}


matching_algo <- function(i, j, stock.prices1, stock.prices2)
{
  stock1 <- stock.prices1[, i];
  stock2 <- stock.prices2[, j];
  stock1_sym <- colnames(stock1);
  stock2_sym <- colnames(stock2);
  
  combined <- merge(stock1, stock2);
  colnames(combined) <- c('stock1', 'stock2')
  
  if ( nrow(na.omit(combined)) < 10 ) { return(NULL) }
  
  test.stat <- lm(stock1 ~ stock2 + 0, data = combined);
  beta <- coef(test.stat[1])
  spread <- combined$stock1 - beta*combined$stock2
  spread <- spread[!is.na(spread)]
  suppressWarnings(ht <- adf.test(as.zoo(spread), alternative="stationary", k=0));
  
  #   if(ht$p.value < 0.05)
  #   {
  pairs_ret_stat <- cal_tradability(stock1, stock2, beta, spread);
  return(cbind(data.frame(price1=i,
                          price2=j,
                          stock1_sym=stock1_sym, stock2_sym=stock2_sym,
                          num_crossing=num_crossing_zero(spread),
                          hedge_ratio=beta,
                          sample_len=nrow(spread),
                          p.value=ht$p.value), pairs_ret_stat));
  #   }
  #   else {
  #     return(NULL);
  #   }
}

matching_algo_prescreen<- function(i, j, stock.prices1, stock.prices2, p_value_lmt=0.05)
{
  stock1 <- stock.prices1[, i];
  stock2 <- stock.prices2[, j];
  combined <- merge(stock1, stock2);
  colnames(combined) <- c('stock1', 'stock2')
  
  if ( nrow(na.omit(combined)) < 10 ) { return(NULL) }
  
  test.stat <- lm(stock1 ~ stock2 + 0, data = combined);
  beta <- coef(test.stat[1])
  spread <- combined$stock1 - beta*combined$stock2
  spread <- spread[!is.na(spread)]
  suppressWarnings(ht <- adf.test(as.zoo(spread), alternative="stationary", k=0));
  
  if(ht$p.value < p_value_lmt)
  {    
    # calculate AR1 coe
    spread_coe <- NA;
    try(suppressWarnings(spread_coe <- arima(spread, order=c(1,0,0))$coef[1]), silent=T);
    
    if(!is.na(spread_coe)){
      return(data.frame(price1=i,
                        price2=j,
                        num_crossing=num_crossing_zero(spread),
                        p.value=ht$p.value,
                        ar1_coe=spread_coe)
      );
    } else {
      return(NULL);
    }
  }
  else {
    return(NULL);
  }
}


matching_algo.rolling <- function(stock.index1, stock.index2, 
                                  stock.prices1, stock.prices2, rolling_width=252)
{  
  stock1 <- stock.prices1[, stock.index1];
  stock2 <- stock.prices2[, stock.index2];  
  stock1_sym <- colnames(stock1);
  stock2_sym <- colnames(stock2);
  
  combined <- merge(stock1, stock2);
  
  
  if ( nrow(na.omit(combined)) < rolling_width ) { return(NULL) }

  res_rolling <- apply.rolling.mod2(combined[,1], width=rolling_width, trim=F, by=22, 
                                   FUN=function(x) matching_algo.rolling_FUN(stock.index1, stock.index2, 
                                                                             x, combined[,2], 
                                                                             stock1_sym, stock2_sym));
  
  spread <- NULL;
  opt_trade_lvl <- NULL;
  
  # skip the first row of res_rolling
  for(i in 2:(NROW(res_rolling)-1)) {
    row = which((index(combined) >= index(res_rolling[i])) 
              & (index(combined) < index(res_rolling[i+1])));
    
    # gen spread
    stock_1 <- combined[row,1];
    stock_2 <- combined[row,2];
    
    # the hedgeRatio, trade lvl from previous rolling window
    hedge_ratio_prev <- as.numeric(res_rolling[i-1]$hedge_ratio);
    opt_trade_lvl_prev <- as.numeric(res_rolling[i-1]$opt_trade_lvl);
    
    spread_rolling <- stock_1 - hedge_ratio_prev * stock_2
    spread_rolling <- spread_rolling[!is.na(spread_rolling)];
    
    # build the same time series as spread_rolling
    opt_trade_lvl_rolling <- spread_rolling;
    opt_trade_lvl_rolling[] <- opt_trade_lvl_prev;
     
    spread <- rbind(spread, spread_rolling);
    opt_trade_lvl <- rbind(opt_trade_lvl, opt_trade_lvl_rolling);
  }

  sample_sig_rule1<- gen_sig_trade_rule1(spread, opt_trade_lvl, -opt_trade_lvl);
  
  # calculate ret stat
  sample_stat <- sample_trade_stat_rule1(sample_sig_rule1);
  sample_TD <- sample_stat[1];
  sample_I <- sample_stat[2];
  sample_num_trade <- sample_stat[3];
  
  ret_stat <- pairs_perf_stat(combined, sample_sig_rule1 );
  sample_ret <- ret_stat$ret;
  sample_sharpe <- ret_stat$sharpe;
  
  
  return(list(price1=stock.index1, price2=stock.index2,
              stock1_sym=stock1_sym, stock2_sym=stock2_sym,
              num_crossing=num_crossing_zero(spread),
              hedge_ratio=mean(res_rolling$hedge_ratio, na.rm=T),
              sample_len=nrow(spread),
              p.value=mean(res_rolling$p.value, na.rm=T),
              sample_TD=sample_TD, sample_I=sample_I,
              sample_num_trade=sample_num_trade,
              sample_ret=sample_ret, sample_sharpe=sample_sharpe, 
              rolling_stat=res_rolling,
              spread=spread,
              sig=sample_sig_rule1,
              daily_ret=ret_stat$daily_ret,
              opt_trade_lvl=opt_trade_lvl
              ));
}

library(plyr);
matched_pairs_extract_df <- function(m_pairs){  
  mm <- lapply(m_pairs, function(x) {x$rolling_stat <- NULL;
                                     x$spread <- NULL;
                                     x$sig <- NULL;
                                     x$daily_ret <- NULL;
                                     x$opt_trade_lvl <- NULL;
                                     x;});
  
  matched_pairs_df <- ldply(mm, data.frame);
  return(matched_pairs_df);
}

apply.rolling.mod <- 
  function (R, width, trim = TRUE,  by_period ='months', FUN = "mean", 
            ...) 
  {
    R = checkData(R)
    R = na.omit(R)
    rows = NROW(R)
    result = xts(, order.by = time(R))
    dates = time(R)
    calcs = data.frame();
    
    #  steps = seq(from = rows, to = gap, by = -by)
    #  steps = steps[order(steps)]
    steps = endpoints(R, by_period);
    
    #exclude the first end points, which is 0
    steps <- steps[2:(length(steps))];
    steps <- steps[(steps + width - 1 <= rows)];
    
    for (row in steps) {
      if (width == 0) 
        r = R[1:row, ]
      else r = R[(row+1):(row + width - 1), ]
      calc = apply(r, MARGIN = 2, FUN = FUN, ... = ...)
      calc = as.data.frame(calc[[1]]);
      calcs = rbind(calcs, calc)
    }
    calcs = xts(calcs, order.by = dates[steps])
    #  result = merge(result, calcs)
    #  result = reclass(result, R)
    return(calcs)
  }

apply.rolling.mod2 <-
function (R, width, trim = TRUE, gap = 1, by = 1, FUN = "mean", 
          ...) 
{
  R = checkData(R)
  R = na.omit(R)
  rows = NROW(R)
  result = xts(, order.by = time(R))
  dates = time(R)
  calcs = data.frame()
  if (width == 0) {
    gap = gap
  }
  else gap = width
  steps = seq(from = rows, to = gap, by = -by)
  steps = steps[order(steps)]
  for (row in steps) {
    if (width == 0) 
      r = R[1:row, ]
    else r = R[(row - width + 1):row, ]
    calc = apply(r, MARGIN = 2, FUN = FUN, ... = ...)
    calc = as.data.frame(calc[[1]]);
    calcs = rbind(calcs, calc)
  }
  #calcs = xts(calcs[-1], order.by = dates[steps])
  #calcs = xts(calcs, order.by = as.Date(dates[steps]));

  #print(col_names);
  calcs = xts(calcs, order.by = dates[steps]);
#  result = merge(result, calcs)
 # result = reclass(result, R)
  return(calcs)
}

matching_algo.rolling_FUN <- function(stock.index1, stock.index2, stock1, stock2, stock1_sym, stock2_sym) {
  
  stock1 <- as.xts(stock1);
  stock2 <- stock2[index(stock1),];
  
  if ( nrow(na.omit(stock1)) < 10 ) { return(NULL) }
  
  test.stat <- lm(stock1 ~ stock2 + 0);
  beta <- coef(test.stat[1])
  spread <- stock1 - beta * stock2
  spread <- spread[!is.na(spread)]
  suppressWarnings(ht <- adf.test(as.zoo(spread), alternative="stationary", k=0));
  
  #   if(ht$p.value < 0.05)
  #   {
  pairs_ret_stat <- cal_tradability(stock1, stock2, beta, spread);
  return(cbind(data.frame(price1=stock.index1,
                          price2=stock.index2,                            
                          num_crossing=num_crossing_zero(spread),
                          hedge_ratio=beta,
                          sample_len=nrow(spread),
                          p.value=ht$p.value), pairs_ret_stat));
  #   }
  #   else {
  #     return(NULL);
  #   }
}
