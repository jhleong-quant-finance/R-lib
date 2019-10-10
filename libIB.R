##############################################################################################
#
#    libIB
#
#    version : 1.01 
#    modified on 23 NOV 2013
#
#    version 1.01 - fixed defect on db_get_prev_asset_detail(), handle case when prev 
#                   trading day has no asset. exec_id_list excluded order_id because
#                   manually enter order dun have order_id.
#
##############################################################################################
#
#    TODO:
#    
#
##############################################################################################

capture.output(library('IBrokers60', quietly=T,warn.conflicts=T, verbose=T), file='/dev/null');
library('RSQLite')
library(quantmod);
source('/home/jhleong/dev/R/lib/libIB_cfg.R');


#.sym_list <- NULL;
snapShot <- function (twsCon, eWrapper, timestamp, file, playback = 1, ...)
{
  if (missing(eWrapper))
    eWrapper <- eWrapper()
  names(eWrapper$.Data$data) <- eWrapper$.Data$symbols
  con <- twsCon[[1]]
  if (inherits(twsCon, "twsPlayback")) {
    sys.time <- NULL
    while (TRUE) {
      if (!is.null(timestamp)) {
        last.time <- sys.time
        sys.time <- as.POSIXct(strptime(paste(readBin(con,
                                                      character(), 2), collapse = " "), timestamp))
        if (!is.null(last.time)) {
          Sys.sleep((sys.time - last.time) * playback)
        }
        curMsg <- .Internal(readBin(con, "character",
                                    1L, NA_integer_, TRUE, FALSE))
        if (length(curMsg) < 1)
          next
        processMsg(curMsg, con, eWrapper, format(sys.time,
                                                 timestamp), file, ...)
      }
      else {
        curMsg <- readBin(con, character(), 1)
        if (length(curMsg) < 1)
          next
        processMsg(curMsg, con, eWrapper, timestamp,
                   file, ...)
        if (curMsg == .twsIncomingMSG$REAL_TIME_BARS)
          Sys.sleep(5 * playback)
      }
    }
  }
  else {
    while (TRUE) {
      socketSelect(list(con), FALSE, NULL)
      curMsg <- .Internal(readBin(con, "character", 1L,
                                  NA_integer_, TRUE, FALSE))
      if (!is.null(timestamp)) {
        processMsg(curMsg, con, eWrapper, format(Sys.time(),
                                                 timestamp), file, ...)
      }
      else {
        processMsg(curMsg, con, eWrapper, timestamp,
                   file, ...)
      }
      if (!any(sapply(eWrapper$.Data$data, is.na)))
        return(do.call(rbind, lapply(eWrapper$.Data$data,
                                     as.data.frame)))
    }
  }
}

snapShotWithTimeOut <- function (twsCon, eWrapper, timestamp, file, playback = 1, .symList,...)
{
  prev_proc_time <- Sys.time();
  prev_proc_time2 <- prev_proc_time;
  num_report <- 0;
  
  if (missing(eWrapper))
    eWrapper <- eWrapper()
  names(eWrapper$.Data$data) <- eWrapper$.Data$symbols
  con <- twsCon[[1]]
  if (inherits(twsCon, "twsPlayback")) {
    sys.time <- NULL
    while (TRUE) {
      if (!is.null(timestamp)) {
        last.time <- sys.time
        sys.time <- as.POSIXct(strptime(paste(readBin(con,
                                                      character(), 2), collapse = " "), timestamp))
        if (!is.null(last.time)) {
          Sys.sleep((sys.time - last.time) * playback)
        }
        curMsg <- .Internal(readBin(con, "character",
                                    1L, NA_integer_, TRUE, FALSE))
        if (length(curMsg) < 1)
          next
        processMsg(curMsg, con, eWrapper, format(sys.time,
                                                 timestamp), file, ...)
      }
      else {
        curMsg <- readBin(con, character(), 1)
        if (length(curMsg) < 1)
          next
        processMsg(curMsg, con, eWrapper, timestamp,
                   file, ...)
        if (curMsg == .twsIncomingMSG$REAL_TIME_BARS)
          Sys.sleep(5 * playback)
      }
    }
  }
  else {
    while (TRUE) {
      socketSelect(list(con), FALSE, NULL)
      curMsg <- .Internal(readBin(con, "character", 1L,
                                  NA_integer_, TRUE, FALSE))
      if (!is.null(timestamp)) {
        processMsg(curMsg, con, eWrapper, format(Sys.time(),
                                                 timestamp), file, ...)
      }
      else {
        processMsg(curMsg, con, eWrapper, timestamp,
                   file, ...)
      }
      
      curr_time <- Sys.time();
      if(as.numeric(difftime(curr_time, prev_proc_time, units='secs')) > 0.5 ){
        prev_proc_time <- curr_time;
        
        data_test_na <- sapply(eWrapper$.Data$data, is.na);
        if (!any(data_test_na) || num_report > 0)
          return(do.call(rbind, lapply(eWrapper$.Data$data,
                                       as.data.frame)))
        else
        { 
          if(as.numeric(difftime(curr_time, prev_proc_time2, units='secs')) > 5 ){
            num_report <- num_report + 1;
            prev_proc_time2 <- curr_time;
            sym_no_data <- NULL;
            for(i in 1:length(eWrapper$.Data$data))
            {
              if(any(sapply(eWrapper$.Data$data[[i]], is.na)))
                sym_no_data <- c(sym_no_data, i);
            }
            
            cat(as.character(Sys.time()), ' ',length(sym_no_data) ,
                      ' contract not yet received: ', paste(.symList[sym_no_data],sep=' '),'\n');
          }
        }
      }
    }
  }
}

eWrapper.data.last.vol <- function (n) 
{
  eW <- eWrapper(NULL)
  eW$assign.Data("data", rep(list(structure(.xts(matrix(rep(NA_real_, 
                                                            2), ncol = 2), 0), .Dimnames = list(NULL, c("Last", "Volume")))), n))
  eW$tickPrice <- function(curMsg, msg, timestamp, file, ...) {
    tickType = msg[3]
    msg <- as.numeric(msg)
    id <- msg[2]
    data <- eW$get.Data("data")
    attr(data[[id]], "index") <- as.numeric(Sys.time())
    nr.data <- NROW(data[[id]])
    
    if (tickType == .twsTickType$LAST) {
      data[[id]][nr.data, 1] <- msg[4]
    }
    eW$assign.Data("data", data)
    c(curMsg, msg)
  }
  eW$tickSize <- function(curMsg, msg, timestamp, file, ...) {
    data <- eW$get.Data("data")
    tickType = msg[3]
    msg <- as.numeric(msg)
    id <- as.numeric(msg[2])
    attr(data[[id]], "index") <- as.numeric(Sys.time())
    nr.data <- NROW(data[[id]])
    if (tickType == .twsTickType$VOLUME) {
      data[[id]][nr.data, 2] <- msg[4]
    }
    eW$assign.Data("data", data)
    c(curMsg, msg)
  }
  return(eW)
}

eWrapper.data.last.vol.WithTimeOut <- function (n) 
{
  eW <- eWrapper(NULL)
  eW$assign.Data("data", rep(list(structure(.xts(matrix(rep(NA_real_, 
                                                            2), ncol = 2), 0), .Dimnames = list(NULL, c("Last", "Volume")))), n))
  eW$tickPrice <- function(curMsg, msg, timestamp, file, ...) {
    tickType = msg[3]
    
    if (tickType == .twsTickType$LAST) {
      msg <- as.numeric(msg)
      id <- msg[2]
      data <- eW$get.Data("data")
      attr(data[[id]], "index") <- as.numeric(Sys.time())
      nr.data <- NROW(data[[id]])
      
      data[[id]][nr.data, 1] <- msg[4]    
      eW$assign.Data("data", data)
      c(curMsg, msg)
    }
  }
  
  eW$tickSize <- function(curMsg, msg, timestamp, file, ...) {
    
    tickType = msg[3]
    if (tickType == .twsTickType$VOLUME) {
      data <- eW$get.Data("data")
      msg <- as.numeric(msg)
      id <- as.numeric(msg[2])
      attr(data[[id]], "index") <- as.numeric(Sys.time())
      nr.data <- NROW(data[[id]])
      
      data[[id]][nr.data, 2] <- msg[4]
      eW$assign.Data("data", data)
      c(curMsg, msg)
    }
  }
  return(eW)
}

getLastPrice.IB <- function(sym_v)
{
  sym_v <- gsub("\\.", " ", sym_v);
  sym_v <- gsub("-", " ", sym_v);
  max <- 80
  x <- seq_along(sym_v)
  sym_grp <- split(sym_v, ceiling(x/max))
  res <- NULL;
  
  for(c_symbol_grp in sym_grp)
  {
    tws <- twsConnect(clientId=100);
    setServerLogLevel(tws,5);
    v_tickerId = "1"
    res_t <- reqMktData(tws, lapply(c_symbol_grp,twsSTK), tickerId = v_tickerId,
                        eventWrapper=eWrapper.data.last.vol(length(c_symbol_grp)),CALLBACK=snapShot)  
#    res_t <- reqMktData(tws, lapply(c_symbol_grp,twsSTK), tickerId = v_tickerId,
#                        eventWrapper=eWrapper.data(length(c_symbol_grp)),CALLBACK=snapShot)  
    cancelMktData(tws,v_tickerId)
    res_t <- cbind(res_t, 'Symbol' = c_symbol_grp)
    res <- rbind(res, res_t)
    twsDisconnect(tws)
  }
  
  return(res)
}

# To take care ambiguous for some symbol.
twsSTK_mod <- function(symbol, ...)
{
  if(symbol == 'CHE'){
    twsSTK(symbol, exch="SMART", primary="NYSE", ...);    
  } else {
    twsSTK(symbol, ...);
  }
}

getLastPrice.IB.WithTimeOut <- function(sym_v)
{
  sym_v <- gsub("\\.", " ", sym_v);
  sym_v <- gsub("-", " ", sym_v);
  max <- 80
  x <- seq_along(sym_v)
  sym_grp <- split(sym_v, ceiling(x/max))
  res <- NULL;
  
  # reserve 4 connections and rotate, avoid connection in used error
  clientId_list <- c(.client_id_market_data:.client_id_market_data + 3);
  client_id <- .client_id_market_data;
  
  for(c_symbol_grp in sym_grp)
  {
    if(client_id > .client_id_market_data + 3){
      client_id = .client_id_market_data;
    }
    
    tws <- twsConnect47(clientId=client_id);
    #setServerLogLevel(tws,5);
    v_tickerId = "1"
    res_t <- reqMktData(tws, lapply(c_symbol_grp,twsSTK_mod), tickerId = v_tickerId,
                        eventWrapper=eWrapper.data.last.vol.WithTimeOut(length(c_symbol_grp)),
                        CALLBACK=snapShotWithTimeOut, .symList=c_symbol_grp)  
    cancelMktData(tws, v_tickerId);
    twsDisconnect(tws);
    
    res_t <- cbind(res_t, 'Symbol' = c_symbol_grp);
    res <- rbind(res, res_t);
    
    client_id <- client_id + 1; # next client id
  }
  
  return(res);
}


getLastPrice.IB.test <- function(sym_v, from_date)
{
  # yahoo daily historical data only available on the next day.
  if(as.Date(trunc(Sys.time(), "days")) <= as.Date(from_date))
  {
    cat('Historical data not yet available.')
    return(NULL);
  }
  
  library('quantmod', quietly = T);
  
  sym_v <- sym_v[ !grepl("\\^", sym_v)]
  sym_v <- gsub("\\.", "-", sym_v)
  price <- NULL;

  for(c_symbol in sym_v)
  {
    try(Sys.sleep(abs(rnorm(1, mean = 0.5, sd =0.2))))
    stock_qoute <- NULL;
    try(stock_qoute <- getSymbols(c_symbol, from=from_date, src="yahoo", auto.assign=FALSE));
      if(!is.null(stock_qoute)){
        
        stock_qoute_price <- data.frame(Symbol=c_symbol, Last=Op(last(stock_qoute)), Close=Cl(last(stock_qoute)));
        # used Last from the dataframe for OPEN price
        names(stock_qoute_price) <- c('Symbol', 'Last','Close');
        price <- rbind(price, stock_qoute_price);
      }
  }
  
  # second run of downloading from Yahoo
  sym_v2 <- setdiff(sym_v, price$Symbol);
  for(c_symbol in sym_v2)
  {
    try(Sys.sleep(abs(rnorm(1, mean = 1.0, sd =0.5))))
    stock_qoute <- NULL;
    try(stock_qoute <- getSymbols(c_symbol, from=from_date, src="yahoo", auto.assign=FALSE));
    if(!is.null(stock_qoute)){
      
      stock_qoute_price <- data.frame(Symbol=c_symbol, Last=Op(last(stock_qoute)), Close=Cl(last(stock_qoute)));
      # used Last from the dataframe for OPEN price
      names(stock_qoute_price) <- c('Symbol', 'Last','Close');
      price <- rbind(price, stock_qoute_price);
    }
  }
  
  # third run of downloading from Yahoo
  sym_v3 <- setdiff(sym_v, price$Symbol);
  for(c_symbol in sym_v3)
  {
    try(Sys.sleep(abs(rnorm(1, mean = 2.0, sd =1.0))))
    stock_qoute <- NULL;
    try(stock_qoute <- getSymbols(c_symbol, from=from_date, src="yahoo", auto.assign=FALSE));
    if(!is.null(stock_qoute)){
      stock_qoute_price <- data.frame(Symbol=c_symbol, Last=Op(last(stock_qoute)), Close=Cl(last(stock_qoute)));
      # used Last from the dataframe for OPEN price
      names(stock_qoute_price) <- c('Symbol', 'Last','Close');
      price <- rbind(price, stock_qoute_price);
    }
  }
  
  sym_v4 <- setdiff(sym_v, price$Symbol);
  if(length(sym_v4) > 0)
  {
    cat('There is ', length(sym_v4), ' symbol(s) not able to download from Yahoo\n');
    print(sym_v4);
  }
  
  rownames(price) <- NULL;
  return(price)
}

getOpenClosePrice.IB <- function(sym, from_date)
{
  tws <- twsConnect47(clientId=.client_id_market_data);
  
  from_date_time <- paste(from_date, ' 16:00:00 EST5EDT', sep='');
  IB_Price <- lapply(sym, function(x) reqHistoricalData(tws,twsSTK(x), barSize='1 day',  
                                                            duration = "1 D", endDateTime=from_date_time));
  twsDisconnect(tws) 
  
  
  num_stock_traded_IB <- length(IB_Price);
  openClosePrice <- data.frame(Symbol=rep("", num_stock_traded_IB), 
                               IB_Open=rep(NA, num_stock_traded_IB),  
                               IB_Close=rep(NA, num_stock_traded_IB),
                               stringsAsFactors=FALSE);
  
  for(i in 1:num_stock_traded_IB)
  {
    openClosePrice[i,1] = sym[i];
    openClosePrice[i,2] = Op(IB_Price[[i]]);
    openClosePrice[i,3] = Cl(IB_Price[[i]]);
  }
  
  return(openClosePrice);
  
}

getFullDayPrice.IB <- function(sym, from_date)
{  
  tws <- twsConnect47(clientId=.client_id_market_data);
  
  from_date_time <- paste(from_date, ' 16:00:00 EST5EDT', sep='');
  ldaily_price <- lapply(sym, function(x) reqHistoricalData(tws,twsSTK(x), barSize='1 min',  
                                                            duration = "1 D", endDateTime=from_date_time));
  twsDisconnect(tws) 
  return(ldaily_price);
  
}


#############################################################################
#
# save today executed order to db
#
#############################################################################



# only set trading_date_str for special case.
db_ins_exection <- function(tws_conn, trading_date_str=NULL, conn=db_conn){

  if(is.null(trading_date_str)){
    lastPfUpdate <- strptime(paste0(trading_date_str,"-00:00:00"), 
                             format="%Y%m%d-%H:%M:%S ", tz="EST5EDT") # start from records which is 4 hours earlier 
    time <- strftime(lastPfUpdate, format="%Y%m%d-%H:%M:%S {%Z}"); 
  } else { time <- ""; }
  
  m_ExecutionFilter<-twsExecutionFilter(clientId="0", #all clients "0"  or this client: 
                                      acctCode="", # leave empty
                                      time=time, #format:  "yyyymmdd-hh:mm:ss"
                                      symbol="",
                                      secType="",
                                      exchange="",
                                      side="");

  exec_df <- twsExecutionRec(reqExecutions(tws_conn, reqId="0", 
                                           ExecutionFilter=m_ExecutionFilter, timeout=60));
  
  if(!is.null(exec_df)){
    # remove those execution that input from the TWS which has client_id 0
    #exec_df <- exec_df[which(!exec_df$clientId == 0),]
    
    if(NROW(exec_df) > 0){
      # add a date column as string
      exec_df <- data.frame(exec_df[1:3], 
                            exec_date=strftime(exec_df$time, format="%Y%m%d", tz='EST5EDT'),
                            exec_time=strftime(exec_df$time, format="%H:%M:%S", tz='EST5EDT'),
                            exec_df[4:14],
                            status="I",     #status inserted
                            mod_time=strftime(Sys.time(), format="%Y%m%d %H:%M:%S"));
      
      r <- dbWriteTable(conn, "execution", exec_df, append=T, row.names=F);
      if(!r){
        stop("failed to insert into table execution");
      }
    }
    
  } else {
    print("no new execution data.")
  }
}


# process the portfolio upto the all available execution data , then build the 
# report according to trading_date_str !!!!!


# primary build for processing BOG reporting
db_process_BOG_execution <- function(conn=db_conn, debug_flag=F){
  
  
  # create savepoint
  dbGetQuery(conn, "SAVEPOINT db_process_BOG_execution;");
  
  tryCatch({
    
    for(portf_id in c(.buy_on_gap_sp500_portf_id, 
                      .buy_on_gap_sp600_portf_id)){
      
      order_ref <- db_get_order_ref(portf_id, conn);
            
      # get execution inserted with status 'I'
      # get the list of exec_id that are processing, later will update with status 'P'
      exec_id_list <- dbGetQuery(conn, paste0("select exec_id from execution ",
                                              "where order_ref = '",order_ref,"' and ",                                                   
                                              "status = 'I' "));
      
      exec_date_list <- dbGetQuery(conn, paste0("select distinct exec_date from execution ",
                                                "where order_ref = '",order_ref,"' and ",                                                   
                                                "status = 'I' ",
                                                "order by exec_date asc"));
      
      if(NROW(exec_date_list)==0)
        next;
      
      # process for each date in asc order
      for(trading_date_str in exec_date_list[,1]){
        
        traded_sym_list1 <- dbGetQuery(conn, paste0("select distinct local 'Symbol' from execution ",
                                                    "where order_ref = '",order_ref,"' and ",
                                                    "(status ='I' or status ='P') and ",
                                                    "exec_date = '",trading_date_str, "'"));
        
        #       traded_sym_list2 <- dbGetQuery(conn, paste0("select distinct local 'Symbol' from asset ",
        #                                                   "where portf_id = '",portf_id,"' "));
        asset <- db_get_prev_asset_detail(portf_id, trading_date_str, conn);
        
        sym_list <- unique(c(as.character(traded_sym_list1$Symbol),
                             as.character(asset$Symbol)));
        
        # download stock historical quote from IB
        openClosePrice <- getOpenClosePrice.IB(sym_list, trading_date_str);
        
        if(debug_flag) { print(openClosePrice); }
        
        # processing all rec for trading_date_str, including status 'I' and 'P'
        bot <- dbGetQuery(conn, paste0("select  local 'Symbol', ",
                                       "sum(shares * price) / sum(shares) 'AvgBuyPrice' , ",
                                       "sum(shares) 'B.Qty' , ",
                                       "sum(commission) 'B.Commission' ",
                                       "from execution ",
                                       "where side = 'BOT' and order_ref = '",order_ref,"' and ",    
                                       "(status ='I' or status ='P') and ",
                                       "exec_date = '",trading_date_str,"' ",
                                       "group by local order by local asc"));
        
        sld <- dbGetQuery(conn, paste0("select  local 'Symbol', ",
                                       "sum(shares * price) / sum(shares) 'AvgSellPrice' , ",
                                       "sum(shares) 'S.Qty' , ",
                                       "sum(commission) 'S.Commission', ",
                                       "ifnull(sum(realizedPNL),0) 'realizedPNL' ",                                          
                                       "from execution ",
                                       "where side = 'SLD' and order_ref = '",order_ref,"' and ",
                                       "(status ='I' or status ='P') and ",
                                       "exec_date = '",trading_date_str,"' ",
                                       "group by local order by local asc"));
        
        # when no rec, the return df is all chr
        bot <- transform(bot, AvgBuyPrice=as.numeric(AvgBuyPrice),
                         B.Qty=as.integer(B.Qty), B.Commission=as.numeric(B.Commission));
        sld <- transform(sld, AvgSellPrice=as.numeric(AvgSellPrice),
                         S.Qty=as.integer(S.Qty), S.Commission=as.numeric(S.Commission), 
                         realizedPNL=as.numeric(realizedPNL));
        
        trade_ret <- merge(x=bot, y=sld, by='Symbol', all=T);
        #remove NA
        trade_ret[is.na(trade_ret)] <- 0;
        
        # combine buy and sell commission
        trade_ret <- data.frame(trade_ret[,c(1,2,3,5,6,8)], 
                                Commission=(trade_ret$B.Commission + trade_ret$S.Commission));
        
        if(debug_flag){ print(bot); print(sld); print(trade_ret); }
        
        # calculate return
        ret <- (((trade_ret[,'AvgSellPrice'] * trade_ret[,'S.Qty']) / 
                   (trade_ret[,'AvgSellPrice'] * trade_ret[,'S.Qty'] - trade_ret[,'realizedPNL'])) - 1 ) * 100;
        ret[is.nan(ret)] <- 0;
        trade_ret <- data.frame(trade_ret, ret=ret)
        
        trade_ret <- merge(x=trade_ret, y=openClosePrice, by='Symbol', all.x=T);
        rownames(trade_ret) <- NULL;
        colnames(trade_ret) <- c('Symbol','AvgBuyPrice','B.Qty','AvgSellPrice',
                                 'S.Qty','realizedPNL','Commission','Return %','Open Price','Closing Price');
        
        # insert trade_ret into table trade_summary      
        db_ins_trade_summary(portf_id, trading_date_str, trade_ret, conn);
        
        #asset <- db_get_prev_asset_detail(portf_id, trading_date_str, conn);
        
        a <- merge(x=trade_ret, y=asset, all=T)
        pos_open <- data.frame(Symbol=a$Symbol,                                
                               Quantity=(ifelse(is.na(a$B.Qty), 0,a$B.Qty)
                                         - ifelse(is.na(a$S.Qty), 0,a$S.Qty)
                                         + ifelse(is.na(a$asset_qty),0,a$asset_qty)
                               ), stringsAsFactors=F
        );
        
        pos_open <- merge(x=pos_open, y=openClosePrice, by='Symbol', all.x=T);
        pos_open <- pos_open[,-3];
        colnames(pos_open) <- c('Symbol', 'Quantity','Closing Price');
        pos_open$'Market Value' <- pos_open$'Closing Price' * pos_open$Quantity;
        
        logic_non_empty <- (pos_open$Quantity==0)
        if(any(logic_non_empty))
          pos_open <- pos_open[-which(logic_non_empty),];
        
        rownames(pos_open) <- NULL;
          
        db_ins_asset_detail(portf_id, trading_date_str, pos_open, conn);
                
        bought_amt <- sum(trade_ret$AvgBuyPrice * trade_ret$B.Qty, na.rm=T);
        sold_amt <- sum(trade_ret$AvgSellPrice * trade_ret$S.Qty, na.rm=T);
        total_commission <- sum(trade_ret$Commission, na.rm=T);
        total_PNL <- sum(trade_ret$realizedPNL, na.rm=T);
        
        portf_prev <- db_get_portf_prev_amt(portf_id, trading_date_str, conn);
        #       dbGetQuery(conn, paste0("select curr_cash 'cash', curr_total 'portf_amt' from portf_detail ",
        #                                                    "where portf_id=",portf_id," and ",
        #                                                    "exec_date < '",trading_date_str,"' ",
        #                                                    "order by exec_date desc limit 1"));
        
        curr_asset <- sum(pos_open$'Market Value', na.rm=T);
        curr_cash <- portf_prev$cash - bought_amt + sold_amt - total_commission;
        curr_total <- curr_asset + curr_cash;
        
        db_ins_portf_detail(portf_id, trading_date_str, bought_amt, sold_amt, total_commission, 
                            curr_total, curr_cash, curr_asset, total_PNL, conn);
        db_update_portfolio(portf_id, trading_date_str, curr_total, curr_cash, curr_asset, conn);
        
        daily_ret <- (curr_total / portf_prev$portf_amt - 1) * 100;
        
        # get market ret
        if(portf_id == .buy_on_gap_sp500_portf_id){
          mkt_sym <- "^GSPC";
        } else if(portf_id == .buy_on_gap_sp600_portf_id){
          mkt_sym <- "IJR";
        }
        
        # yahoo historical qoute only available on the next day.
        dt_trading_date <- strptime(trading_date_str, "%Y%m%d", tz='ETS5EDT')
        if( as.numeric(difftime(Sys.time() , dt_trading_date, units='days'))  > 1.25){
          mkt_quote <- getSymbols(mkt_sym, src = "yahoo", from = (as.Date(dt_trading_date) - 5), 
                                  to = dt_trading_date, auto.assign=F);
          market_ret <- ROC(Cl(mkt_quote), type="discrete");
          market_ret <- last(market_ret) * 100;
        } else {
          mkt_quote <- getQuote(mkt_sym, src = "yahoo", auto.assign=F);
          market_ret <- mkt_quote$'% Change';
          market_ret <- as.numeric(sub("%","",market_ret))
        }
        
        daily_ret <- round(daily_ret, digits=2);
        market_ret <- round(market_ret, digits=2);
        #     desc <- c('Portofolio Amount:','Asset Amt:','Bought Amount:', 'Sold   Amount:',
        #               'Commission   :','P/L incl Comm:', 
        #               'Ret incl Comm %:', 'S&P 500 Ret %:');                          
        trade_stat <- data.frame(curr_total, curr_asset, bought_amt, sold_amt, 
                                 total_commission, total_PNL, 
                                 daily_ret, market_ret);
        
        dbGetPreparedQuery(conn, paste0("insert or replace into trade_stat ( portf_id, exec_date, portf_amt,",
                                        "asset_amt, bought_amt, sold_amt, commission, realizedPNL,",
                                        "return, mkt_return, mod_time ) values (",
                                        portf_id,",'",trading_date_str,"',",
                                        "?,?,?,?,?,?,?,?",",",
                                        "'",strftime(Sys.time(), format="%Y%m%d %H:%M:%S"),"'",
                                        ")"), trade_stat);  
      }
      
      # update processed execution rec to status 'P'
        dbGetPreparedQuery(conn, paste0("update execution set status = 'P', ",
                                        "mod_time=","'",strftime(Sys.time(), format="%Y%m%d %H:%M:%S"),"' ",
                                        "where exec_id = ? "), exec_id_list);  
    }
    
  },
           error=function(cond) {
             dbGetQuery(conn, "ROLLBACK TO db_process_BOG_execution;"); 
             print("Error in db_process_BOG_execution(), roll back.")
           },
           finally={
             dbGetQuery(conn, "RELEASE db_process_BOG_execution;"); 
           })
}

init_db <- function()
{
  TP_DB_SQLITE_FILE <- '/home/jhleong/dev/R/data/TP_sqlite_db.sqlite';
  #db_conn <- dbConnect(SQLite(), dbname=TP_DB_SQLITE_FILE);
  db_conn <- dbConnect(SQLite(), dbname=TP_DB_SQLITE_FILE);
  #on.exit(dbDisconnect(db_conn)); # disconnect db
  
  return(db_conn);
}

db_get_next_reqId <- function(conn=db_conn, n=1) # generate order id,  n number of id returned.
{
  order_id <- dbGetQuery(conn, "select ifnull(max(seq_value), 2000) from seq_no");
  if(order_id == .Machine$integer.max)
    order_id = 2000;
  
  order_id <- as.numeric(order_id);
  
  dbGetQuery(conn, paste0("update seq_no set seq_value =", order_id + n,
                          ", mod_time='", strftime(Sys.time(), format="%Y%m%d %H:%M:%S"),
                          "' where seq_id = 1000"));
  
  return(order_id:(order_id + n-1));
}

db_get_execution <- function(order_ref, trading_date_str, conn=db_conn) 
{
  bot <- dbGetQuery(conn, paste0("select  local 'Symbol', ",
                                 "sum(shares * price) / sum(shares) 'AvgBuyPrice' , ",
                                 "sum(shares) 'B.Qty' , ",
                                 "sum(commission) 'B.Commission' ",
                                 "from execution ",
                                 "where side = 'BOT' and order_ref = '",order_ref,"' and ",    
                                 "(status ='I' or status ='P') and ",
                                 "exec_date = '",trading_date_str,"' ",
                                 "group by local order by local asc"));
  
  return(bot);
}


# retrieve the previous day asset_detail of trading_date_str
db_get_prev_asset_detail <- function(portf_id, trading_date_str, conn=db_conn) 
{
  asset <- dbGetQuery(conn, paste0("select local 'Symbol', qty 'asset_qty' from ",
                                   "asset_detail where portf_id=",portf_id," and exec_date =",
                                   "(select max(exec_date) from asset_detail where exec_date < '",trading_date_str,"')"                                   
                                   ));
  asset <- asset[which(asset$Symbol!='***NO-ASSET***'),]
  return(asset);
}

db_ins_asset_detail <- function(portf_id, trading_date_str, pos_open, conn=db_conn) 
{
  # remove db rec, eg. rerun
  dbGetQuery(conn, paste0("delete from asset_detail ",
                          "where portf_id = ", portf_id," and ",
                          "exec_date =","'",trading_date_str,"'"));
  
  if(NROW(pos_open) > 0 ){
    dbGetPreparedQuery(conn, paste0("insert or replace into asset_detail ( portf_id, exec_date, local, qty,",
                                    "closing_price, mod_time ) values (",
                                    portf_id,",'",trading_date_str,"',",
                                    "?,?,?",",",
                                    "'",strftime(Sys.time(), format="%Y%m%d %H:%M:%S"),"'",
                                    ")"), pos_open[,1:3]);
  } else {
    dbGetQuery(conn, paste0("insert into asset_detail ( portf_id, exec_date, local, ",
                                    "mod_time ) values (",
                                    portf_id,",'",trading_date_str,"',","'***NO-ASSET***',",                                    
                                    "'",strftime(Sys.time(), format="%Y%m%d %H:%M:%S"),"'",
                                    ")"));
  }
}

db_get_order_ref <- function(portf_id, conn=db_conn) 
{
  order_ref <- dbGetQuery(conn, paste0("select order_ref from portfolio where portf_id=",
                                   portf_id));
  return(as.character(order_ref));
}


db_ins_portf_detail <- function(portf_id, trading_date_str, bought_amt, sold_amt, commission, 
                    curr_total, curr_cash, curr_asset, total_PNL, conn=db_conn){
  prev <- dbGetQuery(conn, paste0("select curr_total 'total', curr_asset 'asset', curr_cash 'cash' from portf_detail ",
                                  "where portf_id=",portf_id," and ",
                                  "exec_date <'",trading_date_str,"' ",
                                  "order by exec_date desc limit 1"
                                  ));
  
  dbGetQuery(conn, paste0("insert or replace into portf_detail (exec_date, portf_id, prev_total, prev_asset,",
                          "prev_cash, bought_amt, sold_amt, commission, curr_asset, curr_cash, curr_total,",
                          "profit, mod_time ) values (" ,
                          "'",trading_date_str, "' ,", portf_id, ",",prev$total,",",prev$asset,",",
                          prev$cash,",",bought_amt,",",sold_amt,",",commission,",",curr_asset,",",
                          curr_cash,",",curr_total,",",total_PNL,",",
                          "'", strftime(Sys.time(), format="%Y%m%d %H:%M:%S"),"'",
                          " ) "));
}

db_get_portf_prev_amt <- function(portf_id, trading_date_str, conn=db_conn)
{
  portf_amt <- dbGetQuery(conn, paste0("select curr_cash 'cash', curr_total 'portf_amt', ",
                                            "curr_asset 'asset' from portf_detail ",
                                            "where portf_id=",portf_id," and ",
                                            "exec_date < '",trading_date_str,"' ",
                                            "order by exec_date desc limit 1"));
  return(portf_amt)
}

db_update_portfolio <- function(portf_id, trading_date_str,  curr_total, curr_cash, curr_asset, conn=db_conn){
  dbGetQuery(conn, paste0("update portfolio set curr_amt =", curr_total ," , ",
                          "curr_cash =",curr_cash, ", curr_asset =", curr_asset," , ",
                          "exec_upd_time='",trading_date_str,"', ",
                          "mod_time='", strftime(Sys.time(), format="%Y%m%d %H:%M:%S"),
                          "' where portf_id =",portf_id));
}

db_ins_trade_summary <- function(portf_id, trading_date_str, trade_ret, conn=db_conn)
{
  # remove db rec, eg. rerun
  dbGetQuery(conn, paste0("delete from trade_summary ",
                          "where portf_id = ", portf_id," and ",
                          "exec_date =","'",trading_date_str,"'"));
  
  dbGetPreparedQuery(conn, paste0("insert or replace into trade_summary ( portf_id, exec_date, local, avg_buy_price,",
                          "b_qty, avg_sell_price, s_qty, realizedPNL, commission, return, open_price,",
                          "closing_price, mod_time ) values (",
                                  portf_id,",'",trading_date_str,"',",
                                  "?,?,?,?,?,?,?,?,?,?",",",
                                  "'",strftime(Sys.time(), format="%Y%m%d %H:%M:%S"),"'",
                                  ")"), trade_ret);
}

db_get_trade_summary <- function(portf_id, trading_date_str, conn=db_conn)
{
  trade_summ <- dbGetQuery(conn, paste0("select local, avg_buy_price, b_qty, avg_sell_price, ",
                                        "s_qty, realizedPNL, commission, return, open_price, closing_price ",
                                        "from trade_summary where portf_id=", portf_id," and ",
                                        "exec_date = '",trading_date_str,"'"
                                        ));
  
  colnames(trade_summ) <- c('Symbol','B.AvgPrc','B.Qty','S.AvgPrc',
                           'S.Qty','Profit','Comm.','Return %','Open Price','Closing Price');
  return(trade_summ);
}

db_get_trade_stat <- function(portf_id, trading_date_str, conn=db_conn)
{
  stat <- dbGetQuery(conn, paste0("select portf_amt, asset_amt, bought_amt, sold_amt, commission,  ",
                                        "realizedPNL, return, mkt_return ",
                                        "from trade_stat where portf_id=", portf_id," and ",
                                        "exec_date = '",trading_date_str,"'"));
  
      desc <- c('Portofolio Amount:','Asset Amount:','Bought Amount:', 'Sold   Amount:',
                'Commission   :','P/L incl Comm:', 
                'Ret incl Comm %:', 'Market Ret %:');
  
  trade_stat <- data.frame(desc, as.numeric(stat[1,]), stringsAsFactors=F);
  colnames(trade_stat) <- c('desc','stat');
  return(trade_stat);
}

db_get_asset_detail <- function(portf_id, trading_date_str, conn=db_conn)
{
  asset <- dbGetQuery(conn, paste0("select local, qty, closing_price, qty * closing_price ",
                                        "from asset_detail where portf_id=", portf_id," and ",
                                        "exec_date = '",trading_date_str,"'"));
  
  colnames(asset) <- c('Symbol','Quantity','Closing price','Market price');
  return(asset);
}


BuyOnGap_placeOrder <- function(twsconn, trade_order)
{
  sym_reqid <- db_get_next_reqId(n=NROW(trade_order));
  trade_order$ReqId <- sym_reqid;
  
  apply(trade_order, 1, function(x)
  IBrokers60:::.placeOrder(twsconn, twsEquity(x['Symbol']), 
                           twsOrder(x['ReqId'], x['Action'], x['NumShare'], x['OrderType'],
                                    ifelse(is.na(x['LmtPrice']), "0.0", x['LmtPrice']), 
                                    orderRef=x['OrderRef']
                                    ))); 
  
  return(trade_order);
}

BuyOnGap_db_ins_orders <- function(trade_rpt, portf_id, conn=db_conn){
  apply(trade_rpt, 1, function(x)
    dbGetQuery(conn, paste0("insert into orders (order_id, client_id, portf_id, local,", 
                            "total_quantity, action, order_type, ",
                            "lmt_price, order_ref, mod_time)" ,
                            "values ( ",x['ReqId'],",",x['ClientId'],",",portf_id,",","'",x['Symbol'],"',",
                            x['NumShare'],",'",x['Action'],"','",x['OrderType'],"',",
                            ifelse(is.na(x['LmtPrice']), "NULL",x['LmtPrice']),
                            ",'",x['OrderRef'] ,"','",
                            strftime(Sys.time(), format="%Y%m%d %H:%M:%S"),
                            "')"
                            )
               )
         );
}
