library('quantmod');

KLSE_list <- read.csv("/data/finance/MY/company_list/bursa_stock.csv", 
                      header = TRUE,sep=",", as.is=TRUE);

#########################################################################
# implementation for retrieving historical data from ichart yahoo.
# needed by KLSE market.
##########################################################################

getSymbols.yahoo_ichart <-
  function (Symbols, env, return.class = "xts", index.class = "Date", 
            from = "2007-01-01", to = Sys.Date(), ...) 
  {
    importDefaults("getSymbols.yahoo")
    this.env <- environment()
    for (var in names(list(...))) {
      assign(var, list(...)[[var]], this.env)
    }
    if (!exists("adjust", environment())) 
      adjust <- FALSE
    default.return.class <- return.class
    default.from <- from
    default.to <- to
    if (missing(verbose)) 
      verbose <- FALSE
    if (missing(auto.assign)) 
      auto.assign <- TRUE
    yahoo.URL <- "http://ichart.finance.yahoo.com/table.csv?"
    for (i in 1:length(Symbols)) {
      return.class <- getSymbolLookup()[[Symbols[[i]]]]$return.class
      return.class <- ifelse(is.null(return.class), default.return.class, 
                             return.class)
      from <- getSymbolLookup()[[Symbols[[i]]]]$from
      from <- if (is.null(from)) 
        default.from
      else from
      to <- getSymbolLookup()[[Symbols[[i]]]]$to
      to <- if (is.null(to)) 
        default.to
      else to
      from.y <- as.numeric(strsplit(as.character(as.Date(from, 
                                                         origin = "1970-01-01")), "-", )[[1]][1])
      from.m <- as.numeric(strsplit(as.character(as.Date(from, 
                                                         origin = "1970-01-01")), "-", )[[1]][2]) - 1
      from.d <- as.numeric(strsplit(as.character(as.Date(from, 
                                                         origin = "1970-01-01")), "-", )[[1]][3])
      to.y <- as.numeric(strsplit(as.character(as.Date(to, 
                                                       origin = "1970-01-01")), "-", )[[1]][1])
      to.m <- as.numeric(strsplit(as.character(as.Date(to, 
                                                       origin = "1970-01-01")), "-", )[[1]][2]) - 1
      to.d <- as.numeric(strsplit(as.character(as.Date(to, 
                                                       origin = "1970-01-01")), "-", )[[1]][3])
      Symbols.name <- getSymbolLookup()[[Symbols[[i]]]]$name
      Symbols.name <- ifelse(is.null(Symbols.name), Symbols[[i]], 
                             Symbols.name)
      if (verbose) 
        cat("downloading ", Symbols.name, ".....\n\n")
      tmp <- tempfile()
      download.file(paste(yahoo.URL, "s=", Symbols.name, "&a=", 
                          from.m, "&b=", sprintf("%.2d", from.d), "&c=", from.y, 
                          "&d=", to.m, "&e=", sprintf("%.2d", to.d), "&f=", 
                          to.y, "&g=d&q=q&y=0", "&z=", Symbols.name, "&x=.csv", 
                          sep = ""), destfile = tmp, quiet = !verbose)
      fr <- read.csv(tmp)
      unlink(tmp)
      
      Symbols_ticker <- convert_to_KLSE_ticker(Symbols[[i]])
      
      if (verbose) 
        cat("done.\n")
      fr <- xts(as.matrix(fr[, -1]), as.POSIXct(fr[, 1], tz = Sys.getenv("TZ")), 
                src = "yahoo", updated = Sys.time())
      colnames(fr) <- paste(toupper(gsub("\\^", "", Symbols_ticker)), 
                            c("Open", "High", "Low", "Close", "Volume", "Adjusted"), 
                            sep = ".")
      if (adjust) {
        div <- getDividends(Symbols[[i]], from = from, to = to, 
                            auto.assign = FALSE)
        spl <- getSplits(Symbols[[i]], from = from, to = to, 
                         auto.assign = FALSE)
        adj <- na.omit(adjRatios(spl, div, Cl(fr)))
        fr[, 1] <- fr[, 1] * adj[, "Split"] * adj[, "Div"]
        fr[, 2] <- fr[, 2] * adj[, "Split"] * adj[, "Div"]
        fr[, 3] <- fr[, 3] * adj[, "Split"] * adj[, "Div"]
        fr[, 4] <- fr[, 4] * adj[, "Split"] * adj[, "Div"]
        fr[, 5] <- fr[, 5] * (1/adj[, "Div"])
      }
      fr <- quantmod:::convert.time.series(fr = fr, return.class = return.class)
      if (is.xts(fr)) 
        indexClass(fr) <- index.class
      Symbols[[i]] <- toupper(gsub("\\^", "", Symbols_ticker))
      if (auto.assign) 
        assign(Symbols[[i]], fr, env)
      if (i >= 5 && length(Symbols) > 5) {
        message("pausing 1 second between requests for more than 5 symbols")
        Sys.sleep(1)
      }
    }
    if (auto.assign) 
      return(Symbols)
    return(fr)
  }

getSymbols_KLSE <-
  function (Symbols = NULL, env = .GlobalEnv, reload.Symbols = FALSE, 
            verbose = FALSE, warnings = TRUE, src = "yahoo", symbol.lookup = TRUE, 
            auto.assign = TRUE, ...) 
  {
    importDefaults("getSymbols")
    if (!auto.assign && length(Symbols) > 1) 
      stop("must use auto.assign=TRUE for multiple Symbols requests")
    if (symbol.lookup && missing(src)) {
      symbols.src <- getOption("getSymbols.sources")
    }
    else {
      symbols.src <- src[1]
    }
    if (is.character(Symbols)) {
      Symbols <- unlist(strsplit(Symbols, ";"))
      tmp.Symbols <- vector("list")
      for (each.symbol in Symbols) {
        if (each.symbol %in% names(symbols.src)) {
          tmp.src <- symbols.src[[each.symbol]]$src[1]
          if (is.null(tmp.src)) {
            tmp.Symbols[[each.symbol]] <- src[1]
          }
          else {
            tmp.Symbols[[each.symbol]] <- tmp.src
          }
        }
        else {
          tmp.Symbols[[each.symbol]] <- src[1]
        }
      }
      Symbols <- tmp.Symbols
    }
    old.Symbols <- NULL
    if (exists(".getSymbols", env, inherits = FALSE)) {
      old.Symbols <- get(".getSymbols", env)
    }
    if (reload.Symbols) {
      Symbols <- c(Symbols, old.Symbols)[unique(names(c(Symbols, 
                                                        old.Symbols)))]
    }
    if (!auto.assign && length(Symbols) > 1) 
      stop("must use auto.assign=TRUE when reloading multiple Symbols")
    if (!is.null(Symbols)) {
      Symbols <- as.list(unlist(lapply(unique(as.character(Symbols)), 
                                       FUN = function(x) {
                                         Symbols[Symbols == x]
                                       })))
      all.symbols <- list()
      for (symbol.source in unique(as.character(Symbols))) {
        if(symbol.source == 'yahoo_ichart')
          current.symbols <- convert_to_KLSE_counter(names(Symbols[Symbols == symbol.source]))
        else
          current.symbols <- names(Symbols[Symbols == symbol.source])
        
        symbols.returned <- do.call(paste("getSymbols.", 
                                          symbol.source, sep = ""), list(Symbols = current.symbols, 
                                                                         env = env, verbose = verbose, warnings = warnings, 
                                                                         auto.assign = auto.assign, ...))
        if (!auto.assign) 
          return(symbols.returned)
        for (each.symbol in symbols.returned) all.symbols[[each.symbol]] <- symbol.source
      }
      
      req.symbols <- names(all.symbols)
      
      all.symbols <- c(all.symbols, old.Symbols)[unique(names(c(all.symbols, 
                                                                old.Symbols)))]
      if (auto.assign) {
        assign(".getSymbols", all.symbols, env)
        if (identical(env, .GlobalEnv)) 
          return(req.symbols)
        return(env)
      }
    }
    else {
      warning("no Symbols specified")
    }
  }


convert_to_KLSE_ticker <- function(x)
{
  x <- gsub("\\.KL","",x)
  return(sapply(x, function (x) KLSE_list[KLSE_list$stock_code == x, 'ticker_code']))
}

convert_to_KLSE_counter <- function(x)
{
  return(sapply(x, 
                function(x) paste(KLSE_list[KLSE_list$ticker_code == x, 'stock_code'], 
                                  "KL", sep=".")))
}