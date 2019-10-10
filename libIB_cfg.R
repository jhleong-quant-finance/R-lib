

######################################### client ID #####################################################

.cleint_id_general_purpose = 20 # for execution, commission report ( CLIENT_VERSION 60 ), open for session.
.client_id_buy_on_gap = 1000  # for placeOrder ( CLIENT_VERSION 47 )
.client_id_market_data = 30   # for reqMktData ( CLIENT_VERSION 47 ), open and close within the function

############################# trading strategies related config ##########################################

# Strategy "Buy On Gap SP500"
.buy_on_gap_sp500_portf_id = 1002;
.buy_on_gap_sp500_nskip <- 0;              # num of stock to skip
.buy_on_gap_sp500_nStocksBuy <- 10;        # max num of trade
.buy_on_gap_sp500_min_gap = -0.015;        # mininum gap for trigger a trade, avoid small gap for 
                                           # low volitility stock.
.buy_on_gap_sp500_large_gap = -0.15;        # define a large gap that need manual attention 


# Strategy "Buy On Gap SP600"
.buy_on_gap_sp600_portf_id = 1003;
.buy_on_gap_sp600_nStocksBuy <- 10
.buy_on_gap_sp600_nskip <- 0
.buy_on_gap_sp600_slippage_gap_limit <- 0.25;  # allowed slippage in term % of the open gap.
.buy_on_gap_sp600_large_gap <- -0.3;        # define a large gap that need manual attention 
.buy_on_gap_sp600_alloc_lmt <- 0.25;        # each trade limited % portofolio

