TAKE_PROFIT_MULTIPLIER = 1.5 #flexible
HARD_STOP_LOSS_PERCENT = 5.0 #flexible
STOP_LOSS_MULTIPLIER = 1.5 #flexible
EMA = [8, 20]  #flexible
VWMA = [20]  #flexible
SIGNAL_TF = "5M" #fixed w/o resampler logic
BASE_TIMEFRAME = "1M" #fixed: fetching 1min candles and then resampling acc to signaltf
SECONDARY_TIMEFRAME = "4H" #fixed w/o resampler logic