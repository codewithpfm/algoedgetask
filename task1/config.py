TAKE_PROFIT_MULTIPLIER = 1.5 #flexible
HARD_STOP_LOSS_PERCENT = 5.0 #flexible
STOP_LOSS_MULTIPLIER = 1.5 #flexible
EMA = 8  #flexible
VWMA = 20  #flexible
SIGNAL_TF = "5M" #fixed w/o resampler logic
ATR_PERIOD = 14 #flexible: ATR length used for dynamic SL / TP distances
# ZMQ endpoints.
# Strategy (A) connects to the adapter (B); the adapter connects to the broker (C).
ORDER_ENDPOINT = "tcp://127.0.0.1:5555"          # A PUSH -> B PULL
UPDATE_ENDPOINT = "tcp://127.0.0.1:5556"         # B PUB  -> A SUB
BROKER_ORDER_ENDPOINT = "tcp://127.0.0.1:5557"   # B PUSH -> C PULL
BROKER_UPDATE_ENDPOINT = "tcp://127.0.0.1:5558"  # C PUB  -> B SUB
