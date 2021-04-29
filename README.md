# ib_gateway
An IB order gateway, order cache system and market data repeater on REDIS

# Underlying IB/TWS lib:
Version: API 9.76 Release Date: May 08 2019 

# behaviour and configuration

* Load TWS\_API\_ADDR TWS\_API\_PORT TWS\_API\_CLIENTID and TWS\_GATEWAY\_NAME from ENV
* Load REDIS\_HOST REDIS\_PORT and REDIS\_PSWD from ENV

* Auto keep API connectivity to IB/TWS, retry every 20s
* Keep updating working status '[1, timestamp]' in Redis/IBGateway:{name}:status every second. If this status goes wrong, all other data could not be trusted.
* Keep latest account balance with position updated at Redis/IBGateway:{name}:balance
* ~~Keep latest account position updated at Redis/IBGateway:{name}:balance~~
* On-req orderbook subscription, data is published at channel Redis/URANUS:{exchange}:{currency}-{symbol}:full_odbk_channel in URANUS format [bids, asks, timestamp]
* On-req contract querying, hit contracts is published at keys like IBGateway:Contract:{SecType}:{exchange}:{currency}-{symbol}:{expiry}:{multiplier}
	- IBGateway:Contract:FUT:CMECRYPTO:USD-BRR:20210625:5
	- IBGateway:Contract:FUT:CMECRYPTO:USD-BRR:20210625:0.1
	- IBGateway:Contract:STK:SEHK:HKD-1137

# Commands and reponses

Command format: {"id":id, "cmd":command, params:{...}}

ib\_gateway listens command on channel Redis/IBGateway:{name}:CMD , ACK with id at channel Redis/IBGateway:{name}:ACK

* SUB\_ODBK
	- params:contract, subscribe order book.
* RESET
	- reset status as new connected, restart all tasks.
