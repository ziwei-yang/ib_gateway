# ib_gateway
IB API sucks

This is an API wrapper for place/modify order, with balance/order cache and on-req market data repeater based on Redis pub/sub channel.

Use ruby/python/nodejs to make trading bot easier, without dealing with dozens of IB API handlers, to save life, and earn life.

# Underlying IB/TWS lib:
Version: API 9.76 Release Date: May 08 2019 

# Behaviour and configuration

* Load `TWS_API_ADDR TWS_API_PORT TWS_API_CLIENTID and TWS_GATEWAY_NAME` from ENV
* Load `REDIS_HOST REDIS_PORT and REDIS_PSWD` from ENV

* Auto keep API connectivity to IB/TWS, retry every 20 seconds
* Keep latest account balance with position updated at Redis `IBGateway:{account}:balance`
* On-req orderbook subscription
	- level1/2 data is published at channel `URANUS:{exchange}:{currency}-{symbol}:full_odbk_channel` in format [bids, asks, timestamp]
	- latest trades is published at channel `URANUS:{exchange}:{currency}-{symbol}:full_tick_channel` in format [trade, timestamp]
* On-req contract querying, hit contracts is published at keys like `IBGateway:Contract:{exchange}:{secType}:{currency}-{symbol}:{expiry}:{multiplier}`
	- `IBGateway:Contract:FUT:CMECRYPTO:USD-BRR:20210625:5`
	- `IBGateway:Contract:FUT:CMECRYPTO:USD-BRR:20210625:0.1`
	- `IBGateway:Contract:STK:SEHK:HKD-1137`
* Market rules are placed at: `IBGateway:MarketRule:{id}`
	- `IBGateway:MarketRule:357`
	- `IBGateway:MarketRule:472`
* Auto query all contract details in user portfolio at startup
* Heartbeat every second, at channel `IBGateway:{name}:ACK`
* Forward TWS message at channel `IBGateway:{name}:ACK`

# Order Cache behaviour

### Start-up:
Refresh all alive orders (complete snapshot), and history orders (may not contain those with 0 trade)
* Setup redis hashmap at:
	- `URANUS:{exchange}:{name}:O:{currency}-{symbol}`
	- `URANUS:{exchange}:{name}:O:{currency}-{symbol}@{expiry}`
	- `URANUS:{exchange}:{name}:O:{currency}-{symbol}@{expiry}@{multiplier}`
* Redis hashmap internal structure:
	- `{ t -> timestamp }`
	- `{ id -> order_json }`
	- `{ client_oid -> order_json }`
* Then, mark OMS cache running with value '1' at:
	- `URANUS:{exchange}:{name}:OMS`

### Work:
* Keep receiving updates from AllOrderHandler
* Broadcast order\_json string at channel:
	- `URANUS:{exchange}:{name}:O_channel`
* Also write order\_json at HSET:
	- `URANUS:{exchange}:{name}:O:{currency}-{symbol}` with key `{i}` and `{client_oid}`

### Tear-down:
mark OMS cache stopped by deleting every `URANUS:{exchange}:{name}:OMS`

# Commands and reponses

Command format: `{"id":id, "cmd":command, params:{...}}`

ib\_gateway listens command on channel `IBGateway:{name}:CMD`, reply ACK with id at channel `Redis/IBGateway:{name}:ACK`

* SUB\_ODBK
	- `{contract={}}`, subscribe orderbook.
* SUB\_TOP
	- `{contract={}}`, subscribe level 1 data and latest trades.
* RESET
	- reset status as new connected, restart all tasks.
* FIND\_CONTRACTS
	- {contract={}} with limit info, query all possible results.
	- secType and symbol is required in contract
* PLACE\_ORDER
	- `{iborder={contract:{full_detail_contract}, order:{}}}`
* CANCEL\_ORDER
	- `{apiOrderId=}`
* CANCEL\_ALL
	- No argument
* ACCOUNT\_LIST
	- No argument
