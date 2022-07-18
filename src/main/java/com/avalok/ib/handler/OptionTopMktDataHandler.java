package com.avalok.ib.handler;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.avalok.ib.IBContract;
import com.bitex.util.Redis;
import com.ib.client.TickAttrib;
import com.ib.client.TickType;
import com.ib.controller.ApiController.IOptHandler;
import redis.clients.jedis.Jedis;

import java.util.function.Consumer;

import static com.bitex.util.DebugUtil.*;
import static com.bitex.util.DebugUtil.warn;

public class OptionTopMktDataHandler implements IOptHandler{
    protected boolean _debug = false;
    public final int max_depth = 1;
    protected IBContract _contract;
    protected final double multiplier;
    protected final double marketDataSizeMultiplier;
    protected final String publishODBKChannel; // Publish odbk to universal system
    protected final String publishTickChannel; // Publish odbk to universal system
    protected final JSONArray topDataSnapshot = new JSONArray();
    protected final JSONObject[] topAsks = new JSONObject[] {new JSONObject()};
    protected final JSONObject[] topBids = new JSONObject[] {new JSONObject()};
    protected boolean topDataInited = false; // Wait until all ASK/BID filled

    protected final JSONArray newTicksData = new JSONArray();
    protected final JSONArray newTicks = new JSONArray();
    // Wait until tickSnapshotEnd(), This function suddenly does not work any more. 20200514
    // protected boolean tickDataInited = false;
    protected boolean tickDataInited = true;

    private Consumer<Jedis> broadcastTopLambda;
    private Consumer<Jedis> broadcastTickLambda;

    public OptionTopMktDataHandler(IBContract contract, boolean broadcastTop, boolean broadcastTick) {
        _contract = contract;
        publishODBKChannel = "URANUS:"+contract.exchange()+":"+contract.pair()+":full_odbk_channel";
        publishTickChannel = "URANUS:"+contract.exchange()+":"+contract.pair()+":full_tick_channel";
        if (contract.multiplier() == null)
            multiplier = 1;
        else
            multiplier = Double.parseDouble(contract.multiplier());
        while (true) {
            JSONObject contractDetail = ContractDetailsHandler.findDetails(contract);
            if (contractDetail != null) {
                marketDataSizeMultiplier = contractDetail.getIntValue("mdSizeMultiplier");
                break;
            }
            log("wait for contract details " + publishODBKChannel);
            sleep(1);
        }
        // Pre-build snapshot
        topDataSnapshot.add(topBids);
        topDataSnapshot.add(topAsks);
        topDataSnapshot.add(0); // Timestamp
        // Pre-build broadcast lambda.
        if (broadcastTop) {
            broadcastTopLambda = new Consumer<Jedis> () {
                @Override
                public void accept(Jedis t) {
                    topDataSnapshot.set(2, System.currentTimeMillis());
                    // Dont do this when same depth handler is working.
                    Long depthT = DeepMktDataHandler.CHANNEL_TIME.get(publishODBKChannel);
                    if (depthT == null || depthT < System.currentTimeMillis() - 1000) {
                        if (_debug)
                            warn("Publish to " + publishODBKChannel);
                        t.publish(publishODBKChannel, JSON.toJSONString(topDataSnapshot));
                    } else if (_debug)
                        warn("Dont publish to " + publishODBKChannel);
                }
            };
        }

        // Pre-build tick data.
        newTicks.add(new JSONObject());
        newTicksData.add(newTicks);
        newTicksData.add(0); // Timestamp
        // Pre-build broadcast lambda.
        if (broadcastTop) {
            broadcastTickLambda = new Consumer<Jedis>() {
                @Override
                public void accept(Jedis t) {
                    if (_debug)
                        warn("Publish to " + publishTickChannel);
                    t.publish(publishTickChannel, JSON.toJSONString(newTicksData));
                }
            };
        }

    }

    public IBContract contract() { return _contract; }

    Double bidPrice, askPrice; // To determine last trade side

    /////////////////////////////////////////////////////
    // Trade tick always comes with triple(or more) messages:
    // tickString() tickType LAST_TIMESTAMP VALUE: 1620015539
    // tickPrice() tickType LAST price 58280.0
    // tickSize() tickType LAST_SIZE size 1
    // tickSize() tickType LAST_SIZE size 2 (same price, multiple trades)
    // tickPrice() tickType LAST price 59120.0 (if have more trade at same time)
    // tickSize() tickType LAST_SIZE size 1 (if have more trade at same time)
    /////////////////////////////////////////////////////
    private Long lastTickTime = 0l;
    private Double lastTickPrice = null;
    private Double lastTickSize = null;
    private JSONObject lastTrade;


    @java.lang.Override
    public void tickPrice(TickType tickType, double price, TickAttrib attribs) {
        if (_debug)
            info(_contract.shownName() + " tickPrice() tickType " + tickType + " price " + price + " attribs " + attribs);
        switch (tickType) {
            case BID:
                bidPrice = price;
                topBids[0].put("p", price);
                if (topBids[0].getDouble("s") == null) break;
                if (tickDataInited) broadcastTop(false);
                break;
            case ASK:
                askPrice = price;
                topAsks[0].put("p", price);
                if (topAsks[0].getDouble("s") == null) break;
                if (tickDataInited) broadcastTop(false);
                break;
            case LAST:
                lastTickPrice = price;
                break;
            case OPEN:
                break;
            case CLOSE:
                break;
            case LOW:
                break;
            case HIGH:
                break;
            case HALTED:
                break;
            // Refer https://interactivebrokers.github.io/tws-api/market_data_type.html
            // If live data is available a request for delayed data would be ignored by TWS
            // DELAYED types
            case DELAYED_BID:
                bidPrice = price;
                topBids[0].put("p", price);
                if (topBids[0].getDouble("s") == null) break;
                if (tickDataInited) broadcastTop(false);
                break;
            case DELAYED_ASK:
                askPrice = price;
                topAsks[0].put("p", price);
                if (topAsks[0].getDouble("s") == null) break;
                if (tickDataInited) broadcastTop(false);
                break;
            case DELAYED_LAST:
                // if have market data subscription only type LAST, don't have type DELAYED_LAST
                lastTickPrice = price;
                // info(_contract.shownName() + " tickPrice() tickType " + tickType + " price " + price + " attribs " + attribs);
                break;
            case DELAYED_OPEN:
                break;
            case DELAYED_CLOSE:
                break;
            case DELAYED_LOW:
                break;
            case DELAYED_HIGH:
                break;
            default:
                info(_contract.shownName() + " tickPrice() tickType " + tickType + " price " + price + " attribs " + attribs);
                break;
        }
    }

    @java.lang.Override
    public void tickSize(TickType tickType, int size_in_lot) {
        Double size;
        if (_contract.exchange().equals("SEHK") || _contract.exchange().equals("HKFE")){
            size = size_in_lot * 1.0;
        } else {
            size = size_in_lot * multiplier * marketDataSizeMultiplier;
        }
        if (_debug)
            info(_contract.shownName() + " tickSize() tickType " + tickType + " size " + size);
        switch (tickType) {
            case BID_SIZE:
                topBids[0].put("s", size);
                if (topBids[0].getDouble("p") == null) break;
                if (tickDataInited) broadcastTop(false);
                break;
            case ASK_SIZE:
                topAsks[0].put("s", size);
                if (topAsks[0].getDouble("p") == null) break;
                if (tickDataInited) broadcastTop(false);
                break;
            case LAST_SIZE:
                lastTickSize = size;
                recordLastTrade();
                break;
            case VOLUME:
                break;
            case OPEN:
                break;
            case CLOSE:
                break;
            case LOW:
                break;
            case HIGH:
                break;
            case HALTED:
                break;
            // Refer https://interactivebrokers.github.io/tws-api/market_data_type.html
            // If live data is available a request for delayed data would be ignored by TWS
            // DELAYED types
            case DELAYED_BID_SIZE:
                topBids[0].put("s", size);
                if (topBids[0].getDouble("p") == null) break;
                if (tickDataInited) broadcastTop(false);
                break;
            case DELAYED_ASK_SIZE:
                topAsks[0].put("s", size);
                if (topAsks[0].getDouble("p") == null) break;
                if (tickDataInited) broadcastTop(false);
                break;
            case DELAYED_LAST_SIZE:
                // if have market data subscription only type LAST, don't have type DELAYED_LAST_SIZE
                lastTickSize = size;
                recordLastTrade();
                // info(_contract.shownName() + " tickSize() tickType " + tickType + " size " + size);
                break;
            case DELAYED_VOLUME:
                break;
            case DELAYED_OPEN:
                break;
            case DELAYED_CLOSE:
                break;
            case DELAYED_LOW:
                break;
            case DELAYED_HIGH:
                break;
            default:
                info(_contract.shownName() + " tickSize() tickType " + tickType + " size " + size);
                break;
        }
    }

    @java.lang.Override
    public void tickString(TickType tickType, String value) {
        if (_debug)
            info(_contract.shownName() + " tickString() tickType " + tickType + " VALUE: " + value);
        switch (tickType) {
            case LAST_TIMESTAMP:
                lastTickTime = Long.parseLong(value + "000");
                // lastTickPrice = null; // Could be reused by next tick
                lastTickSize = null;
                break;
            // Refer https://interactivebrokers.github.io/tws-api/market_data_type.html
            // If live data is available a request for delayed data would be ignored by TWS
            // DELAYED types
            case DELAYED_LAST_TIMESTAMP:
                lastTickTime = Long.parseLong(value + "000");
                lastTickSize = null;
                break;
            default:
                info(_contract.shownName() + " tickString() tickType " + tickType + " VALUE: " + value);
                break;
        }

    }

    @java.lang.Override
    public void tickSnapshotEnd() {
        // This function suddenly does not work. 20200514
        tickDataInited = true;
        broadcastTop(true);
    }

    @java.lang.Override
    public void marketDataType(int marketDataType) {
        // https://interactivebrokers.github.io/tws-api/market_data_type.html
        // Switch to live (1) frozen (2) delayed (3) or delayed frozen (4)
        if (marketDataType == 1)
            info(_contract.shownName() + " marketDataType() " + marketDataType);
        else
            warn(_contract.shownName() + " marketDataType() " + marketDataType);
    }

    @java.lang.Override
    public void tickReqParams(int tickerId, double minTick, String bboExchange, int snapshotPermissions) {
        info(_contract.shownName() + " tickReqParams() tickerId " + tickerId + " minTick " + minTick + " bboExchange " + bboExchange + " snapshotPermissions " + snapshotPermissions);
    }

    @java.lang.Override
    public void tickOptionComputation(TickType tickType, double impliedVol, double delta, double optPrice, double pvDividend, double gamma, double vega, double theta, double undPrice) {
        if (_debug)
            info(_contract.shownName() + " tickOptionComputation() tickType " + tickType +
                    " ImpliedVol: " + impliedVol + " Delta: "+ delta + " OptPrice: " + optPrice +
                    " PvDividend: " + pvDividend + " Gamma: "+ gamma + " Vega: " + vega +
                    " Theta: " + theta + " UndPrice: "+ undPrice);
        switch (tickType) {
            case BID_OPTION:
                break;
            case ASK_OPTION:
                break;
            case LAST_OPTION:
                break;
            case MODEL_OPTION:
                break;
            case DELAYED_BID_OPTION:
                break;
            case DELAYED_ASK_OPTION:
                break;
            case DELAYED_LAST_OPTION:
                break;
            case DELAYED_MODEL_OPTION:
                break;
            default:
                info(_contract.shownName() + " tickOptionComputation() tickType " + tickType +
                        " ImpliedVol: " + impliedVol + " Delta: "+ delta + " OptPrice: " + optPrice +
                        " PvDividend: " + pvDividend + " Gamma: "+ gamma + " Vega: " + vega +
                        " Theta: " + theta + " UndPrice: "+ undPrice);
                break;
        }
    }
    private void broadcastTop(boolean verbose) {
        if (broadcastTopLambda != null) {
            Redis.exec(broadcastTopLambda);
            if (verbose)
                log(">>> broadcast top " + publishODBKChannel);
        }
    }

    private void recordLastTrade() {
        if (tickDataInited == false) return;
        if (lastTickSize == 0) return;
        if (lastTickPrice == null || lastTickPrice <= 0 || lastTickSize == null || lastTickSize < 0) {
            err(_contract.shownName() + " Call recordLastTrade() with incompleted data " + _contract.shownName() + " lastTickPrice "
                    + lastTickPrice + " lastTickSize " + lastTickSize);
            return;
        }
        lastTrade = new JSONObject();
        // Guess last trade side by price difference.
        if (bidPrice != null && askPrice != null) {
            if (Math.abs(bidPrice-lastTickPrice) < Math.abs(askPrice-lastTickPrice))
                lastTrade.put("T", "SELL");
            else
                lastTrade.put("T", "BUY");
        } else
            lastTrade.put("T", "BUY");
        lastTrade.put("p", lastTickPrice);
        lastTrade.put("s", lastTickSize);
        lastTrade.put("t", lastTickTime);
        newTicks.set(0, lastTrade);
        newTicksData.set(1, System.currentTimeMillis());
        if (broadcastTickLambda != null)
            Redis.exec(broadcastTickLambda);
    }
}