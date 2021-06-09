package com.avalok.ib.handler;

import java.util.function.Consumer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.avalok.ib.IBContract;
import com.bitex.util.Redis;
import com.ib.client.TickAttrib;
import com.ib.client.TickType;
import com.ib.controller.ApiController.ITopMktDataHandler;

import redis.clients.jedis.Jedis;

import static com.bitex.util.DebugUtil.*;

/**
 * Reuse the broadcast and internal cache structure from DeepMktDataHandler
 */
public class TopMktDataHandler implements ITopMktDataHandler{
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
	public TopMktDataHandler(IBContract contract, boolean broadcastTop, boolean broadcastTick) {
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
	@Override
	public void tickPrice(TickType tickType, double price, TickAttrib attribs) {
		if (_debug)
			info(_contract.shownName() + " tickPrice() tickType " + tickType + " price " + price + " attribs " + attribs);
		switch (tickType) {
		case BID:
			bidPrice = price;
			topBids[0].put("p", price);
			if (tickDataInited) broadcastTop(false);
			break;
		case ASK:
			askPrice = price;
			topAsks[0].put("p", price);
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
		default:
			info(_contract.shownName() + " tickPrice() tickType " + tickType + " price " + price + " attribs " + attribs);
			break;
		}
	}

	@Override
	public void tickSize(TickType tickType, int size_in_lot) {
		double size = size_in_lot * multiplier * marketDataSizeMultiplier;
		if (_debug)
			info(_contract.shownName() + " tickSize() tickType " + tickType + " size " + size);
		switch (tickType) {
		case BID_SIZE:
			topBids[0].put("s", size);
			if (tickDataInited) broadcastTop(false);
			break;
		case ASK_SIZE:
			topAsks[0].put("s", size);
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
		default:
			info(_contract.shownName() + " tickSize() tickType " + tickType + " size " + size);
			break;
		}
	}

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

	@Override
	public void tickString(TickType tickType, String value) {
		if (_debug)
			info(_contract.shownName() + " tickString() tickType " + tickType + " VALUE: " + value);
		switch (tickType) {
		case LAST_TIMESTAMP:
			lastTickTime = Long.parseLong(value + "000");
			// lastTickPrice = null; // Could be reused by next tick
			lastTickSize = null;
			break;
		default:
			info(_contract.shownName() + " tickString() tickType " + tickType + " VALUE: " + value);
			break;
		}
	}

	@Override
	public void tickSnapshotEnd() {
		// This function suddenly does not work. 20200514
		tickDataInited = true;
		broadcastTop(true);
	}

	private void broadcastTop(boolean verbose) {
		if (broadcastTopLambda != null) {
			Redis.exec(broadcastTopLambda);
			if (verbose)
				log(">>> broadcast top " + publishODBKChannel);
		}
	}

	@Override
	public void marketDataType(int marketDataType) {
		// https://interactivebrokers.github.io/tws-api/market_data_type.html
		/*** Switch to live (1) frozen (2) delayed (3) or delayed frozen (4) ***/
		if (marketDataType == 1)
			info(_contract.shownName() + " marketDataType() " + marketDataType);
		else
			err(_contract.shownName() + " marketDataType() " + marketDataType);
	}

	@Override
	public void tickReqParams(int tickerId, double minTick, String bboExchange, int snapshotPermissions) {
		info(_contract.shownName() + " tickReqParams() tickerId " + tickerId + " minTick " + minTick + " bboExchange " + bboExchange + " snapshotPermissions " + snapshotPermissions);
	}

}
