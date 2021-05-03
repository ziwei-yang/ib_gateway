package com.avalok.ib.handler;

import java.util.ArrayList;
import java.util.List;
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
	public final int max_depth = 1;
	protected IBContract _contract;
	protected final double multiplier;
	protected final String publishODBKChannel; // Publish odbk to universal system
	protected final String publishTickChannel; // Publish odbk to universal system
	
	protected final JSONArray topDataSnapshot = new JSONArray();
	protected final JSONObject[] topAsks = new JSONObject[] {new JSONObject()};
	protected final JSONObject[] topBids = new JSONObject[] {new JSONObject()};
	protected boolean topDataInited = false; // Wait until all ASK/BID filled

	protected final JSONArray newTicksData = new JSONArray();
	protected final JSONArray newTicks = new JSONArray();
	protected boolean tickDataInited = false; // Wait until tickSnapshotEnd()
	
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
					t.publish(publishODBKChannel, JSON.toJSONString(topDataSnapshot));
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
					t.publish(publishTickChannel, JSON.toJSONString(newTicksData));
				}
			};
		}
	}
	
	public IBContract contract() { return _contract; }

	Double bidPrice, askPrice; // To determine last trade side
	@Override
	public void tickPrice(TickType tickType, double price, TickAttrib attribs) {
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
	public void tickSize(TickType tickType, int size) {
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
		case LOW:
			break;
		case HIGH:
			break;
		default:
			info("tickSize() tickType " + tickType + " size " + size);
			break;
		}
	}

	/////////////////////////////////////////////////////
	// Trade tick always comes with triple messages:
	// tickString() tickType LAST_TIMESTAMP VALUE: 1620015539
	// tickPrice() tickType LAST price 58280.0
	// tickSize() tickType LAST_SIZE size 1
	/////////////////////////////////////////////////////
	private Long lastTickTime = null;
	private Double lastTickPrice = null;
	private Integer lastTickSize = null;
	private JSONObject lastTrade;
	private void recordLastTrade() {
		if (tickDataInited == false)
			return;
		if (lastTickTime == null || lastTickPrice == null || lastTickSize == null) {
			err("Should not call recordLastTrade() with not completed data " + _contract.shownName());
			return;
		}
		lastTrade = new JSONObject();
		// Guess last trade side by price difference.
		if (bidPrice != null && askPrice != null) {
			if (Math.abs(bidPrice-lastTickPrice) < Math.abs(askPrice-lastTickPrice))
				lastTrade.put("T", "BUY");
			else
				lastTrade.put("T", "SELL");
		} else
			lastTrade.put("T", "BUY");
		lastTrade.put("p", lastTickPrice);
		lastTrade.put("s", lastTickSize);
		lastTrade.put("t", lastTickTime);
		newTicks.set(0, lastTrade);
		newTicksData.set(1, System.currentTimeMillis());
		if (broadcastTickLambda != null)
			Redis.exec(broadcastTickLambda);
		lastTickTime = null;
	}

	@Override
	public void tickString(TickType tickType, String value) {
		switch (tickType) {
		case LAST_TIMESTAMP:
			lastTickTime = Long.parseLong(value + "000");
			lastTickPrice = null;
			lastTickSize = null;
			break;
		default:
			info("tickString() tickType " + tickType + " VALUE: " + value);
			break;
		}
	}

	@Override
	public void tickSnapshotEnd() {
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
		info("marketDataType() " + marketDataType);
	}

	@Override
	public void tickReqParams(int tickerId, double minTick, String bboExchange, int snapshotPermissions) {
		info("tickReqParams() tickerId " + tickerId + " minTick " + minTick + " bboExchange " + bboExchange + " snapshotPermissions " + snapshotPermissions);
	}

}
