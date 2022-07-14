package com.avalok.ib.handler;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.concurrent.ConcurrentHashMap;

import com.ib.controller.ApiController.IDeepMktDataHandler;

import redis.clients.jedis.Jedis;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.avalok.ib.IBContract;
import com.bitex.util.Redis;
import com.ib.client.Types.*;

import static com.bitex.util.DebugUtil.*;

public class DeepMktDataHandler implements IDeepMktDataHandler {
	// Record last channel work timestamp.
	public final static ConcurrentHashMap<String, Long> CHANNEL_TIME = new ConcurrentHashMap<>();
	public final int max_depth = 30;
	protected IBContract _contract;
	protected final double multiplier;
	protected final double marketDataSizeMultiplier;
	protected final String publishODBKChannel; // Publish odbk to universal system
	
	protected boolean depthInited = false; // Wait until all ASK/BID filled
	protected int _ct = 0;
	protected List<JSONObject> asks = new ArrayList<>();
	protected List<JSONObject> bids = new ArrayList<>();
	protected final JSONArray odbkSnapshot = new JSONArray();

	private Consumer<Jedis> broadcastLambda;
	public DeepMktDataHandler(IBContract contract, boolean broadcast) {
		_contract = contract;
		publishODBKChannel = "URANUS:"+contract.exchange()+":"+contract.pair()+":full_odbk_channel";
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
		odbkSnapshot.add(bids);
		odbkSnapshot.add(asks);
		odbkSnapshot.add(0); // Timestamp
		// Pre-build broadcast lambda.
		if (broadcast) {
			broadcastLambda = new Consumer<Jedis> () {
				@Override
				public void accept(Jedis t) {
					CHANNEL_TIME.put(publishODBKChannel, System.currentTimeMillis());
					t.publish(publishODBKChannel, JSON.toJSONString(odbkSnapshot));
				}
			};
		}
	}
	
	public IBContract contract() { return _contract; }

	private int fixSizeX10Bug(int size){
		// From ib received "IBOND" depth size is wrong
		// received depth size is 10 times larger than display on tws
		// Seems from 'self account' place order doesn't x10

		// self order size = 20
		// other order size = 90
		// API received (depth size x10 bug): size = 910 (90 * 10 + 20)
		// Expect received: size = 1100 (90 * 10 + 20 * 10)

		// Cannot detect:
		// self order size = 100
		// other order size = 90
		// API received size (depth size x10 bug): 1000 (90 * 10 + 100)
		// Expect received size: 1900 (90 * 10 + 100 * 10)

		if ( _contract.tradingClass().equals("IBOND")){
			warn("Fixing IBOND depth size x10 bug, always size = " + size + " / 10");
			size = size / 10;
		}

		return size;
	}

	@Override
	public void updateMktDepth(int pos, String mm, DeepType operation, DeepSide side, double price, int size) {
		if (pos >= max_depth) return;
//		log("DeepType " + pos + " " + side + " " + operation + " " + price + " " + size);
		if (_ct == 0)
			log(">>> broadcast depth " + publishODBKChannel);
		_ct += 1;
		//		double size = size_in_lot * multiplier * marketDataSizeMultiplier;
		size = fixSizeX10Bug(size);
		JSONObject o = null;
		if (operation == DeepType.INSERT) {
			depthInited = false;
			o = new JSONObject();
			o.put("p", price);
			o.put("s", size);
			if (side == DeepSide.BUY) {
				if (bids.size() >= pos)
					bids.add(pos, o);
				else
					return;
			} else {
				if (asks.size() >= pos)
					asks.add(pos, o);
				else
					return;
			}
		} else if (operation == DeepType.UPDATE) {
			// Reuse o from asks/bids
			depthInited = true;
			if (side == DeepSide.BUY) {
				if (bids.size() > pos)
					o = bids.get(pos);
				else
					return;
			} else {
				if (asks.size() > pos)
					o = asks.get(pos);
				else
					return;
			}
			o.put("p", price);
			o.put("s", size);
		} else if (operation == DeepType.DELETE) {
			depthInited = true;
			if (side == DeepSide.BUY) {
				if (bids.size() > pos)
					bids.remove(pos);
			} else {
				if (asks.size() > pos)
					asks.remove(pos);
			}
		} else {
			log("Error DeepType " + pos + " " + side + " " + operation + " " + price + " " + size);
			return;
		}
		// Debug
//		if (pos < 3 && depthInited) {
//			log(_contract.shownName() + "------------------------------------------" + bids.size());
//			for (int i = 0; i < 3; i++)
//				if (asks.size() > i && bids.size() > i)
//					log(_contract.shownName() + "[" + i + "]" + bids.get(i).getIntValue("s") + " "
//							+ bids.get(i).getDoubleValue("p") + " --- " + asks.get(i).getDoubleValue("p") + " "
//							+ asks.get(i).getIntValue("s"));
//		}
		if (depthInited && broadcastLambda != null) {
			odbkSnapshot.set(2, System.currentTimeMillis());
			Redis.exec(broadcastLambda);
		}
	}
}

