package com.avalok.ib.handler;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

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
	protected IBContract _contract;
	protected boolean dataInited = false; // Wait until all ASK/BID filled
	protected int _ct = 0;
	public final static int MAX_DEPTH = 10;
	protected List<JSONObject> asks = new ArrayList<>();
	protected List<JSONObject> bids = new ArrayList<>();
	protected final double multiplier;
	protected final JSONArray odbkSnapshot = new JSONArray();
	protected final String publishChannel; // Publish odbk to universal system

	private Consumer<Jedis> broadcastLambda;
	public DeepMktDataHandler(IBContract contract, boolean broadcast) {
		_contract = contract;
		publishChannel = "URANUS:"+contract.exchange()+":"+contract.pair()+":full_odbk_channel";
		if (contract.multiplier() == null)
			multiplier = 1;
		else
			multiplier = Double.parseDouble(contract.multiplier());
		// Pre-build snapshot
		odbkSnapshot.add(bids);
		odbkSnapshot.add(asks);
		odbkSnapshot.add(0); // Timestamp
		// Pre-build broadcast lambda.
		if (broadcast) {
			broadcastLambda = new Consumer<Jedis> () {
				@Override
				public void accept(Jedis t) {
					t.publish(publishChannel, JSON.toJSONString(odbkSnapshot));
				}
			};
		}
	}
	
	public IBContract contract() { return _contract; }
	
	public void clearData() {
		bids.clear();
		asks.clear();
		odbkSnapshot.set(2, 0);
	}

	@Override
	public void updateMktDepth(int pos, String mm, DeepType operation, DeepSide side, double price, int size_in_lot) {
		if (pos >= MAX_DEPTH) return;
//		log("DeepType " + pos + " " + side + " " + operation + " " + price + " " + size);
		if (_ct == 0)
			log(_contract.shownName() + " first depth data, broadcast to " + (broadcastLambda == null ? "No" : publishChannel));
		_ct += 1;
		
		double size = size_in_lot * multiplier;
		JSONObject o = null;
		if (operation == DeepType.INSERT) {
			dataInited = false;
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
			dataInited = true;
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
			dataInited = true;
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
//		if (pos < 3 && dataInited) {
//			log(_contract.shownName() + "------------------------------------------" + bids.size());
//			for (int i = 0; i < 3; i++)
//				if (asks.size() > i && bids.size() > i)
//					log(_contract.shownName() + "[" + i + "]" + bids.get(i).getIntValue("s") + " "
//							+ bids.get(i).getDoubleValue("p") + " --- " + asks.get(i).getDoubleValue("p") + " "
//							+ asks.get(i).getIntValue("s"));
//		}
		if (dataInited && broadcastLambda != null) {
			odbkSnapshot.set(2, System.currentTimeMillis());
			Redis.exec(broadcastLambda);
		}
	}
}

