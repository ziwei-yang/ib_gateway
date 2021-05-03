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
	public final int max_depth = 10;
	protected IBContract _contract;
	protected final double multiplier;
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
		// Pre-build snapshot
		odbkSnapshot.add(bids);
		odbkSnapshot.add(asks);
		odbkSnapshot.add(0); // Timestamp
		// Pre-build broadcast lambda.
		if (broadcast) {
			broadcastLambda = new Consumer<Jedis> () {
				@Override
				public void accept(Jedis t) {
					t.publish(publishODBKChannel, JSON.toJSONString(odbkSnapshot));
				}
			};
		}
	}
	
	public IBContract contract() { return _contract; }

	@Override
	public void updateMktDepth(int pos, String mm, DeepType operation, DeepSide side, double price, int size_in_lot) {
		if (pos >= max_depth) return;
//		log("DeepType " + pos + " " + side + " " + operation + " " + price + " " + size);
		if (_ct == 0)
			log(">>> broadcast depth " + publishODBKChannel);
		_ct += 1;
		
		double size = size_in_lot * multiplier;
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

