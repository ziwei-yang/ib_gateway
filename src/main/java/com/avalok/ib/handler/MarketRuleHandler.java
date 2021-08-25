package com.avalok.ib.handler;

import static com.bitex.util.DebugUtil.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.avalok.ib.GatewayController;
import com.bitex.util.Redis;
import com.ib.client.PriceIncrement;
import com.ib.controller.ApiController.IMarketRuleHandler;

public class MarketRuleHandler implements IMarketRuleHandler {
	
	private static Map<Integer, JSONArray> RULE_CACHE = new ConcurrentHashMap<>();
	private static final Map<Integer, Long> QUERY_HIS = new ConcurrentHashMap<>();
	public static GatewayController GW_CONTROLLER = null;
	public static final MarketRuleHandler instance = new MarketRuleHandler();
	
	private MarketRuleHandler() {}
	
	public static JSONArray getMarketRule(int marketRuleId) {
		JSONArray rules = RULE_CACHE.get(marketRuleId);
		if (rules != null) return rules;
		Long lastQueryT = QUERY_HIS.get(marketRuleId);
		if (lastQueryT == null || lastQueryT + 10_000 < System.currentTimeMillis()) {
			QUERY_HIS.put(marketRuleId, System.currentTimeMillis());
			warn("--> Auto query market rule id " + marketRuleId);
			GW_CONTROLLER.queryMarketRule(marketRuleId);
		}
		return null;
	}

	@Override
	public void marketRule(int marketRuleId, PriceIncrement[] priceIncrements) {
		JSONArray rules = new JSONArray();
		StringBuilder sb = new StringBuilder();
		for (PriceIncrement pi : priceIncrements) {
			JSONObject j = new JSONObject();
			j.put("low_edge", pi.lowEdge());
			j.put("price_increment", pi.increment());
			sb.append("After " + pi.lowEdge() + " price incr " + pi.increment() + ". ");
			rules.add(j);
		}
		RULE_CACHE.put(marketRuleId, rules);
		writeRules(marketRuleId, rules);
		
		info("market rule id got:" + marketRuleId + " " + sb.toString());
	}

	private void writeRules(int marketRuleId, JSONArray rules) {
		String key = "IBGateway:MarketRule:" + marketRuleId;
		log(">>> Redis " + key);
		Redis.set(key, rules);
	}
}