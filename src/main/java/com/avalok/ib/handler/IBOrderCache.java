package com.avalok.ib.handler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.fastjson.JSONObject;
import com.avalok.ib.IBOrder;

/**
 * Start-up:
 * Refresh all alive orders (complete snapshot), and history orders (may not contain those with 0 trade)
 * Setup hashmap at:
 * 		Redis/URANUS:{exchange}:{name}:O:{currency}-{symbol}
 * 		Redis/URANUS:{exchange}:{name}:O:{currency}-{symbol}@{expiry}
 * 		Redis/URANUS:{exchange}:{name}:O:{currency}-{symbol}@{expiry}@{multiplier}
 * Redis hashmap internal structure:
 * 		{ t -> timestamp }
 * 		{ id -> order_json }
 * 		{ client_oid -> order_json }
 * Then, mark OMS cache running with value '1' at:
 * 		Redis/URANUS:{exchange}:{name}:OMS
 * 
 * Work:
 * Keep receiving updates from AllOrderHandler
 * Broadcast order_json string at channel:
 * 		Redis/URANUS:{exchange}:{name}:O_channel
 * 
 * Tear-down:
 * mark OMS cache stopped by deleting every Redis/URANUS:{exchange}:{name}:OMS
 */
public class IBOrderCache {
	private static Map<Integer, IBOrder> ORDER_BY_PERMID = new ConcurrentHashMap<>();
	protected static boolean DATA_READY = false;
	public static void reset() {
		ORDER_BY_PERMID.clear();
		DATA_READY = false;
	}
	
	public static void recOrder(IBOrder o) {
		ORDER_BY_PERMID.put(o.permId(), o);
	}

	public static void main(String[] args) {
	}
}
