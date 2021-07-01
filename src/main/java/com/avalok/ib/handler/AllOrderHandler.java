package com.avalok.ib.handler;

import static com.bitex.util.DebugUtil.*;

import redis.clients.jedis.Jedis;
import com.bitex.util.Redis;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.*;
import java.util.function.Consumer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.avalok.ib.IBContract;
import com.avalok.ib.IBOrder;
import com.avalok.ib.GatewayController;
import com.ib.client.CommissionReport;
import com.ib.client.Contract;
import com.ib.client.Execution;
import com.ib.client.Order;
import com.ib.client.OrderState;
import com.ib.client.OrderStatus;
import com.ib.controller.ApiController.ICompletedOrdersHandler;
import com.ib.controller.ApiController.ILiveOrderHandler;
import com.ib.controller.ApiController.ITradeReportHandler;

/**
 * Start-up:
 * Refresh all alive orders (complete snapshot), and history orders (may not contain those with 0 trade)
 * Setup hashmap at:
 * 		Redis/URANUS:{exchange}:{account}:O:{currency}-{symbol}
 * 		Redis/URANUS:{exchange}:{account}:O:{currency}-{symbol}@{expiry}
 * 		Redis/URANUS:{exchange}:{account}:O:{currency}-{symbol}@{expiry}@{multiplier}
 * Redis hashmap internal structure:
 * 		{ t -> timestamp }
 * 		{ id -> order_json }
 * 		{ client_oid -> order_json }
 * Then, mark OMS cache running with value '1' at:
 * 		Redis/URANUS:{exchange}:{account}:OMS
 * 
 * Work:
 * Keep receiving updates from AllOrderHandler
 * Broadcast order_json string at channel:
 * 		Redis/URANUS:{exchange}:{account}:O_channel
 * 
 * Tear-down:
 * mark OMS cache stopped by deleting key 't' in every hashmap Redis/URANUS:{exchange}:{account}:O:*
 */

public class AllOrderHandler implements ILiveOrderHandler,ICompletedOrdersHandler,ITradeReportHandler {
	private final GatewayController _ibController;
	public final String _twsName;
	protected OrderCache _allOrders = new OrderCache();
	protected static ConcurrentHashMap<String, String> KNOWN_EXCHANGES = new ConcurrentHashMap<>();
	public AllOrderHandler(GatewayController ibController) {
		_ibController = ibController;
		_twsName = ibController.name();
	}
	
	////////////////////////////////////////////////////////////////
	// Cache utilities
	////////////////////////////////////////////////////////////////
	
	public IBOrder orderByOMSId(String id) {
		return _allOrders.byOMSId(id);
	}
	
	public void writeToCacheAndOMS(IBOrder o) {
		String ex = o.contract.exchange();
		if (KNOWN_EXCHANGES.containsKey(ex) == false) { // New exchange order received, mark its OMS status.
			KNOWN_EXCHANGES.put(ex, ex);
			for (String acc : _ibController.accountList()) {
				String k = "URANUS:"+ex+":"+acc+":OMS";
				info("Mark OMS started " + k);
				Redis.del(k);
			}
		}
		_allOrders.recOrder(o);
		if (!_omsInit) return; // Don't hurry to write until _omsInit
		Redis.exec(new Consumer<Jedis>() {
			@Override
			public void accept(Jedis r) { writeOMS(r, o); }
		});
	}
	
	/**
	 * Write to hset "URANUS:"+ibc.exchange()+":"+o.account()+":O:"+ibc.pair()
	 * Also publish at channel "URANUS:"+ibc.exchange()+":"+o.account()+":O_channel"
	 */
	private void writeOMS(Jedis t, IBOrder o) {
		// This SMART exchange would make a wrong OMS hashmap name
		// Make real OMS hashmap data missing.
		IBContract ibc = o.contract;
		if (ibc.exchange().equals("SMART")) {
			IBOrder real_o = o.cloneWithRealExchange();
			if (real_o != null) {
				o = real_o;
				ibc = o.contract;
			}
		}

		String timeStr = "" + System.currentTimeMillis();
		JSONObject j = o.toOMSJSON();
		String jstr = JSON.toJSONString(j);
		JSONObject pubJ = new JSONObject();
		String hmap = "URANUS:"+ibc.exchange()+":"+o.account()+":O:"+ibc.pair();
		String pubChannel = "URANUS:"+ibc.exchange()+":"+o.account()+":O_channel";
		String hmapShort = "URANUS:"+ibc.exchange()+":"+o.account()+":O:";
		t.hdel(hmap, "0"); // Clear historical remained trash, could delete this after stable version released.
		if (o.omsClientOID() != null) {
			log(">>> OMS " + hmapShort + " / " + o.omsClientOID() + "\n" + o);
			t.hset(hmap, o.omsClientOID(), jstr);
			pubJ.put(o.omsClientOID(), jstr);
		} else if (o.omsAltId() != null) {
			log(">>> OMS " + hmapShort + " / " + o.omsAltId() + "\n" + o);
			t.hset(hmap, o.omsAltId(), jstr);
			pubJ.put(o.omsAltId(), jstr);
		}
		t.hset(hmap, "t", timeStr); // Mark latest updated timestamp.
		t.publish(pubChannel, JSON.toJSONString(pubJ));
	}

	public void teardownOMS(String reason) {
		err("Tear down OMS, reason " + reason);
		String[] exchanges = KNOWN_EXCHANGES.values().toArray(new String[0]);
		for (String acc : _ibController.accountList()) {
			for (String ex: exchanges) {
				String k = "URANUS:"+ex+":"+acc+":OMS";
				info("Mark OMS stopped " + k);
				Redis.del(k);
			}
		}
		err("Tear down OMS finished");
	}
	
	////////////////////////////////////////////////////////////////
	// ITradeReportHandler
	////////////////////////////////////////////////////////////////
	public void tradeReport(String tradeKey, Contract contract, Execution execution) {
		IBContract ibc = new IBContract(contract);
		log("<-- tradeReport() " + tradeKey + " " + ibc.shownName() + execution.cumQty() + "@" + execution.price());
	}
	public void tradeReportEnd() {
		log("<-- tradeReportEnd");
	}
	public void commissionReport(String tradeKey, CommissionReport commissionReport) { }
	
	////////////////////////////////////////////////////////////////
	// ICompletedOrdersHandler
	////////////////////////////////////////////////////////////////
	protected ConcurrentLinkedQueue<IBOrder> _recvCompletedOrder = new ConcurrentLinkedQueue<>();
//	protected OrderCache _deadOrders = new OrderCache();
	@Override
	public void completedOrder(Contract contract, Order order, OrderState orderState) {
		IBOrder o = new IBOrder(contract, order, orderState);
		o.setCompleted();
		log("<-- completedOrder:\n" + o);
		writeToCacheAndOMS(o);
		_recvCompletedOrder.add(o);
	}

	@Override
	public void completedOrdersEnd() {
		log("<-- completedOrder END");
		_allOrders.recOrders(_recvCompletedOrder.toArray(new IBOrder[0]));
		_recvCompletedOrder.clear();
		_deadOrderInit = true;
		if (!_omsInit && _aliveOrderInit) initOMS();
	}
	
	////////////////////////////////////////////////////////////////
	// ILiveOrderHandler
	// For order data, a openOrder() followed by orderStatus() is most common case.
	// For others, a openOrder(), a orderStatus(), then handle().
	// Keep updating a temporary _processingOrderId in openOrder() for possible lateral orderStatus() and handle()
	////////////////////////////////////////////////////////////////
	private ConcurrentLinkedQueue<IBOrder> _recvOpenOrders = new ConcurrentLinkedQueue<>();
//	private OrderCache _aliveOrders = new OrderCache();
	private Integer _processingOrderId = null;
	private IBOrder _processingOrder = null; // Cross validation
	@Override
	public void openOrder(Contract contract, Order order, OrderState orderState) {
		IBOrder o = new IBOrder(contract, order, orderState);
		// Order info is not full yet, need wait orderStatus() to print.
		// log("<-- openOrder:\n" + o);
		log("<-- openOrder: " + o.permId());
		_processingOrderId = o.orderId();
		_processingOrder = o;
		// Don't record this order now, not enough detail yet.
		// leave job to orderStatus()
	}

	@Override
	public void orderStatus(
			int orderId, OrderStatus status, double filled, 
			double remaining, double avgFillPrice,
			int permId, int parentId, double lastFillPrice, 
			int clientId, String whyHeld, double mktCapPrice) {
		if (_processingOrderId != null &&_processingOrderId == orderId) {
			// log in setStatus() already.
			_processingOrder.setStatus(orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice);
			_recvOpenOrders.add(_processingOrder);
			writeToCacheAndOMS(_processingOrder);
		} else {
			// Search from alive_orders
			IBOrder o = _allOrders.byId(orderId);
			if (o == null) {
				err("orderStatus() Unexpected orderId " + orderId + ", not " + _processingOrderId);
				return;
			}
			o.setStatus(orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice);
			writeToCacheAndOMS(o);
		}
	}

	/**
	 * All message would come here first, then sent to _ibController.message()
	 */
	@Override
	public void handle(int orderId, int errorCode, String errorMsg) {
		if (orderId >= 10000000 ) // the orderId means API reqId
			return;
		boolean process = false;
		IBOrder o = null;
		if (_processingOrderId != null && _processingOrderId == orderId) {
			o = _processingOrder;
			process = true;
		} else {
			o = _allOrders.byId(orderId);
			if (o != null) process = true;
		}
		if (! process) {
			if (orderId != -1) // Very common server message ID
				err("<-- can't find order [" + orderId + "]:" + errorCode + "," + errorMsg);
			return;
		}
		JSONObject j = new JSONObject();
		j.put("type", "order_error");
		j.put("orderId", orderId);
		j.put("permId", o.permId());
		j.put("client_oid", o.omsClientOID());
		j.put("code", errorCode);
		j.put("msg", errorMsg);
		boolean printMsg = true;
		// mark order status.
		switch(errorCode) {
		case 161: // code:161, msg:Cancel attempted when order is not in a cancellable state.  Order permId =1338982574
			log(o);
			err("<-- Not cancellable " + errorMsg);
		case 201: // code:201, msg:Order rejected - reason:
			o.setRejected(errorMsg);
			writeToCacheAndOMS(o);
			break;
		case 202: // code:202, msg:Order Canceled - reason:
			o.setCancelled(errorMsg);
			writeToCacheAndOMS(o);
			break;
		case 399: // Order error: check them by message
			if (errorMsg.contains("Warning: your order will not be placed at the exchange until "))
				break; // This is okay.
		case 10147: // OrderId 51 that needs to be cancelled is not found.
			o.setCancelled(errorMsg);
			writeToCacheAndOMS(o);
			break;
		case 10148: // OrderId 51 that needs to be cancelled cannot be cancelled, state: Cancelled.
			o.setCancelled(errorMsg);
			writeToCacheAndOMS(o);
			break;
		case 10198: // Order bound is rejected: No such order
			o.setCancelled(errorMsg);
			writeToCacheAndOMS(o);
			break;
		default:
			log(o);
			// Should be some kind of error, let clients know.
			err("<-- broadcast error for order [" + o.omsClientOID() + "]\norder id [" + orderId + "]:" + errorCode + "," + errorMsg);
			_ibController.ack(j);
			printMsg = false;
			break;
		}
		// o includes errorMsg
		if (printMsg == false)
			;
		else if (errorMsg.length() > 20)
			info("<-- order msg for [" + orderId + "]," + errorCode + " " + errorMsg.substring(0, 19) + "...\n" + o);
		else
			info("<-- order msg for [" + orderId + "]," + errorCode + " " + errorMsg + "...\n" + o);
		// info("<-- order msg for [" + orderId + "]," + errorCode + "," + errorMsg);
		// Tell _ibController this message is processed.
		_ibController.lastAckErrorID = orderId;
		_ibController.lastAckErrorCode = errorCode;
		_ibController.lastAckErrorMsg = errorMsg;
	}

	@Override
	public void openOrderEnd() {
		log("<-- openOrder END");
		_processingOrderId = null;
		_processingOrder = null;
		_allOrders.recOrders(_recvOpenOrders.toArray(new IBOrder[0]));
		_recvOpenOrders.clear();
		_aliveOrderInit = true;
		if (!_omsInit && _deadOrderInit) initOMS();
	}

	////////////////////////////////////////////////////////////////
	// initOMS, normally do this when TWS is connected or reconnected
	////////////////////////////////////////////////////////////////
	public void resetStatus() {
		info("Reset AllOrderHandler status");
		_omsInit = false;
		_aliveOrderInit = false;
		_deadOrderInit = false;
		_recvOpenOrders.clear();
//		_aliveOrders = new OrderCache();
		_recvCompletedOrder.clear();
//		_deadOrders = new OrderCache();
		_allOrders = new OrderCache();
	}
	/**
	 * Execute after _aliveOrderInit and _deadOrderInit becomes true.
	 * _deadOrders and _aliveOrders only represents snapshot after receiving updates.
	 * New placed/rejected order after snapshot might not be contained.
	 */
	private boolean _aliveOrderInit = false;
	private boolean _deadOrderInit = false;
	private boolean _omsInit = false;
	private void initOMS() {
		if (_omsInit) {
			err("Should not call initOMS() when _omsInit is true");
			return;
		}
		if (!_aliveOrderInit || !_deadOrderInit) {
			err("Should not call initOMS() when _aliveOrderInit " + _aliveOrderInit + " _deadOrderInit " + _deadOrderInit);
			return;
		}
		_omsInit = true;
		info("Init OMS now");
		final Collection<IBOrder> orders = _allOrders.orders();
		Redis.exec(new Consumer<Jedis>() {
			@Override
			public void accept(Jedis r) {
				int ct = 1;
				for(IBOrder o : orders) {
					writeOMS(r, o);
					KNOWN_EXCHANGES.put(o.contract.exchange(), o.contract.exchange());
					ct += 1;
				}
				info("OMS init with " + ct + "  orders");
				String[] exchanges = KNOWN_EXCHANGES.values().toArray(new String[0]);
				for (String acc : _ibController.accountList()) {
					for (String ex: exchanges) {
						String k = "URANUS:"+ex+":"+acc+":OMS";
						info("Mark OMS running " + k);
						Redis.set(k, "1");
					}
				}
			}
		});
	}
}

class OrderCache {
	private Map<String, IBOrder> _orderByOMSId = new ConcurrentHashMap<>();
	private Map<Integer, IBOrder> _orderById = new ConcurrentHashMap<>();
	OrderCache() {}
	void recOrders(IBOrder[] list) {
		for (IBOrder o : list) recOrder(o);
	}
	void recOrder(IBOrder o) {
		String omsId = o.omsId();
		if (omsId != null) _orderByOMSId.put(o.omsId(), o);
		_orderById.put(o.orderId(), o);
		// errWithTrace("recOrder " + o.permId() + " - " + o.orderId());
	}
	IBOrder byId(int id) { return _orderById.get(id); }
	IBOrder byOMSId(String id) { return _orderByOMSId.get(id); }
	Collection<IBOrder> orders() { return _orderByOMSId.values(); }
}
