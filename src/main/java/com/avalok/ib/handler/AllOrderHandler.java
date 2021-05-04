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
import com.avalok.ib.controller.BaseIBController;
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

public class AllOrderHandler implements ILiveOrderHandler,ICompletedOrdersHandler,ITradeReportHandler {
	private final BaseIBController _ibController;
	private final String _twsName;
	public AllOrderHandler(BaseIBController ibController) {
		_ibController = ibController;
		_twsName = ibController.name();
	}
	
	public void writeOMS(IBOrder o) {
		Redis.exec(new Consumer<Jedis>() {
			@Override
			public void accept(Jedis r) { writeOMS(r, o); }
		});
	}
	
	/**
	 * Write to hset "URANUS:"+ibc.exchange()+":"+_twsName+":O:"+ibc.pair()
	 * Also publish at channel "URANUS:"+ibc.exchange()+":"+_twsName+":O_channel"
	 */
	private void writeOMS(Jedis t, IBOrder o) {
		JSONObject j = o.toOMSJSON();
		String jstr = JSON.toJSONString(j);
		IBContract ibc = o.contract;
		String hmap = "URANUS:"+ibc.exchange()+":"+_twsName+":O:"+ibc.pair();
		log(">>> OMS " + hmap + " / " + o.omsId());
		t.hset(hmap, o.omsId(), jstr);
		JSONObject pubJ = new JSONObject();
		pubJ.put(""+o.omsId(), jstr);
		if (o.omsClientOID() != null) {
			log(">>> OMS " + hmap + " / " + o.omsClientOID());
			t.hset(hmap, o.omsClientOID(), jstr);
			pubJ.put(""+o.omsClientOID(), jstr);
		}
		t.publish("URANUS:"+ibc.exchange()+":"+_twsName+":O_channel", JSON.toJSONString(pubJ));
	}

	public void teardownOMS(String reason) {
		err("Tear down OMS, reason " + reason);
		sleep (300); // TODO
		err("Tear down OMS finished");
	}
	
	////////////////////////////////////////////////////////////////
	// ITradeReportHandler
	////////////////////////////////////////////////////////////////
	public void tradeReport(String tradeKey, Contract contract, Execution execution) {
		IBContract ibc = new IBContract(contract);
		log("<-- " + tradeKey + " " + ibc.shownName() + execution.cumQty() + "@" + execution.price());
	}
	public void tradeReportEnd() {
		log("<-- tradeReportEnd");
	}
	public void commissionReport(String tradeKey, CommissionReport commissionReport) {
	}
	
	////////////////////////////////////////////////////////////////
	// ICompletedOrdersHandler
	////////////////////////////////////////////////////////////////
	protected ConcurrentLinkedQueue<IBOrder> _recvCompletedOrder = new ConcurrentLinkedQueue<>();
	protected OrderCache _deadOrders = new OrderCache();
	@Override
	public void completedOrder(Contract contract, Order order, OrderState orderState) {
		IBOrder o = new IBOrder(contract, order, orderState);
		o.setCompleted();
		log("<-- completedOrder:\n" + o);
		_deadOrders.recOrder(o);
		_recvCompletedOrder.add(o);
	}

	@Override
	public void completedOrdersEnd() {
		log("<-- completedOrder END");
		_deadOrders = new OrderCache(_recvCompletedOrder.toArray(new IBOrder[0]));
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
	private OrderCache _aliveOrders = new OrderCache();
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
			_aliveOrders.recOrder(_processingOrder);
			_recvOpenOrders.add(_processingOrder);
			if(_omsInit) writeOMS(_processingOrder); // Update new order after OMS initialised.
		} else {
			err("orderStatus() Unexpected orderId " + orderId + ", not " + _processingOrderId);
		}
	}

	/**
	 * All message would come here first, then sent to _ibController.message()
	 */
	@Override
	public void handle(int orderId, int errorCode, String errorMsg) {
		if (orderId >= -1){
			// 10000007 200,Invalid destination exchange specified
			// id:-1, code:2158, msg:Sec-def data farm connection is OK:secdefhk
			// id:-1, code:2106, msg:HMDS data farm connection is OK:hkhmds
			// Believe BaseIBController could handle that.
			// log("<-- handle omit " + orderId + "," + errorCode + "," + errorMsg);
			log("<-- handle omit " + orderId + "," + errorCode);
			return;
		}
		boolean process = false;
		IBOrder o = null;
		if (_processingOrderId != null && _processingOrderId == orderId) {
			o = _processingOrder;
			process = true;
		} else {
			o = _aliveOrders.byId(orderId);
			if (o != null) process = true;
		}
		if (! process) {
			err("<-- handle no orderId[" + orderId + "] " + errorCode + "," + errorMsg);
			return;
		}
		// mark order status.
		switch(errorCode) {
			case 201: // code:201, msg:Order rejected - reason:
				o.setRejected(errorMsg);
				_deadOrders.recOrder(o);
				if(_omsInit) writeOMS(o);
				break;
			case 202: // code:202, msg:Order Canceled - reason:
				o.setCancelled(errorMsg);
				_deadOrders.recOrder(o);
				if(_omsInit) writeOMS(o);
				break;
			default:
				log(o);
				err("<-- handle unknown code for [" + orderId + "]:" + errorCode + "," + errorMsg);
				return;
		}
		// o includes errorMsg
		if (errorMsg.length() > 20)
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
		_aliveOrders = new OrderCache(_recvOpenOrders.toArray(new IBOrder[0]));
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
		_aliveOrders = new OrderCache();
		_recvCompletedOrder.clear();
		_deadOrders = new OrderCache();
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
		final Collection<IBOrder> orders1 = _deadOrders.orders();
		final Collection<IBOrder> orders2 = _aliveOrders.orders();
		Redis.exec(new Consumer<Jedis>() {
			@Override
			public void accept(Jedis r) {
				int ct = 1;
				for(IBOrder o : orders1) {
					writeOMS(r, o);
					ct += 1;
				}
				info("OMS init with " + ct + " dead orders");
				ct = 0;
				for(IBOrder o : orders2) {
					writeOMS(r, o);
					ct += 1;
				}
				info("OMS init with " + ct + " alive orders");
			}
		});
	}
}

class OrderCache {
	private Map<Integer, IBOrder> _orderByPermId = new ConcurrentHashMap<>();
	private Map<Integer, IBOrder> _orderById = new ConcurrentHashMap<>();
	OrderCache() {}
	OrderCache(IBOrder[] list) { recOrders(list); }
	void recOrders(IBOrder[] list) {
		for (IBOrder o : list) recOrder(o);
	}
	void recOrder(IBOrder o) {
		_orderByPermId.put(o.permId(), o);
		_orderById.put(o.permId(), o);
	}
	IBOrder byId(int id) { return _orderById.get(id); }
	IBOrder byPermId(int permId) { return _orderByPermId.get(permId); }
	Collection<IBOrder> orders() { return _orderByPermId.values(); }
}
