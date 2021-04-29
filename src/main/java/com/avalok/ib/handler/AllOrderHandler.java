package com.avalok.ib.handler;

import static com.bitex.util.DebugUtil.*;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.*;

import com.avalok.ib.IBContract;
import com.avalok.ib.IBOrder;
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
class OrderCache {
	private Map<Integer, IBOrder> _orderByPermId = new ConcurrentHashMap<>();
	private Map<Integer, IBOrder> _orderById = new ConcurrentHashMap<>();
	public OrderCache() {}
	public OrderCache(IBOrder[] list) { recOrders(list); }
	public void recOrders(IBOrder[] list) {
		for (IBOrder o : list) recOrder(o);
	}
	public void recOrder(IBOrder o) {
		_orderByPermId.put(o.permId(), o);
		_orderById.put(o.permId(), o);
	}
	public IBOrder byId(int id) { return _orderById.get(id); }
	public IBOrder byPermId(int permId) { return _orderByPermId.get(permId); }
}

public class AllOrderHandler implements ILiveOrderHandler,ICompletedOrdersHandler,ITradeReportHandler {
	
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
		} else {
			err("orderStatus() Unexpected orderId " + orderId + ", not " + _processingOrderId);
		}
		_aliveOrders.recOrder(_processingOrder);
		_recvOpenOrders.add(_processingOrder);
	}
	
	@Override
	public void handle(int orderId, int errorCode, String errorMsg) {
		if (orderId == -1 || orderId == 0){
			// id:-1, code:2158, msg:Sec-def data farm connection is OK:secdefhk
			// id:-1, code:2106, msg:HMDS data farm connection is OK:hkhmds
			// Believe BaseIBController could handle that.
			log("<-- handle omit " + orderId + "," + errorCode + "," + errorMsg);
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
				break;
			case 202: // code:202, msg:Order Canceled - reason:
				o.setCancelled(errorMsg);
				break;
			default:
				log(o);
				err("<-- handle unknown code for [" + orderId + "]:" + errorCode + "," + errorMsg);
				return;
		}
		// o includes errorMsg
		info("<-- order msg for [" + orderId + "]," + errorCode + "\n" + o);
		// info("<-- order msg for [" + orderId + "]," + errorCode + "," + errorMsg);
	}

	@Override
	public void openOrderEnd() {
		log("<-- openOrder END");
		_processingOrderId = null;
		_processingOrder = null;
		_aliveOrders = new OrderCache(_recvOpenOrders.toArray(new IBOrder[0]));
		_recvOpenOrders.clear();
	}
}
