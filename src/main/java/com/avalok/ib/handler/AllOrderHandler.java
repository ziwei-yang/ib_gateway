package com.avalok.ib.handler;

import static com.bitex.util.DebugUtil.*;

import java.util.concurrent.ConcurrentLinkedQueue;

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
	protected ConcurrentLinkedQueue<IBOrder> _recvCompletedOrder = new ConcurrentLinkedQueue<IBOrder>();
	@Override
	public void completedOrder(Contract contract, Order order, OrderState orderState) {
		IBOrder o = new IBOrder(contract, order, orderState);
		log("<-- completedOrder:\n" + o);
		_recvCompletedOrder.add(o);
	}

	@Override
	public void completedOrdersEnd() {
		log("<-- completedOrder END");
		_recvCompletedOrder = new ConcurrentLinkedQueue<IBOrder>();
	}
	
	////////////////////////////////////////////////////////////////
	// ILiveOrderHandler
	// For order data, a openOrder() followed by orderStatus() is most common case.
	// For others, a openOrder(), a orderStatus(), then handle().
	// Keep updating a temporary _processingOrderId in openOrder() for possible lateral orderStatus() and handle()
	////////////////////////////////////////////////////////////////
	protected ConcurrentLinkedQueue<IBOrder> _recvOpenOrders = new ConcurrentLinkedQueue<IBOrder>();
	private Integer _processingOrderId = null;
	private IBOrder _processingOrder = null; // Cross validation
	@Override
	public void openOrder(Contract contract, Order order, OrderState orderState) {
		IBOrder o = new IBOrder(contract, order, orderState);
		log("<-- openOrder:\n" + o);
		_processingOrderId = o.orderId();
		_processingOrder = o;
		_recvOpenOrders.add(o);
	}

	@Override
	public void orderStatus(int orderId, OrderStatus status, double filled, double remaining, double avgFillPrice,
			int permId, int parentId, double lastFillPrice, int clientId, String whyHeld, double mktCapPrice) {
		if (_processingOrderId == orderId) {
			// log in setStatus() already.
			_processingOrder.setStatus(orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice);
		} else {
			err("orderStatus() Unexpected orderId " + orderId + ", not " + _processingOrderId);
		}
	}
	
	@Override
	public void handle(int orderId, int errorCode, String errorMsg) {
		if (_processingOrderId != null && _processingOrderId == orderId) {
			// log in setStatus() already.
			log("<-- handle " + orderId + "," + errorCode + "," + errorMsg);
		} else {
			err("<-- handle " + orderId + "," + errorCode + "," + errorMsg);
			err("handle() Unexpected orderId " + orderId + ", not " + _processingOrderId);
		}
	}

	@Override
	public void openOrderEnd() {
		log("<-- openOrder END");
		_processingOrderId = null;
		_processingOrder = null;
		_recvOpenOrders = new ConcurrentLinkedQueue<IBOrder>();
	}
}
