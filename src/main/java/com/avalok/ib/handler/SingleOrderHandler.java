package com.avalok.ib.handler;

import static com.bitex.util.DebugUtil.err;
import static com.bitex.util.DebugUtil.info;
import static com.bitex.util.DebugUtil.log;

import com.avalok.ib.IBOrder;
import com.avalok.ib.controller.BaseIBController;
import com.ib.client.OrderState;
import com.ib.client.OrderStatus;
import com.ib.controller.ApiController.IOrderHandler;

/**
 * See AllOrderHandler, this SingleOrderHandler for placing/modifying single order only.
 * It does the same thing as AllOrderHandler.
 *
 */
public class SingleOrderHandler implements IOrderHandler {
	
	private IBOrder _order;
	private BaseIBController _ibController;
	private AllOrderHandler _orderCacheHandler;

	public SingleOrderHandler(BaseIBController ibController, AllOrderHandler orderCacheHandler, IBOrder o) {
		_ibController = ibController;
		_order = o;
		_orderCacheHandler = orderCacheHandler;
	}

	////////////////////////////////////////////////////////////////
	// IOrderHandler
	////////////////////////////////////////////////////////////////
	@Override
	public void orderState(OrderState orderState) {
		_order.orderState(orderState);
		log("<-- SingleOrder udpate orderState: " + orderState.status());
	}
	
	@Override
	public void orderStatus(
			OrderStatus status, double filled, 
			double remaining, double avgFillPrice,
			int permId, int parentId, double lastFillPrice, 
			int clientId, String whyHeld, double mktCapPrice) {
		_order.setStatus(status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice);
		_orderCacheHandler.writeToCacheAndOMS(_order);
	}

	@Override
	public void handle(int errorCode, String errorMsg) {
		int orderId = _order.orderId();
		// mark order status.
		switch(errorCode) {
			case 201: // code:201, msg:Order rejected - reason:
				_order.setRejected(errorMsg);
				_orderCacheHandler.writeToCacheAndOMS(_order);
				break;
			case 202: // code:202, msg:Order Canceled - reason:
				_order.setCancelled(errorMsg);
				_orderCacheHandler.writeToCacheAndOMS(_order);
				break;
			default:
				log(_order);
				err("<-- handle unknown code for [" + orderId + "]:" + errorCode + "," + errorMsg);
				return;
		}
		// o includes errorMsg
		if (errorMsg.length() > 20)
			info("<-- order msg for [" + orderId + "]," + errorCode + " " + errorMsg.substring(0, 19) + "...\n" + _order);
		else
			info("<-- order msg for [" + orderId + "]," + errorCode + " " + errorMsg + "...\n" + _order);
		// info("<-- order msg for [" + orderId + "]," + errorCode + "," + errorMsg);
		// Tell _ibController this message is processed.
		_ibController.lastAckErrorID = _order.orderId();
		_ibController.lastAckErrorCode = errorCode;
		_ibController.lastAckErrorMsg = errorMsg;
	}
}
