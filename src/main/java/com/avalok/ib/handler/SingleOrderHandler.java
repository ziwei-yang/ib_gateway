package com.avalok.ib.handler;

import static com.bitex.util.DebugUtil.err;
import static com.bitex.util.DebugUtil.info;
import static com.bitex.util.DebugUtil.log;

import java.util.Timer;
import java.util.TimerTask;

import com.avalok.ib.IBOrder;
import com.avalok.ib.GatewayController;

import com.ib.client.OrderState;
import com.ib.client.OrderStatus;
import com.ib.controller.ApiController.IOrderHandler;

import com.alibaba.fastjson.JSONObject;

/**
 * See AllOrderHandler, this SingleOrderHandler for placing/modifying single order only.
 * It does the same thing as AllOrderHandler.
 *
 */
public class SingleOrderHandler implements IOrderHandler {
	
	private IBOrder _order;
	private GatewayController _ibController;
	private AllOrderHandler _orderCacheHandler;

	public SingleOrderHandler(GatewayController ibController, AllOrderHandler orderCacheHandler, IBOrder o) {
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
		log("<-- SingleOrder udpate orderStatus: filled " + filled + " remaining:" + remaining + " permId:" + permId);
		_order.setStatus(status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice);
		_orderCacheHandler.writeToCacheAndOMS(_order);

		if (filled > 0) {
			log("Force req account balance again after 1 seconds");
			// started from 20220615 no account mv update after order placed.			
			new Timer("GatewayControllerDelayTask _postConnected()").schedule(new TimerTask() {
				@Override
				public void run() {
					_ibController.subscribeAccountMV();
				}
			}, 1000);
		}
	}

	@Override
	public void handle(int errorCode, String errorMsg) {
		int orderId = _order.orderId();
		JSONObject j = new JSONObject();
		j.put("type", "order_error");
		j.put("orderId", orderId);
		j.put("permId", _order.permId());
		j.put("client_oid", _order.omsClientOID());
		j.put("code", errorCode);
		j.put("msg", errorMsg);
		// mark order status.
		boolean printMsg = true;
		switch(errorCode) {
			case 201: // code:201, msg:Order rejected - reason:
				_order.setRejected(errorMsg);
				_orderCacheHandler.writeToCacheAndOMS(_order);
				break;
			case 202: // code:202, msg:Order Canceled - reason:
				_order.setCancelled(errorMsg);
				_orderCacheHandler.writeToCacheAndOMS(_order);
				break;
			case 399: // Order error: check them by message
				if (errorMsg.contains("Warning: your order will not be placed at the exchange until "))
					break; // This is okay.
			case 10147: // OrderId 51 that needs to be cancelled is not found.
				_order.setCancelled(errorMsg);
				_orderCacheHandler.writeToCacheAndOMS(_order);
			case 10148: // OrderId 51 that needs to be cancelled cannot be cancelled, state: Cancelled.
				_order.setCancelled(errorMsg);
				_orderCacheHandler.writeToCacheAndOMS(_order);
				break;
			case 10198: // Order bound is rejected: No such order
				_order.setCancelled(errorMsg);
				_orderCacheHandler.writeToCacheAndOMS(_order);
				break;
			default:
				log(_order);
				// Should be some kind of error, let clients know.
				err("<-- broadcast unknown error for order [" + _order.omsClientOID() + "]\norder id [" + orderId + "]:" + errorCode + "," + errorMsg);
				_ibController.ack(j);
				printMsg = false;
				break;
		}
		// o includes errorMsg
		if (printMsg == false)
			;
		else if (errorMsg.length() > 20)
			info("<-- order msg for order [" + orderId + "]," + errorCode + " " + errorMsg.substring(0, 19) + "...\n" + _order);
		else
			info("<-- order msg for order [" + orderId + "]," + errorCode + " " + errorMsg + "...\n" + _order);
		// info("<-- order msg for [" + orderId + "]," + errorCode + "," + errorMsg);
		// Tell _ibController this message is processed.
		_ibController.lastAckErrorID = _order.orderId();
		_ibController.lastAckErrorCode = errorCode;
		_ibController.lastAckErrorMsg = errorMsg;
	}
}
