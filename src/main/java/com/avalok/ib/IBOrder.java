package com.avalok.ib;

import static com.bitex.util.DebugUtil.*;

import org.apache.commons.lang3.StringUtils;

import com.ib.client.Contract;
import com.ib.client.Order;
import com.ib.client.OrderState;
import com.ib.client.OrderStatus;
import com.ib.client.Types.Action;

public class IBOrder {
	protected IBContract contract;
	protected Order order;
	protected OrderState orderState;
	public IBOrder(Contract _contract, Order _order, OrderState _orderState) {
		contract = new IBContract(_contract);
		order = _order;
		orderState = _orderState;
	}
	
	// Updated from AllOrderHandler.orderStatus();
	Double remaining, avgFillPrice, lastFillPrice, mktCapPrice;
	String whyHeld;
	public void setStatus(
			int _orderId, OrderStatus _status, double _filled,
			double _remaining, double _avgFillPrice,
			int _permId, int _parentId, double _lastFillPrice,
			int _clientId, String _whyHeld, double _mktCapPrice) {
		if(_orderId != order.orderId())
			err("setStatus() orderId not coinsistent " + _orderId + "," + order.orderId());
		if(_permId != order.permId())
			err("setStatus() permId not coinsistent " + _permId + "," + order.permId());
		if (order.filledQuantity() != _filled)
			err("setStatus() filled not coinsistent " + _filled + ","+ order.filledQuantity());
		if (order.totalQuantity() != _filled + remaining)
			err("setStatus() size not coinsistent (" + _filled + "+" + remaining + "), "+ order.totalQuantity());
		orderState.status(_status);
		remaining = _remaining;
		avgFillPrice = _avgFillPrice;
		lastFillPrice = _lastFillPrice;
		mktCapPrice = _mktCapPrice;
		whyHeld = _whyHeld;
		log("<-- orderStatus:\n" + toString());
	}
	
	public void setCompleted() {
		;
	}

	public int orderId() {
		return order.orderId();
	}
	
	public int permId() {
		return order.permId();
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(StringUtils.rightPad(contract.shownName(), 40));
		
		// BUY Price
		sb.append(StringUtils.rightPad(order.action().toString(), 5));
		sb.append(StringUtils.rightPad(""+order.lmtPrice(), 8));
		
		// execute/total
		if (order.filledQuantity() == 0)
			sb.append(StringUtils.leftPad("", 8));
		else
			sb.append(StringUtils.leftPad("" + order.filledQuantity(), 8));
		sb.append('/');
		sb.append(StringUtils.rightPad("" + order.totalQuantity(), 8));
		
		sb.append(StringUtils.rightPad(orderState.status().toString(), 16));
		sb.append(StringUtils.rightPad("" + order.permId(), 12));
		sb.append(StringUtils.rightPad("[" + order.orderId() + "]", 6));
		// sb.append("NO-TIME");
		
		if (order.action() == Action.BUY)
			return green(sb.toString());
		if (order.action() == Action.SELL)
			return red(sb.toString());
		return sb.toString();
	}
}
