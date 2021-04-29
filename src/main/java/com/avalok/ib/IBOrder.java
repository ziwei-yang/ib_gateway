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
	Double filled, remaining, avgFillPrice, lastFillPrice, mktCapPrice;
	String whyHeld;
	boolean statusFilled = false; // Basic status need to be filled before using this order.
	public void setStatus(
			int _orderId, OrderStatus _status, double _filled,
			double _remaining, double _avgFillPrice,
			int _permId, int _parentId, double _lastFillPrice,
			int _clientId, String _whyHeld, double _mktCapPrice) {
		if(_orderId != order.orderId())
			err("setStatus() orderId not coinsistent " + _orderId + "," + order.orderId());
		if(_permId != order.permId())
			err("setStatus() permId not coinsistent " + _permId + "," + order.permId());
		if (order.totalQuantity() != _filled + _remaining)
			err("setStatus() size not coinsistent (" + _filled + "+" + _remaining + "), "+ order.totalQuantity());
		orderState.status(_status);
		remaining = _remaining;
		filled = _filled;
		order.filledQuantity(_filled);
		avgFillPrice = _avgFillPrice;
		lastFillPrice = _lastFillPrice;
		mktCapPrice = _mktCapPrice;
		whyHeld = _whyHeld;
		statusFilled = true;
		log("<-- orderStatus:\n" + toString());
	}
	
	public void setCompleted() { statusFilled = true; }

	public int orderId() { return order.orderId(); }
	
	public int permId() { return order.permId(); }

	// Additional status extension, for AllOrderHandler.handle(msg).
	private String extStatus = null;
	private String extMsg = null;
	public void setRejected (String msg) {
		extStatus = "Rejected";
		extMsg = msg;
	}
	public void setCancelled (String msg) {
		extStatus = "Cancelled";
		extMsg = msg;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(StringUtils.rightPad(contract.shownName(), 40));
		
		// BUY Price
		sb.append(StringUtils.rightPad(order.action().toString(), 5));
		sb.append(StringUtils.rightPad(""+order.lmtPrice(), 8));
		
		// execute/total
		if (statusFilled)
			sb.append(StringUtils.leftPad("" + order.filledQuantity(), 8));
		else
			sb.append(StringUtils.leftPad("??", 8));
		sb.append('/');
		sb.append(StringUtils.rightPad("" + order.totalQuantity(), 8));
		
		if (extStatus == null)
			sb.append(StringUtils.rightPad(orderState.status().toString(), 16));
		else
			sb.append(StringUtils.rightPad(extStatus, 16));

		sb.append(StringUtils.rightPad("" + order.permId(), 12));
		sb.append(StringUtils.rightPad("[" + order.orderId() + "]", 6));

		if (extMsg != null) {
			sb.append('\n');
			sb.append(extMsg);
		}
		
		if (order.action() == Action.BUY)
			return green(sb.toString());
		if (order.action() == Action.SELL)
			return red(sb.toString());
		return sb.toString();
	}
}
