package com.avalok.ib;

import com.avalok.ib.handler.ContractDetailsHandler;
import static com.bitex.util.DebugUtil.*;

import org.apache.commons.lang3.StringUtils;
import com.alibaba.fastjson.JSONObject;

import com.ib.client.Contract;
import com.ib.client.Order;
import com.ib.client.OrderState;
import com.ib.client.OrderStatus;
import com.ib.client.Types.Action;

public class IBOrder {
	public IBContract contract;
	public Order order;
	// https://interactivebrokers.github.io/tws-api/order_submission.html#order_status
	protected OrderState orderState;
	boolean toBePlaced = false;
	/**
	 * Build new order to place
	 */
	public IBOrder(Contract _contract, Order _order) {
		contract = new IBContract(_contract);
		ContractDetailsHandler.findDetails(contract);
		order = _order;
		toBePlaced = true;
	}
	/**
	 * Build new order to place,
	 * Parse full contract from j['contract']
	 * Parse URANUS format order from j['order']
	 */
	public IBOrder(JSONObject j) {
		contract = new IBContract(j.getJSONObject("contract"));
		ContractDetailsHandler.findDetails(contract);
		JSONObject oj = j.getJSONObject("order");
		order = new Order();
		order.account(oj.getString("account"));
		if (oj.getString("i") != null) // Only when modifying existed order.
			order.permId(Integer.parseInt(oj.getString("i")));
		order.action(oj.getString("T").toUpperCase()); // BUY SELL
		order.totalQuantity(oj.getDoubleValue("s"));
		order.lmtPrice(oj.getDoubleValue("p"));
		if (oj.containsKey("executed"))
			order.filledQuantity(oj.getDoubleValue("executed"));
		// Don't parse status, will get updates from IB after placing/modifying
		if (oj.containsKey("orderType"))
			order.orderType(oj.getString("orderType")); // Only place limit order
		else
			order.orderType("LMT"); // Only place limit order
		if (oj.getString("tif") != null) // Default: DAY
			order.tif(oj.getString("tif"));
		if (oj.getString("orderRef") != null) // Default: DAY
			order.orderRef(oj.getString("orderRef"));
		toBePlaced = true;
	}
	/**
	 * Build known order
	 */
	public IBOrder(Contract _contract, Order _order, OrderState _orderState) {
		contract = new IBContract(_contract);
		ContractDetailsHandler.findDetails(contract);
		order = _order;
		orderState = _orderState;
		fixIBBug01();
	}

	private void fixIBBug01() {
		// IB BUG fix, fuck totalQuantity
		if (orderState.status() == OrderStatus.Filled && order.totalQuantity() == 0) {
			order.totalQuantity(order.filledQuantity());
			warn("Fixing IB zero totalQuantity bug, fuck me -> " + order.totalQuantity());
		}
	}
	
	
	public int orderId() { return order.orderId(); }
	public int permId() { return order.permId(); }
	public String account() { return order.account(); }
	
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
		setStatus(_status, _filled, _remaining, _avgFillPrice, _permId, _parentId, _lastFillPrice, _clientId, _whyHeld, _mktCapPrice);
	}
	public void setStatus(
			OrderStatus _status, double _filled,
			double _remaining, double _avgFillPrice,
			int _permId, int _parentId, double _lastFillPrice,
			int _clientId, String _whyHeld, double _mktCapPrice) {
		if(order.permId() != 0 && _permId != order.permId())
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
		fixIBBug01();
		log("<-- orderStatus:\n" + toString());
	}

	public void orderState(OrderState os) { orderState = os; }
	
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
	public void setCompleted() { statusFilled = true; }
	/**
	 * return null : unknown
	 */
	public Boolean isAlive() {
		if (statusFilled != true) return null;
		if (extStatus != null) {
			switch (extStatus) {
			case "Rejected":
				return false;
			case "Cancelled":
				return false;
			default:
				errWithTrace("Unknwon extStatus " + extStatus);
				break;
			}
		}
		if (orderState == null) return null;
		if (orderState.status().isActive()) return true;
		if (orderState.status() == OrderStatus.Inactive) {
			// https://interactivebrokers.github.io/tws-api/order_submission.html#order_status
			if (order.orderRef() != null && order.orderRef().startsWith("uranus_"))
				return true; // an order is placed manually in TWS while the exchange is closed.
			else
				return false;
		}
		if (orderState.status() == OrderStatus.Unknown) return true;
		return false;
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
		
		if (extStatus != null)
			sb.append(StringUtils.rightPad(extStatus, 16));
		else if (orderState != null)
			sb.append(StringUtils.rightPad(orderState.status().toString(), 16));
		else
			sb.append(StringUtils.rightPad("---", 16));

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

	/**
	 * Compatiable with URANUS OMS system.
	 */
	public JSONObject toOMSJSON() {
		if (!statusFilled)
			errWithTrace("Should not call toOMSJSON() when statusFilled is false");
		else if (!contract.isFullDetailed())
			errWithTrace("Should not call toOMSJSON() when contract has no full detail");
		JSONObject j = new JSONObject();
		j.put("i", ""+order.permId());
		// j.put("client_oid", order.orderRef()); // suggest using orderRef to store client_oid+timestamp when created
		j.put("pair", contract.pair());
		if (order.action() == Action.BUY)
			j.put("T", "buy");
		else if (order.action() == Action.SELL)
			j.put("T", "sell");
		else
			errWithTrace("Unknown order action " + order.action());
		j.put("ttl_qty", order.totalQuantity());
		j.put("p", order.lmtPrice());
		if (avgFillPrice == null)
			j.put("avg_price", order.lmtPrice());
		else
			j.put("avg_price", avgFillPrice);
		j.put("executed_qty", order.filledQuantity());
		j.put("remained_qty", order.totalQuantity()-order.filledQuantity());
		j.put("status", extStatus == null ? orderState.status().toString() : extStatus);
		// created time missing, default 2000-01-01 00:00:00
		// suggest using orderRef to store client_oid+timestamp when created.
		j.put("t", 946656000000l);
		j.put("market", contract.exchange());
		j.put("orderType", order.orderType()); // LMT
		j.put("tif", order.tif()); // LMT
		
		j.put("orderRef", order.orderRef());
		j.put("extMsg", extMsg);
		return j;
	}
	
	public String omsId() {
		if (order.permId() == 0) return null;
		return ""+order.permId();
	}
	public String omsClientOID() {
		String orderRef = order.orderRef();
		if (orderRef != null && orderRef.startsWith("uranus_")) return orderRef;
		return null;
	}
}
