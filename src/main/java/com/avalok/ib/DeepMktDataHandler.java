package com.avalok.ib;

import java.util.*;

import com.ib.controller.ApiController.IDeepMktDataHandler;
import com.ib.client.Types.*;

import static com.bitex.util.DebugUtil.*;

public class DeepMktDataHandler implements IDeepMktDataHandler {
	protected IBContract _contract;
	protected boolean dataInited = false; // Wait until all ASK/BID filled
	public DeepMktDataHandler(IBContract contract) {
		_contract = contract;
	}

	class Order implements Comparable<Order> {
		String mm;
		double price = -1;
		int size = 0;
		@Override
		public int compareTo(Order o) { return Double.compare(price, o.price); }
	}

	public final static int MAX_DEPTH = 20;
	protected Order[] asks = new Order[MAX_DEPTH];
	protected Order[] bids = new Order[MAX_DEPTH];

	@Override
	public void updateMktDepth(int pos, String mm, DeepType operation, DeepSide side, double price, int size) {
//		log("DeepType " + pos + " " + side + " " + operation + " " + price + " " + size);
		if (pos >= MAX_DEPTH) return;
		Order o = new Order();
		o.mm = mm;
		o.price = price;
		o.size = size;
		if (operation == DeepType.INSERT) {
			dataInited = false;
			if (side == DeepSide.BUY) bids[pos] = o;
			else asks[pos] = o;
		} else if (operation == DeepType.UPDATE) {
			dataInited = true;
			if (side == DeepSide.BUY) bids[pos] = o;
			else asks[pos] = o;
		} else if (operation == DeepType.DELETE) {
			dataInited = true;
			if (side == DeepSide.BUY) bids[pos] = null;
			else asks[pos] = null;
		} else {
			log("Error DeepType " + pos + " " + side + " " + operation + " " + price + " " + size);
			return;
		}
		// Debug
		if (dataInited && asks[0] != null && bids[0] != null && pos == 0)
			log(_contract.shownName() + " " + bids[0].size + " " + bids[0].price+ " --- " + asks[0].price + " " + asks[0].size);
	}
}

