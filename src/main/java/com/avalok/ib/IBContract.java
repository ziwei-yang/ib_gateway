package com.avalok.ib;

import com.ib.client.ComboLeg;
import com.ib.client.Contract;
import com.ib.client.ContractDetails;
import com.ib.client.Types.*;
import com.alibaba.fastjson.JSONObject;
import com.avalok.ib.handler.ContractDetailsHandler;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.bitex.util.DebugUtil.*;

/**
 * Wrapper of Contract
 */
public class IBContract extends Contract {
	protected String _shownName = null;
	protected String _parsedName = null;
	protected String _pair = null;
	protected boolean _fullDetailed = false;
	public String shownName() {
		if (_shownName != null)
			return _shownName;
		return _parsedName;
	}
	public String pair() { return _pair; }

	public IBContract() { }
	public IBContract(String exchange, String shownName) throws Exception {
		_parsedName = "from:"+exchange+" "+shownName;
		exchange(exchange.toUpperCase());
		parseName(shownName);
		buildFullInfo();
	}
	
	/**
	 * {exchange}:{SecType}:{currency}-{symbol}:{expiry}:{multiplier}
	 */
	private void buildFullInfo() {
		if (_fullDetailed == false) {
			ContractDetailsHandler.fillIBContract(this);
			if (_fullDetailed == false)
				warn("Could not find contract details in cache:\n"+toJSON());
		}
		if (isCombo()) {
			List<String> conidList = new ArrayList<>();
			for (ComboLeg leg: comboLegs()) { conidList.add(String.valueOf(leg.conid())); }

			_pair= String.join("+", conidList);
			_shownName = exchange() + ":" + secType() + ":" + _pair;
		} else {
			String s = currency() + "-" + symbol();
			if (secType() == SecType.STK)
				;
			else if (secType() == SecType.FUT) {
				if (multiplier() == null || Double.parseDouble(multiplier()) == 1)
					s = s + "@" + lastTradeDateOrContractMonth();
				else
					s = s + "@" + lastTradeDateOrContractMonth() + "@" + multiplier();
			} else if(secType() == SecType.OPT) {
				s = s + "@" + lastTradeDateOrContractMonth() + "@" + multiplier() + getRight() + strike();
			} else if(secType() == SecType.BOND) {
				// bonds contract don't symbol()
				if (symbol() == null) {
					// from ib response contract
					s = tradingClass() + "-" + conid();
				} else {
					// from input args
					// use "s = symbol()" before get contract
					s = symbol();
				}
			} else {
				info("Unknown secType: " + secType());
				if (lastTradeDateOrContractMonth() == null || lastTradeDateOrContractMonth().length() == 0)
					;
				else
					s = s + "@" + lastTradeDateOrContractMonth();
				if (multiplier() == null || Double.parseDouble(multiplier()) == 1)
					;
				else
					s = s + "@" + multiplier();
			}
			_pair= s;
			s = exchange() + ":" + secType() + ":" + s;
			_shownName = s;
		}
	}
	
	public boolean isFullDetailed() { return _fullDetailed; }
	
	/**
	 * Parse string like:
	 * [EXCHANGE/STK/]CURRENCY-SYMBOL[@EXPIRY][@MUL]
	 * 
	 * USD.HKD as FX TODO
	 * currency-symbol as STK
	 * currency-symbol@expiry as FUT
	 * currency-symbol@expiry@multiplier as FUT
	 * USD-TSLA
	 * USD-BAKKT@20210625
	 * USD-BAKKT@202106
	 * USD-BRR@202106@5
	 * USD-BRR@202106@0.1
	 * HKD-1137
	 * 
	 * 
	 */
	private void parseName(String shortName) throws Exception {
		String[] segs = shortName.split("/");
		String name;
		if (segs.length >= 3) {
			exchange(segs[0]);
			secType(SecType.valueOf(segs[1]));
			name = segs[2];
		} else {
			secType(SecType.STK);
			name = segs[0];
		}
		if (name.matches("^[A-Z]{3,5}-[A-Z0-9.]{1,8}$")) {
			segs = name.split("-");
			currency(segs[0]);
			symbol(segs[1]);
			secType(SecType.STK);
		} else if (name.matches("^[A-Z]{3,5}-[A-Z0-9.]{1,8}@[0-9]{6,8}(@[0-9.]*|)$")) {
			segs = name.split("@");
			lastTradeDateOrContractMonth(segs[1]);
			if (segs.length >= 3)
				multiplier(segs[2]);
			segs = segs[0].split("-");
			currency(segs[0]);
			symbol(segs[1]);
			secType(SecType.FUT);
		} else throw new Exception("Unknwon contract descrption " + shortName);
		buildFullInfo();
	}

	public IBContract(JSONObject j) {
		if (j.getInteger("conid") != null)
			conid(j.getInteger("conid"));
		if (j.getString("symbol") != null)
			symbol(j.getString("symbol"));
		if (j.getString("secType") == null)
			secType(SecType.None);
		else
			secType(SecType.valueOf(j.getString("secType")));
		if (j.getString("lastTradeDateOrContractMonth") != null)
			lastTradeDateOrContractMonth(j.getString("lastTradeDateOrContractMonth"));
		if (j.getDouble("strike") != null)
			strike(j.getDouble("strike"));
		if (j.getString("right") == null)
			right(Right.None);
		else
			right(Right.valueOf(j.getString("right")));
		if (j.getString("multiplier") != null)
			multiplier(j.getString("multiplier"));
		if (j.getString("exchange") != null)
			exchange(j.getString("exchange"));
		if (j.getString("primaryExch") != null)
			primaryExch(j.getString("primaryExch"));
		if (j.getString("currency") != null)
			currency(j.getString("currency"));
		if (j.getString("localSymbol") != null)
			localSymbol(j.getString("localSymbol"));
		if (j.getString("tradingClass") != null)
			tradingClass(j.getString("tradingClass"));

		if (j.getBoolean("_fullDetailed") != null)
			_fullDetailed = j.getBoolean("_fullDetailed");
		buildFullInfo();
	}
	
	public IBContract(Contract c) {
		conid(c.conid());
		symbol(c.symbol());
		secType(c.secType());
		lastTradeDateOrContractMonth(c.lastTradeDateOrContractMonth());
		strike(c.strike());
		right (c.right());
		multiplier(c.multiplier());
		exchange(c.exchange());
		primaryExch(c.primaryExch());
		currency(c.currency());
		localSymbol(c.localSymbol());
		tradingClass(c.tradingClass());
		secIdType (c.secIdType());
		secId(c.secId());
		comboLegs(c.comboLegs());
		comboLegsDescrip(c.comboLegsDescrip());
		_fullDetailed = true; // Always trust Contract from IB API
		buildFullInfo();
	}
	
	public void copyFrom(IBContract c) {
		conid(c.conid());
		symbol(c.symbol());
		secType(c.secType());
		lastTradeDateOrContractMonth(c.lastTradeDateOrContractMonth());
		strike(c.strike());
		right (c.right());
		multiplier(c.multiplier());
		exchange(c.exchange());
		primaryExch(c.primaryExch());
		currency(c.currency());
		localSymbol(c.localSymbol());
		tradingClass(c.tradingClass());
		secIdType (c.secIdType());
		secId(c.secId());
		comboLegs(c.comboLegs());
		comboLegsDescrip(c.comboLegsDescrip());
		_fullDetailed = c._fullDetailed;
		buildFullInfo();
	}
	
	public JSONObject toJSON() {
		JSONObject j = new JSONObject();
		j.put("conid", conid());
		if (symbol() != null && symbol().length() > 0)
			j.put("symbol", symbol());
		j.put("secType", secType());
		if (lastTradeDateOrContractMonth() != null && lastTradeDateOrContractMonth().length() > 0)
			j.put("lastTradeDateOrContractMonth", lastTradeDateOrContractMonth());
		j.put("strike", strike());
		j.put("right", right());
		if (multiplier() != null && multiplier().length() > 0)
			j.put("multiplier", multiplier());
		if (exchange() != null && exchange().length() > 0)
			j.put("exchange", exchange());
		if (primaryExch() != null && primaryExch().length() > 0)
			j.put("primaryExch", primaryExch());
		if (currency() != null && currency().length() > 0)
			j.put("currency", currency());
		if (localSymbol() != null && localSymbol().length() > 0)
			j.put("localSymbol", localSymbol());
		if (tradingClass() != null && tradingClass().length() > 0)
			j.put("tradingClass", tradingClass());
		j.put("_fullDetailed", _fullDetailed);
		return j;
	}
	
	@Override
	public boolean equals(Object o) {
		IBContract ibc = null;
		if (o instanceof IBContract)
			ibc = (IBContract)o;
		else
			return false;
		if (conid() != ibc.conid()) return false;
		if (symbol() == null && ibc.symbol() == null) {
			;
		} else if (symbol() != null && ibc.symbol() != null) {
			if (symbol().equals(ibc.symbol()) == false)
					return false;
		} else {
			return false;
		}
		if (secType() != ibc.secType()) return false;
		if (lastTradeDateOrContractMonth() == null && ibc.lastTradeDateOrContractMonth() == null) {
			;
		} else if (lastTradeDateOrContractMonth() != null && ibc.lastTradeDateOrContractMonth() != null) {
			if (lastTradeDateOrContractMonth().equals(ibc.lastTradeDateOrContractMonth()) == false)
				return false;
		} else {
			return false;
		}
		if (strike() != ibc.strike()) return false;
		if (right() != ibc.right()) return false;
		if (multiplier() == null && ibc.multiplier() == null) {
			;
		} else if (multiplier() != null && ibc.multiplier() != null) {
			if (multiplier().equals(ibc.multiplier()) == false)
			return false;
		} else {
			return false;
		}
		if (exchange() == null && ibc.exchange() == null) {
			;
		} else if (exchange() != null && ibc.exchange() != null) {
			if (exchange().equals(ibc.exchange()) == false)
				return false;
		} else {
			return false;
		}
		if (primaryExch() == null && ibc.primaryExch() == null) {
			;
		} else if (primaryExch() != null && ibc.primaryExch() != null) {
			if (primaryExch().equals(ibc.primaryExch()) == false)
				return false;
		} else {
			return false;
		}
		if (currency() == null && ibc.currency() == null) {
			;
		} else if (currency() != null && ibc.currency() != null) {
			if (currency().equals(ibc.currency()) == false)
				return false;
		} else {
			return false;
		}
		if (localSymbol() == null && ibc.localSymbol() == null) {
			;
		} else if (localSymbol() != null && ibc.localSymbol() != null) {
			if (localSymbol().equals(ibc.localSymbol()) == false)
				return false;
		} else {
			return false;
		}
		if (tradingClass() == null && ibc.tradingClass() == null) {
			;
		} else if (tradingClass() != null && ibc.tradingClass() != null) {
			if (tradingClass().equals(ibc.tradingClass()) == false)
				return false;
		} else {
			return false;
		}
		return true;
	}
	
	/**
	 * If existed details match another contract
	 */
	public boolean matchFullDetails(IBContract fullDetailedContract) {
		if (conid() != 0)
			if (conid() != fullDetailedContract.conid())
				return false;
		if (symbol() != null)
			if (symbol().equals(fullDetailedContract.symbol()) == false)
				return false;
		if (secType() != SecType.None)
			if (secType() != fullDetailedContract.secType())
				return false;
		if (lastTradeDateOrContractMonth() != null)
			if (fullDetailedContract.lastTradeDateOrContractMonth().startsWith(lastTradeDateOrContractMonth()) == false)
				return false;
		if (strike() != 0)
			if (strike() != fullDetailedContract.strike())
				return false;
		if (right() != Right.None)
			if (right() != fullDetailedContract.right())
				return false;
		if (multiplier() != null)
			if (multiplier().equals(fullDetailedContract.multiplier()) == false)
				return false;
		if (exchange() != null)
			if (exchange().equals(fullDetailedContract.exchange()) == false)
				return false;
		if (primaryExch() != null)
			if (primaryExch().equals(fullDetailedContract.primaryExch()) == false)
				return false;
		if (currency() != null)
			if (currency().equals(fullDetailedContract.currency()) == false)
				return false;
		if (localSymbol() != null)
			if (localSymbol().equals(fullDetailedContract.localSymbol()) == false)
				return false;
		if (tradingClass() != null)
			if (tradingClass().equals(fullDetailedContract.tradingClass()) == false)
				return false;
		return true;
	}
}

