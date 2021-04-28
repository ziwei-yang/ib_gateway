package com.avalok.ib;

import com.ib.client.Contract;
import com.ib.client.Types.*;

import com.alibaba.fastjson.JSONObject;

/**
 * Wrapper of Contract
 */
public class IBContract extends Contract {
	public int timezone = 0;
	public int millisecond_offset = 0;
	protected String _shownName = "unamed";
	public String shownName() { return _shownName; }

	public IBContract() { }
	/**
	 * Parse string like:
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
	 */
	public IBContract(String exchange, String shownName) throws Exception {
		exchange(exchange.toUpperCase());
		_shownName = shownName.toUpperCase();
		if (shownName.matches("^[A-Z]{3,5}-[A-Z0-9.]{1,8}$")) {
			String[] segs = shownName.split("-");
			currency(segs[0]);
			symbol(segs[1]);
			secType("STK");
		} else if (shownName.matches("^[A-Z]{3,5}-[A-Z0-9.]{1,8}@[0-9]{6,8}(@[0-9.]*|)$")) {
			String[] segs = shownName.split("@");
			lastTradeDateOrContractMonth(segs[1]);
			if (segs.length >= 3)
				multiplier(segs[2]);
			segs = segs[0].split("-");
			currency(segs[0]);
			symbol(segs[1]);
			secType("FUT");
			// TODO Complement of expiry
		} else throw new Exception("Unknwon contract descrption " + shownName);
	}
	
	/**
	 * Converted symbol used in universal system.
	 */
	public String standard_symbol() {
		if (exchange().equals("ICECRYPTO") && symbol().equals("BAKKT"))
			return "BTC";
		if (exchange().equals("CMECRYPTO") && symbol().equals("BRR") && multiplier().equals("5"))
			return "BTC";
		if (exchange().equals("CMECRYPTO") && symbol().equals("BRR") && multiplier().equals("0.1"))
			return "BTC";
		return symbol();
	}

	public IBContract(JSONObject j) {
		JSONObject j1 = j.getJSONObject("contract");
		conid(j1.getInteger("conid"));
		currency(j1.getString("currency"));
		exchange(j1.getString("exchange"));
		lastTradeDateOrContractMonth(j1.getString("lastTradeDateOrContractMonth"));
		localSymbol(j1.getString("localSymbol"));
		if (j1.getString("right").length() == 0)
			right(Right.None);
		else
			right(Right.valueOf(j1.getString("right")));
		if (j1.getString("secType").length() == 0)
			secType(SecType.None);
		else
			secType(SecType.valueOf(j1.getString("secType")));
		strike(j1.getDouble("strike"));
		symbol(j1.getString("symbol"));
		tradingClass(j1.getString("tradingClass"));

		if (j.getInteger("timezone") != null)
			timezone = j.getInteger("timezone");
		if (j.getInteger("millisecond_offset") != null)
			millisecond_offset = j.getInteger("millisecond_offset");

		if (j1.getString("shownName").length() != 0)
		_shownName = j1.getString("shownName");
	}

	public void timezone(int tz) {
		timezone = tz;
		millisecond_offset = timezone * 60 * 60 * 1000;
	}
}

