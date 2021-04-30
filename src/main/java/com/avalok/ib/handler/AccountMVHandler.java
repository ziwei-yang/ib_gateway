package com.avalok.ib.handler;

import static com.bitex.util.DebugUtil.*;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.avalok.ib.IBContract;
import com.bitex.util.Redis;
import com.ib.controller.Position;
import com.ib.controller.ApiController.IAccountHandler;

import redis.clients.jedis.Jedis;

public class AccountMVHandler implements IAccountHandler {

	// Modify _tmpData before _dataInit
	// Modify _data directly after _dataInit
	private boolean _dataInit = false;
	private Map<String, Map<String, JSONObject>> _data = new ConcurrentHashMap<>();
	private Map<String, Map<String, JSONObject>> _tmpData = new ConcurrentHashMap<>();

	@Override
	public synchronized void accountValue(String account, String key, String desc1, String desc2) {
		// Example:
		// AccountCode null U8620103
		// AccountOrGroup BASE zwyang
		// AccountOrGroup HKD zwyang
		// AccountReady null true
		// AccountType null INDIVIDUAL
		// CashBalance HKD 20000.00
		// CashBalance USD 484333.21
		// FullInitMarginReq USD 15663.90
		// What we have interest is how much balance remained in currency.
		JSONObject j = new JSONObject();
		if (desc2 != null && desc2.equals("BASE")) return; // Skip all info in BASE currency. 
		if (key.equals("CashBalance")) {
			j.put("type", key);
			j.put("currency", desc1);
			j.put("balance", desc2);
		} else return;
		info("<-- AccountMV " + account + " key " + key + " " + desc1 + " " + desc2);
		if (_dataInit) {
			_data.putIfAbsent(account, new ConcurrentHashMap<String, JSONObject>());
			_data.get(account).put(desc1, j);
			writePosition();
		} else {
			_tmpData.putIfAbsent(account, new ConcurrentHashMap<String, JSONObject>());
			_tmpData.get(account).put(desc1, j);
		}
	}

	@Override
	public void accountTime(String timeStamp) {
		// info("<-- AccountMV accountTime " + timeStamp);
	}

	@Override
	public synchronized void updatePortfolio(Position position) {
		IBContract ibc = new IBContract(position.contract());
		ContractDetailsHandler.findDetails(ibc); // Auto query details for instruments in portfolio
		String account = position.account();
		info("<-- AccountMV pfl " + account + " " + ibc.exchange() + "/" + ibc.shownName() + " pos:" + position.position() + " cost:" + position.averageCost());
		JSONObject j = new JSONObject();
		j.put("contract", ibc.toJSON());
		j.put("pos", position.position());
		j.put("avgCost", position.averageCost());
		if (_dataInit) {
			_data.putIfAbsent(account, new ConcurrentHashMap<String, JSONObject>());
			_data.get(account).put(ibc.shownName(), j);
			writePosition();
		} else {
			_tmpData.putIfAbsent(account, new ConcurrentHashMap<String, JSONObject>());
			_tmpData.get(account).put(ibc.shownName(), j);
		}
	}

	@Override
	public synchronized void accountDownloadEnd(String account) {
		info("<-- AccountMV Download End " + account);
		if (_dataInit == false) {
			_dataInit = true;
			_data = _tmpData;
			_tmpData = new ConcurrentHashMap<>();
		}
		writePosition();
	}

	protected void writePosition() {
		Set<String> accounts = _data.keySet();
		for (String acc : accounts) {
			final String key = "IBGateway:" + acc + ":balance";
			Map<String, JSONObject> pos = _data.get(acc);
			if (pos == null) continue;
			log(">>> Redis " + key);
			Redis.set(key, pos);
		}
	}

	public Map<String, Map<String, JSONObject>> position() {
		if (_dataInit)
			return _data;
		else
			return null;
	}
}
