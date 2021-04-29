package com.avalok.ib.handler;

import static com.bitex.util.DebugUtil.*;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.avalok.ib.IBContract;
import com.bitex.util.Redis;
import com.ib.client.Contract;
import com.ib.controller.ApiController.IPositionMultiHandler;

import redis.clients.jedis.Jedis;

/**
 * Please use AccountMVHandler, PositionHandler does not receive CASH balance.
 */
public class PositionHandler implements IPositionMultiHandler{
	
	private boolean _dataInit = false;
	private Map<String, JSONArray> _data = new ConcurrentHashMap<>();
	private Map<String, JSONArray> _tmpData = new ConcurrentHashMap<>();

	@Override
	public synchronized void positionMulti(String account, String modelCode, Contract contract, double pos, double avgCost) {
		IBContract ibc = new IBContract(contract);
		info("<-- Position " + account + " " + ibc.exchange() + "/" + ibc.shownName() + " pos:" + pos + " cost:" + avgCost);
		// log(ibc.toJSON());
		_tmpData.putIfAbsent(account, new JSONArray());
		JSONObject j = new JSONObject();
		j.put("modelCode", modelCode);
		j.put("contract", ibc.toJSON());
		j.put("pos", pos);
		j.put("avgCost", avgCost);
		_tmpData.get(account).add(j);
	}

	@Override
	public synchronized void positionMultiEnd() {
		info("<-- Position End");
		_dataInit = true;
		_data = _tmpData;
		_tmpData = new ConcurrentHashMap<>();
		writePosition();
	}
	
	protected void writePosition() {
		Set<String> accounts = _data.keySet();
		for (String acc : accounts) {
			final String key = "IBGateway:"+acc+":position";
			JSONArray pos = _data.get(acc);
			if (pos == null) continue;
			final String posStr = JSON.toJSONString(pos);
			Redis.exec(new Consumer<Jedis>() {
				@Override
				public void accept(Jedis t) {
					t.set(key, posStr);
				}
			});
		}
	}
	
	public Map<String, JSONArray> position() {
		if (_dataInit)
			return _data;
		else
			return null;
	}
}
