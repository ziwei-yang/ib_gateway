package com.avalok.ib.handler;

import com.ib.controller.ApiController;
import com.ib.controller.Bar;

import static com.bitex.util.DebugUtil.log;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bitex.util.Redis;

public class HistoricalDataHandler implements ApiController.IHistoricalDataHandler {
	protected final JSONArray historyBar = new JSONArray();
	private Long id;
	
	public HistoricalDataHandler(Long _id) {
		id = _id;
	}
	
    @Override
    public void historicalData(Bar bar) {
//      String msg = EWrapperMsgGenerator.historicalData(0,String.valueOf(bar.time()), bar.open(), bar.high(), bar.low(), bar.close(), bar.volume(), bar.count(), bar.wap());
//    	String msg = bar.toString();

        JSONObject j = new JSONObject();
        j.put("timestamp", bar.time());
        j.put("open", bar.open());
        j.put("high", bar.high());
        j.put("low", bar.low());
        j.put("close", bar.close());
        j.put("volume", bar.volume());
//        j.put("formattedTime", bar.formattedTime());
        historyBar.add(j);
//    	log(bar.formattedTime() + " " + msg);
    }

    @Override
    public void historicalDataEnd() {
//    	Redis.setex(null, 30, historyBar);
		String key = "IBGateway:ReqIdHistoricalData:" + id;
		log("Redis -> " + key);
		Redis.setex(key, 30, historyBar);

    }
}
