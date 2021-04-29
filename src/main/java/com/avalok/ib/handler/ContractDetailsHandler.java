package com.avalok.ib.handler;

import static com.bitex.util.DebugUtil.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.avalok.ib.GatewayController;
import com.avalok.ib.IBContract;
import com.bitex.util.Redis;
import com.ib.client.*;
import com.ib.controller.ApiController.*;

import redis.clients.jedis.Jedis;

// Write STK and FUT info to redis:
// IBGateway:Contract:FUT:CMECRYPTO:USD-BRR:20210625:5
// IBGateway:Contract:FUT:CMECRYPTO:USD-BRR:20210625:0.1
// IBGateway:Contract:STK:SEHK:HKD-1137
public class ContractDetailsHandler implements IContractDetailsHandler {
	////////////////////////////////////////////////////////////////
	// Fill IBContract details, query if possible.
	////////////////////////////////////////////////////////////////
	public static final Map<String, IBContract> KNOWN_CONTRACTS = new ConcurrentHashMap<>();
	public static final Map<String, JSONObject> KNOWN_CONTRACT_DETAILS = new ConcurrentHashMap<>();
	public static GatewayController GW_CONTROLLER = null;

	public static void fillIBContract(IBContract ibc) {
		Collection<IBContract> contracts = KNOWN_CONTRACTS.values();
		IBContract result = null;
		for (IBContract _ibc : contracts) {
			if (ibc.matchFullDetails(_ibc)) {
				if (result == null)
					result = _ibc;
				else
					warn("Multiple results matches:" + ibc.shownName());
			}
		}
		if (result != null) {
			ibc.copyFrom(result);
			return;
		}
		if (GW_CONTROLLER != null) {
			err("Auto query contract details:" + ibc.shownName());
			GW_CONTROLLER.queryContractList(ibc);
		}
	}

	public static JSONObject findDetails(IBContract ibc) {
		if (ibc.isFullDetailed() == false)
			fillIBContract(ibc);
		JSONObject ret = KNOWN_CONTRACT_DETAILS.get(ibc.shownName());
		if (ret != null) return ret;
		if (GW_CONTROLLER != null) {
			warn("Auto query contract details:" + ibc.shownName());
			GW_CONTROLLER.queryContractList(ibc);
		}
		return null;
	}

	////////////////////////////////////////////////////////////////
	// Handler interfaces
	////////////////////////////////////////////////////////////////
	public void contractDetails(List<ContractDetails> list) {
		for (ContractDetails detail: list) {
			// info("<-- " + detail);
			Contract c = detail.contract();
			IBContract ibc = new IBContract(c);
			String shortName = new IBContract(c).shownName();
			log("<-- contract detail " + shortName);
			JSONObject json = writeDetail("IBGateway:Contract:"+shortName, detail);
			KNOWN_CONTRACTS.put(shortName, ibc);
			KNOWN_CONTRACT_DETAILS.put(shortName, json);
		}
	}

	private JSONObject writeDetail(String key, ContractDetails detail) {
		JSONObject j = new JSONObject();
		// vim marco helps a lot from ContractDetails source code.
		j.put("contract", new IBContract(detail.contract()).toJSON());
		j.put("marketName", detail.marketName());
		j.put("minTick", detail.minTick());
		j.put("priceMagnifier", detail.priceMagnifier());
		j.put("orderTypes", detail.orderTypes());
		j.put("validExchanges", detail.validExchanges());
		j.put("underConId", detail.underConid());
		j.put("longName", detail.longName());
		j.put("contractMonth", detail.contractMonth());
		j.put("industry", detail.industry());
		j.put("category", detail.category());
		j.put("subcategory", detail.subcategory());
		j.put("timeZoneId", detail.timeZoneId());
		j.put("tradingHours", detail.tradingHours());
		j.put("liquidHours", detail.liquidHours());
		j.put("evRule", detail.evRule());
		j.put("evMultiplier", detail.evMultiplier());
		j.put("mdSizeMultiplier", detail.mdSizeMultiplier());
		j.put("aggGroup", detail.aggGroup());
		j.put("underSymbol", detail.underSymbol());
		j.put("underSecType", detail.underSecType());
		j.put("marketRuleIds", detail.marketRuleIds());
		j.put("realExpirationDate", detail.realExpirationDate());
		j.put("lastTradeTime", detail.lastTradeTime());
		j.put("cusip", detail.cusip());
		j.put("ratings", detail.ratings());
		j.put("descAppend", detail.descAppend());
		j.put("bondType", detail.bondType());
		j.put("couponType", detail.couponType());
		j.put("callable", detail.callable());
		j.put("putable", detail.putable());
		j.put("coupon", detail.coupon());
		j.put("convertible", detail.convertible());
		j.put("maturity", detail.maturity());
		j.put("issueDate", detail.issueDate());
		j.put("nextOptionDate", detail.nextOptionDate());
		j.put("nextOptionType", detail.nextOptionType());
		j.put("nextOptionPartial", detail.nextOptionPartial());
		j.put("notes", detail.notes());
		final String s = JSON.toJSONString(j);
		Redis.exec(new Consumer<Jedis>() {
			@Override
			public void accept(Jedis t) { t.set(key, s); }
		});
		return j;
	}
}