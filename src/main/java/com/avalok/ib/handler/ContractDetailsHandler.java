package com.avalok.ib.handler;

import static com.bitex.util.DebugUtil.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.avalok.ib.GatewayController;
import com.avalok.ib.IBContract;
import com.bitex.util.Redis;
import com.ib.client.*;
import com.ib.client.Types.SecType;
import com.ib.controller.ApiController.*;

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
	public static final Map<String, Long> QUERY_HIS = new ConcurrentHashMap<>();
	public static final ContractDetailsHandler instance = new ContractDetailsHandler();
	private ContractDetailsHandler() {}

	public static boolean fillIBContract(IBContract ibc) {
		Collection<IBContract> contracts = KNOWN_CONTRACTS.values();
		IBContract result = null;
		for (IBContract _ibc : contracts) {
			// Don't fill with those SMART exchange contract
			if (_ibc.exchange().equals("SMART") == false && ibc.matchFullDetails(_ibc)) {
				if (result == null)
					result = _ibc;
				else if (ibc.shownName() == null)
					warn("Multiple results matches:" + ibc.shownName());
				else
					warn("Multiple results matches:" + ibc);
			}
		}
		if (result != null) {
			ibc.copyFrom(result);
			return true;
		}
		if (GW_CONTROLLER != null) queryDetails(ibc);
		return false;
	}

	public static JSONObject findDetails(IBContract ibc) {
		if (GW_CONTROLLER != null) queryDetails(ibc);
		if (ibc.isFullDetailed() == false)
			fillIBContract(ibc);
		String key = ibc.shownName();
		if (key == null) key = ibc.toString();
		JSONObject ret = KNOWN_CONTRACT_DETAILS.get(key);
		if (ret != null) return ret;
		return null;
	}

	protected static void queryDetails(IBContract ibc) {
		String key = ibc.shownName();
		log("in queryDetails, key: " + key);
		if (key == null) key = ibc.toString();
		Long lastQueryT = QUERY_HIS.get(key);

		if (lastQueryT == null || lastQueryT + 10_000 < System.currentTimeMillis()) {
			QUERY_HIS.put(key, System.currentTimeMillis());
			warn("--> Auto query contract details:" + key);
			GW_CONTROLLER.queryContractList(ibc);
			if (ibc.exchange() != null && ibc.exchange().equals("SMART")) {
				IBContract ibc2 = new IBContract(ibc);
				ibc2.exchange(null);
				warn("--> Auto query contract details:" + key.replace("SMART", "???"));
				GW_CONTROLLER.queryContractList(ibc2);
			}
		}
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
			String marketRuleIds = detail.marketRuleIds();
			log("<-- contract detail " + shortName + " , mkt rule list " + marketRuleIds);
			if (marketRuleIds != null) {
				for (String id : marketRuleIds.split(",")) {
					int i = -1;
					try {
						i = Integer.parseInt(id);
					} catch (Exception e) {
						err("Unexpected market rule id " + id);
						continue;
					}
					MarketRuleHandler.getMarketRule(i);
				}
			}
			JSONObject json = writeDetail("IBGateway:Contract:"+shortName, detail);
			KNOWN_CONTRACTS.put(shortName, ibc);
			KNOWN_CONTRACT_DETAILS.put(shortName, json);
//			if (ibc.exchange().equals( "SMART")) {
//				String fixShortName = shortName.replace("SMART", ibc.primaryExch());
//				log("Fix shortName: " + shortName + " -> " + fixShortName);
//				JSONObject json2 = writeDetail("IBGateway:Contract:" + fixShortName, detail);
//				KNOWN_CONTRACTS.put(fixShortName, ibc);
//				KNOWN_CONTRACT_DETAILS.put(fixShortName, json2);
//			}
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
		j.put("_timestamp", System.currentTimeMillis()); // Write generated timestamp to redis
		
//		if (detail.contract().secType() == SecType.OPT) {
//			OptionTopMktDataHandler handler = (OptionTopMktDataHandler) GW_CONTROLLER.findMktDataHandler(new IBContract(detail.contract()));
//			if (handler != null) log("opt lastPrice: " + handler.lastTickPrice);
//			else log(handler);
//		} else {
//			TopMktDataHandler handler = (TopMktDataHandler) GW_CONTROLLER.findMktDataHandler(new IBContract(detail.contract()));
//			if (handler != null) log("top lastPrice: " + handler.lastTickPrice);
//			else log(handler);
//		}
//		j.put("lastPrice", "");

		log(">>> Redis " + key);
		Redis.set(key, j);
		return j;
	}

	// direct copy writeDetail
	private JSONObject detailToJSONObject(ContractDetails detail) {
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
		j.put("_timestamp", System.currentTimeMillis()); // Write generated timestamp to redis

//		if (detail.contract().secType() == SecType.OPT) {
//			OptionTopMktDataHandler handler = (OptionTopMktDataHandler) GW_CONTROLLER.findMktDataHandler(new IBContract(detail.contract()));
//			if (handler != null) log("opt lastPrice: " + handler.lastTickPrice);
//		} else {
//			TopMktDataHandler handler = (TopMktDataHandler) GW_CONTROLLER.findMktDataHandler(new IBContract(detail.contract()));
//			if (handler != null) log("top lastPrice: " + handler.lastTickPrice);
//		}
//		j.put("lastPrice", "");
		return j;
	}

	public void setexDetailList(List<ContractDetails> list, Long id) {
		JSONArray array = new JSONArray();
		for (ContractDetails detail: list) {
			array.add( detailToJSONObject(detail));
		}
		//			JSONArray jsonArray = new JSONArray();
		//			jsonArray.add(details);
		//			log(jsonArray.toString());
		String key = "IBGateway:ReqIdContract:" + id;
		log("Redis -> " + key);
		Redis.setex(key, 30, JSON.toJSONString(array));
	}
}
