package com.avalok.ib.handler;

import apidemo.AccountSummaryPanel;
import com.alibaba.fastjson.JSONObject;
import com.ib.controller.AccountSummaryTag;
import com.ib.controller.ApiController.IAccountSummaryHandler;

import java.util.HashMap;
import java.util.Map;

import static com.bitex.util.DebugUtil.*;
import com.bitex.util.Redis;
import static com.ib.controller.AccountSummaryTag.AccountType;

public class AccountSummaryHandler implements IAccountSummaryHandler{
    Map<String, Map<String, String>> m_map = new HashMap<>();

    @Override
    public void accountSummary(String account, AccountSummaryTag tag, String value, String currency) {
        Map<String, String> summary = m_map.get(account);
        if (summary == null) summary = new HashMap<>();

        switch (tag){
            case AccountType:
                summary.put("AccountType", value);
//                m_map.get(account).put("AccountType", value);
                break;
            case NetLiquidation:
                summary.put("NetLiquidation", value);
//                m_map.get(account).put("NetLiquidation", value);
                break;
            case TotalCashValue:
                summary.put("TotalCashValue", value);
//                m_map.get(account).put("TotalCashValue", value);
                break;
            case SettledCash:
                summary.put("SettledCash", value);
//                m_map.get(account).put("SettledCash", value);
                break;
            case AccruedCash:
                summary.put("AccruedCash", value);
//                m_map.get(account).put("AccruedCash", value);
                break;
            case BuyingPower:
                summary.put("BuyingPower", value);
//                m_map.get(account).put("BuyingPower", value);
                break;
            case EquityWithLoanValue:
                summary.put("EquityWithLoanValue", value);
//                m_map.get(account).put("EquityWithLoanValue", value);
                break;
            case RegTEquity:
                summary.put("RegTEquity", value);
//                m_map.get(account).put("RegTEquity", value);
                break;
            case RegTMargin:
                summary.put("RegTMargin", value);
//                m_map.get(account).put("RegTMargin", value);
                break;
            case InitMarginReq:
                summary.put("InitMarginReq", value);
//                m_map.get(account).put("InitMarginReq", value);
                break;
            case MaintMarginReq:
                summary.put("MaintMarginReq", value);
//                m_map.get(account).put("MaintMarginReq", value);
                break;
            case ExcessLiquidity:
                summary.put("ExcessLiquidity", value);
//                m_map.get(account).put("ExcessLiquidity", value);
                break;
            case Cushion:
                summary.put("Cushion", value);
//                m_map.get(account).put("Cushion", value);
                break;
            case LookAheadInitMarginReq:
                summary.put("LookAheadInitMarginReq", value);
//                m_map.get(account).put("LookAheadInitMarginReq", value);
                break;
            case LookAheadMaintMarginReq:
                summary.put("LookAheadMaintMarginReq", value);
//                m_map.get(account).put("LookAheadMaintMarginReq", value);
                break;
            case LookAheadAvailableFunds:
                summary.put("LookAheadAvailableFunds", value);
//                m_map.get(account).put("LookAheadAvailableFunds", value);
                break;
            case LookAheadExcessLiquidity:
                summary.put("LookAheadExcessLiquidity", value);
//                m_map.get(account).put("LookAheadExcessLiquidity", value);
                break;
            case Leverage:
                summary.put("Leverage", value);
//                m_map.get(account).put("Leverage", value);
                break;
        }
        m_map.put(account, summary);
//        log("account: "+account+" tag: "+tag + " value: " + value + " currency: "+currency);

    }

    @Override
    public void accountSummaryEnd() {
        JSONObject j = new JSONObject();
        m_map.forEach((account,v)-> {
            String key = "IBGateway:Summary:" + account;
            j.put("data", v);
            j.put("updateTime", System.currentTimeMillis());
            Redis.set(key, j.toJSONString());
            log("Redis -> " + key);
        });


    }
}
