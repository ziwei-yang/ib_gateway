package com.avalok.ib.controller;

import static com.bitex.util.DebugUtil.*;

import java.util.concurrent.ConcurrentLinkedQueue;

import com.avalok.ib.IBContract;
import com.avalok.ib.handler.ContractDetailsHandler;
import com.ib.client.*;
import com.ib.controller.*;
import com.ib.controller.ApiConnection.*;
import com.ib.controller.ApiController.IAccountHandler;
import com.ib.controller.ApiController.ICompletedOrdersHandler;
import com.ib.controller.ApiController.IConnectionHandler;
import com.ib.controller.ApiController.IContractDetailsHandler;
import com.ib.controller.ApiController.IDeepMktDataHandler;
import com.ib.controller.ApiController.IHistoricalDataHandler;
import com.ib.controller.ApiController.ILiveOrderHandler;
import com.ib.controller.ApiController.IMarketRuleHandler;
import com.ib.controller.ApiController.IOptHandler;
import com.ib.controller.ApiController.IOrderHandler;
import com.ib.controller.ApiController.IPositionHandler;
import com.ib.controller.ApiController.IRealTimeBarHandler;
import com.ib.controller.ApiController.ITopMktDataHandler;
import com.ib.controller.ApiController.ITradeReportHandler;

/**
 * A warpper of ApiController for rate control and other proxy.
 */
public class IBApiController {
	private ApiController _api;
	public IBApiController(IConnectionHandler handler, ILogger inLogger, ILogger outLogger) {
		_api = new ApiController(handler, inLogger, outLogger);
	}
	public int lastReqId() { return _api.m_reqId - 1; }
	public int nextReqId() { return _api.m_reqId; }

	////////////////////////////////////////////////////////////////
	// API Operation history.
	////////////////////////////////////////////////////////////////
	protected final ConcurrentLinkedQueue<String> _opRecs = new ConcurrentLinkedQueue<>();
	protected static final int OPERATION_HISTORY_MAX = 5;

	protected void recordOperationHistory(String his) {
		info("--> [" + nextReqId() + "] " + his);
		_opRecs.add(his);
		if (_opRecs.size() > OPERATION_HISTORY_MAX)
			_opRecs.poll();
	}

	public String[] lastOperationHistory() {
		String[] ret = new String[OPERATION_HISTORY_MAX];
		return _opRecs.toArray(ret);
	}

	////////////////////////////////////////////////////////////////
	// TWS API Rate controller
	////////////////////////////////////////////////////////////////
	protected static final int MAX_IB_API_RATE = 48; // TODO Where is limitation explanation?
	protected final ConcurrentLinkedQueue<Long> _apiRecs = new ConcurrentLinkedQueue<>();

	protected void twsAPIRateControl() {
		synchronized(_apiRecs) {
			if (_apiRecs.size() >= MAX_IB_API_RATE) {
				long oldestTime = _apiRecs.poll();
				while (_apiRecs.size() >= MAX_IB_API_RATE)
					_apiRecs.poll();
				// Wait until 1s after the oldestTime
				long waitTime = 1000 - (System.currentTimeMillis() - oldestTime);
				if (waitTime > 0) {
					log("TWS api rate reached: " + MAX_IB_API_RATE + "/s, halt for " + waitTime + "ms");
					sleep(waitTime);
				}
			}
			_apiRecs.add(System.currentTimeMillis());
		}
	}

	////////////////////////////////////////////////////////////////
	// Overwrite original API methods.
	////////////////////////////////////////////////////////////////
	public void connect( String host, int port, int clientId, String connectionOpts ) {
		recordOperationHistory("connect");
		_api.connect(host, port, clientId, connectionOpts);
	}
	public void disconnect() {
		recordOperationHistory("disconnect");
		_api.disconnect();
	}
	public void reqMarketRule(int marketRuleId, IMarketRuleHandler handler) {
		twsAPIRateControl();
		recordOperationHistory("marketRule:" + marketRuleId);
		_api.reqMarketRule(marketRuleId, handler);
	}
	public void reqAccountUpdates(boolean subscribe, String acctCode, IAccountHandler handler) {
		twsAPIRateControl();
		recordOperationHistory("reqAccountUpdates:" + acctCode);
		_api.reqAccountUpdates(subscribe, acctCode, handler);
	}
	public void reqPositions(IPositionHandler handler) {
		twsAPIRateControl();
		recordOperationHistory("reqPositions");
		_api.reqPositions(handler);
	}
	public void cancelPositions(IPositionHandler handler) {
		twsAPIRateControl();
		recordOperationHistory("cancelPositions");
		_api.cancelPositions(handler);
	}
	public void reqDeepMktData(IBContract contract, int numRows, boolean isSmartDepth, IDeepMktDataHandler handler) {
		twsAPIRateControl();
		recordOperationHistory("reqDeepMktData:" + contract.shownName());
		_api.reqDeepMktData(contract, numRows, isSmartDepth, handler);
	}
	public void cancelDeepMktData(boolean isSmartDepth, IDeepMktDataHandler handler) {
		twsAPIRateControl();
		recordOperationHistory("cancelDeepMktData");
		_api.cancelDeepMktData(isSmartDepth, handler);
	}
    public void reqTopMktData(Contract contract, String genericTickList, boolean snapshot, boolean regulatorySnapshot, ITopMktDataHandler handler) {
		twsAPIRateControl();
		recordOperationHistory("reqTopMktData");
		_api.reqTopMktData(contract, genericTickList, snapshot, regulatorySnapshot, handler);
    }
    public void reqOptionMktData(Contract contract, String genericTickList, boolean snapshot, boolean regulatorySnapshot, IOptHandler handler) {
		twsAPIRateControl();
		recordOperationHistory("reqOptionMktData");
		_api.reqOptionMktData(contract, genericTickList, snapshot, regulatorySnapshot, handler);
    }
    public void cancelTopMktData( ITopMktDataHandler handler) {
		twsAPIRateControl();
		recordOperationHistory("cancelTopMktData");
		_api.cancelTopMktData(handler);
    }
	public void reqExecutions(ExecutionFilter filter, ITradeReportHandler handler) {
		twsAPIRateControl();
		recordOperationHistory("reqExecutions");
		_api.reqExecutions(filter, handler);
	}
	public void reqLiveOrders(ILiveOrderHandler handler) {
		twsAPIRateControl();
		recordOperationHistory("reqLiveOrders");
		_api.reqLiveOrders(handler);
	}
	public void takeTwsOrders(ILiveOrderHandler handler) {
		twsAPIRateControl();
		recordOperationHistory("takeTwsOrders");
		_api.takeTwsOrders(handler);
	}
	public void takeFutureTwsOrders(ILiveOrderHandler handler) {
		twsAPIRateControl();
		recordOperationHistory("takeFutureTwsOrders");
		_api.takeFutureTwsOrders(handler);
	}
	public void removeLiveOrderHandler(ILiveOrderHandler handler) {
		twsAPIRateControl();
		recordOperationHistory("removeLiveOrderHandler");
		_api.removeLiveOrderHandler(handler);
	}
//	public void reqHistoricalData(NewContract contract, String endDateTime, int duration, DurationUnit durationUnit, BarSize barSize, WhatToShow whatToShow, boolean rthOnly, IHistoricalDataHandler handler) {
//		twsAPIRateControl();
//		recordOperationHistory("reqHistoricalData:" + JSON.toJSONString(contract));
//		_api.reqHistoricalData(contract, endDateTime, duration, durationUnit, barSize, whatToShow, rthOnly, handler);
//	}
	public void cancelHistoricalData(IHistoricalDataHandler handler) {
		twsAPIRateControl();
		recordOperationHistory("cancelHistoricalData");
		_api.cancelHistoricalData(handler);
	}
//	public void reqRealTimeBars(NewContract contract, WhatToShow whatToShow, boolean rthOnly, IRealTimeBarHandler handler) {
//		twsAPIRateControl();
//		recordOperationHistory("reqRealTimeBars:" + JSON.toJSONString(contract));
//		_api.reqRealTimeBars(contract, whatToShow, rthOnly, handler);
//	}
	public void cancelRealtimeBars(IRealTimeBarHandler handler) {
		twsAPIRateControl();
		recordOperationHistory("cancelRealtimeBars");
		_api.cancelRealtimeBars(handler);
	}
	public void reqCompletedOrders(ICompletedOrdersHandler handler) {
		twsAPIRateControl();
		recordOperationHistory("reqCompletedOrders");
		_api.reqCompletedOrders(handler);
	}
	public void placeOrModifyOrder(Contract contract, final Order order, final IOrderHandler handler) {
		twsAPIRateControl();
		recordOperationHistory("placeOrModifyOrder");
		_api.placeOrModifyOrder(contract, order, handler);
	}
	public void cancelOrder(int orderId) {
		twsAPIRateControl();
		recordOperationHistory("cancelOrder " + orderId);
		_api.cancelOrder(orderId);
	}
	public void cancelAllOrders() {
		twsAPIRateControl();
		recordOperationHistory("cancelAllOrders");
		_api.cancelAllOrders();
	}
	public void reqContractDetails( Contract contract, final IContractDetailsHandler processor) {
		twsAPIRateControl();
		recordOperationHistory("reqContractDetails");
		_api.reqContractDetails(contract, processor);
	}
	public void reqContractDetailsToRedis(Contract contract, final ContractDetailsHandler processor, Long id) {
		twsAPIRateControl();
		recordOperationHistory("reqContractDetailsToRedis");
		_api.reqContractDetailsToRedis(contract, processor, id);
	}
}
