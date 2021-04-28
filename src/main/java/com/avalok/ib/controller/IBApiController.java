package com.avalok.ib.controller;

import static com.bitex.util.DebugUtil.*;

import java.util.concurrent.ConcurrentLinkedQueue;

import com.avalok.ib.IBContract;
import com.ib.client.*;
import com.ib.controller.*;
import com.ib.controller.ApiConnection.*;

/**
 * A warpper of ApiController for rate control and other proxy.
 */
public class IBApiController extends ApiController {
	public IBApiController(IConnectionHandler handler, ILogger inLogger, ILogger outLogger) {
		super(handler, inLogger, outLogger);
	}

	////////////////////////////////////////////////////////////////
	// API Operation history.
	////////////////////////////////////////////////////////////////
	protected final ConcurrentLinkedQueue<String> _opRecs = new ConcurrentLinkedQueue<>();
	protected static final int OPERATION_HISTORY_MAX = 5;
	
	protected void recordOperationHistory(String his) {
		info("--> " + his);
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
	public void reqAccountUpdates(boolean subscribe, String acctCode, IAccountHandler handler) {
		twsAPIRateControl();
		recordOperationHistory("reqAccountUpdates:" + acctCode);
		super.reqAccountUpdates(subscribe, acctCode, handler);
	}
	public void reqPositions(IPositionHandler handler) {
		twsAPIRateControl();
		recordOperationHistory("reqPositions");
		super.reqPositions(handler);
	}
	public void cancelPositions(IPositionHandler handler) {
		twsAPIRateControl();
		recordOperationHistory("cancelPositions");
		super.cancelPositions(handler);
	}
//	public void reqTopMktData(NewContract contract, String genericTickList, boolean snapshot, ITopMktDataHandler handler) {
//		twsAPIRateControl();
//		recordOperationHistory("reqTopMktData:" + JSON.toJSONString(contract));
//		super.reqTopMktData(contract, genericTickList, snapshot, handler);
//	}
	public void cancelTopMktData(ITopMktDataHandler handler) {
		twsAPIRateControl();
		recordOperationHistory("cancelTopMktData");
		super.cancelTopMktData(handler);
	}
	public void reqDeepMktData(IBContract contract, int numRows, boolean isSmartDepth, IDeepMktDataHandler handler) {
		twsAPIRateControl();
		recordOperationHistory("reqDeepMktData:" + contract.shownName());
		super.reqDeepMktData(contract, numRows, isSmartDepth, handler);
	}
	public void cancelDeepMktData(boolean isSmartDepth, IDeepMktDataHandler handler) {
		twsAPIRateControl();
		recordOperationHistory("cancelDeepMktData");
		super.cancelDeepMktData(isSmartDepth, handler);
	}
	public void reqExecutions(ExecutionFilter filter, ITradeReportHandler handler) {
		twsAPIRateControl();
		recordOperationHistory("reqExecutions");
		super.reqExecutions(filter, handler);
	}
	public void reqLiveOrders(ILiveOrderHandler handler) {
		twsAPIRateControl();
		recordOperationHistory("reqLiveOrders");
		super.reqLiveOrders(handler);
	}
	public void takeTwsOrders(ILiveOrderHandler handler) {
		twsAPIRateControl();
		recordOperationHistory("takeTwsOrders");
		super.takeTwsOrders(handler);
	}
	public void takeFutureTwsOrders(ILiveOrderHandler handler) {
		twsAPIRateControl();
		recordOperationHistory("takeFutureTwsOrders");
		super.takeFutureTwsOrders(handler);
	}
	public void removeLiveOrderHandler(ILiveOrderHandler handler) {
		twsAPIRateControl();
		recordOperationHistory("removeLiveOrderHandler");
		super.removeLiveOrderHandler(handler);
	}
//	public void reqHistoricalData(NewContract contract, String endDateTime, int duration, DurationUnit durationUnit, BarSize barSize, WhatToShow whatToShow, boolean rthOnly, IHistoricalDataHandler handler) {
//		twsAPIRateControl();
//		recordOperationHistory("reqHistoricalData:" + JSON.toJSONString(contract));
//		super.reqHistoricalData(contract, endDateTime, duration, durationUnit, barSize, whatToShow, rthOnly, handler);
//	}
	public void cancelHistoricalData(IHistoricalDataHandler handler) {
		twsAPIRateControl();
		recordOperationHistory("cancelHistoricalData");
		super.cancelHistoricalData(handler);
	}
//	public void reqRealTimeBars(NewContract contract, WhatToShow whatToShow, boolean rthOnly, IRealTimeBarHandler handler) {
//		twsAPIRateControl();
//		recordOperationHistory("reqRealTimeBars:" + JSON.toJSONString(contract));
//		super.reqRealTimeBars(contract, whatToShow, rthOnly, handler);
//	}
	public void cancelRealtimeBars(IRealTimeBarHandler handler) {
		twsAPIRateControl();
		recordOperationHistory("cancelRealtimeBars");
		super.cancelRealtimeBars(handler);
	}
}
