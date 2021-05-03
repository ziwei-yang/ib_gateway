package com.avalok.ib;

import static com.bitex.util.DebugUtil.*;

import java.util.Collection;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.avalok.ib.controller.BaseIBController;
import com.avalok.ib.handler.*;
import com.bitex.util.Redis;
import java.util.function.Consumer;

import com.ib.client.*;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

public class GatewayController extends BaseIBController {
	public static void main(String[] args) throws Exception {
		if (Redis.connectivityTest() == false)
			systemAbort("Seems redis does not work properlly");
		new GatewayController().listenCommand();
	}

	private final String ackChannel;
	public GatewayController() {
		ContractDetailsHandler.GW_CONTROLLER = this;
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				orderCacheHandler.teardownOMS("Shutting down");
			}
		});
		// Keep updating working status '[true, timestamp]' in 
		// Redis/IBGateway:{name}:status every second.
		// If this status goes wrong, all other data could not be trusted.
		long liveStatusInvertal = 1000;
		final String liveStatusKey = "IBGateway:" + _name + ":status";
		ackChannel = "IBGateway:"+_name+":ACK";
		new Timer("GatewayControllerLiveStatusWriter").scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				JSONObject j = new JSONObject();
				j.put("type", "heartbeat");
				j.put("status", isConnected());
				j.put("t", System.currentTimeMillis());
				Redis.set(liveStatusKey, j);
				Redis.pub(ackChannel, j);
			}
		}, 0, liveStatusInvertal);
	}

	////////////////////////////////////////////////////////////////
	// Market depth data module, ridiculous limitation here:
	// Max number (3) of market depth requests has been reached
	////////////////////////////////////////////////////////////////
	private ConcurrentHashMap<String, DeepMktDataHandler> _depthTasks = new ConcurrentHashMap<>();
	private ConcurrentHashMap<Integer, String> _depthTaskByReqID = new ConcurrentHashMap<>();
	private final boolean isSmartDepth = false;

	private int subscribeDepthData(IBContract contract) {
		String jobKey = contract.exchange() + "/" + contract.shownName();
		if (_depthTasks.get(jobKey) != null) {
			err("Task dulicated, skip subscribing depth data " + jobKey);
			return 0;
		}
		log("Subscribe depth data for " + jobKey);
		int numOfRows = 10;
		DeepMktDataHandler handler = new DeepMktDataHandler(contract, true);
		_apiController.reqDeepMktData(contract, numOfRows, isSmartDepth, handler);
		int qid = _apiController.lastReqId();
		_depthTaskByReqID.put(qid, jobKey); // reference for error msg
		_depthTasks.put(jobKey, handler);
		return qid;
	}

	private int unsubscribeDepthData(IBContract contract) {
		String jobKey = contract.exchange() + "/" + contract.shownName();
		DeepMktDataHandler handler = _depthTasks.get(jobKey);
		if (_depthTasks.get(jobKey) == null) {
			err("Task not exist, skip canceling depth data " + jobKey);
			return 0;
		}
		log("Cancel depth data for " + jobKey);
		_apiController.cancelDeepMktData(isSmartDepth, handler);
		int qid = _apiController.lastReqId();
		_depthTasks.remove(jobKey);
		return qid;
	}
	
	////////////////////////////////////////////////////////////////
	// Market top data module
	////////////////////////////////////////////////////////////////
	private ConcurrentHashMap<String, TopMktDataHandler> _topTasks = new ConcurrentHashMap<>();
	private ConcurrentHashMap<Integer, String> _topTaskByReqID = new ConcurrentHashMap<>();
	private int subscribeTopData(IBContract contract) {
		String jobKey = contract.exchange() + "/" + contract.shownName();
		if (_topTasks.get(jobKey) != null) {
			log("Task dulicated, skip subscribing top data " + jobKey);
			return 0;
		}
		log("Subscribe top data for " + jobKey);
		boolean broadcastTop = true, broadcastTick = true;
		TopMktDataHandler handler = new TopMktDataHandler(contract, broadcastTop, broadcastTick);
		// See <Generic tick required> at https://interactivebrokers.github.io/tws-api/tick_types.html
		String genericTickList = "";
		// Request snapshot, then updates
		boolean snapshot = true, regulatorySnapshot = true;
		_apiController.reqTopMktData(contract, genericTickList, snapshot, regulatorySnapshot, handler);
		snapshot = false; regulatorySnapshot = false;
		_apiController.reqTopMktData(contract, genericTickList, snapshot, regulatorySnapshot, handler);
		int qid = _apiController.lastReqId();
		_topTaskByReqID.put(qid, jobKey); // reference for error msg
		_topTasks.put(jobKey, handler);
		return qid;
	}

	private int unsubscribeTopData(IBContract contract) {
		String jobKey = contract.exchange() + "/" + contract.shownName();
		TopMktDataHandler handler = _topTasks.get(jobKey);
		if (_topTasks.get(jobKey) == null) {
			err("Task not exist, skip canceling top data " + jobKey);
			return 0;
		}
		log("Cancel top data for " + jobKey);
		_apiController.cancelTopMktData(handler);
		int qid = _apiController.lastReqId();
		_topTasks.remove(jobKey);
		return qid;
	}
	
	private void restartMarketData() {
		info("Re-subscribe all depth data");
		for (DeepMktDataHandler h : _depthTasks.values()) {
			unsubscribeDepthData(h.contract());
			subscribeDepthData(h.contract());
		}
		info("Re-subscribe all top data");
		for (TopMktDataHandler h : _topTasks.values()) {
			unsubscribeTopData(h.contract());
			subscribeTopData(h.contract());
		}
	}
	
	////////////////////////////////////////////////////////////////
	// Account balance
	////////////////////////////////////////////////////////////////
	// Use AccountMVHandler instead, PositionHandler does not have CASH balance.
//	protected PositionHandler posHandler = new PositionHandler();
//	protected void subscribeAccountPosition() { // TODO Is this streaming updating?
//		_apiController.reqPositionsMulti("", "", posHandler);
//	}
	
	protected AccountMVHandler accountMVHandler = new AccountMVHandler();
	protected void subscribeAccountMV() { // TODO Is this streaming updating?
		boolean subscribe = true;
		String account = "";
		_apiController.reqAccountUpdates(subscribe, account, accountMVHandler);
	}
	
	////////////////////////////////////////////////////////////////
	// Order & trades updates.
	////////////////////////////////////////////////////////////////
	protected AllOrderHandler orderCacheHandler = new AllOrderHandler(this);
	protected void subscribeTradeReport() {
		_apiController.reqExecutions(new ExecutionFilter(), orderCacheHandler);
	}
	protected void refreshLiveOrders() {
		_apiController.takeFutureTwsOrders(orderCacheHandler);
		_apiController.takeTwsOrders(orderCacheHandler);
		_apiController.reqLiveOrders(orderCacheHandler);
	}
	protected void refreshCompletedOrders() {
		_apiController.reqCompletedOrders(orderCacheHandler);
	}
	
	////////////////////////////////////////////////////////////////
	// Order actions
	////////////////////////////////////////////////////////////////
	protected int placeOrder(IBOrder order) {
		_apiController.placeOrModifyOrder(order.contract, order.order, new SingleOrderHandler(this, orderCacheHandler, order));
		return _apiController.lastReqId();
	}
	protected int cancelOrder(int orderId) {
		_apiController.cancelOrder(orderId);
		return _apiController.lastReqId();
	}
	protected int cancelAll() {
		_apiController.cancelAllOrders();
		return _apiController.lastReqId();
	}
	
	////////////////////////////////////////////////////////////////
	// Contract details query.
	////////////////////////////////////////////////////////////////
	public int queryContractListWithCache(JSONObject contractWithLimitInfo) {
		IBContract contract = new IBContract(contractWithLimitInfo);
		// Query this in cache, would trigger reqContractDetails() if cache missed.
		JSONObject details = ContractDetailsHandler.findDetails(contract);
		if (details != null) // Cache hit.
			return 0;
		return _apiController.lastReqId();
	}
	public int queryContractList(IBContract ibc) {
		_apiController.reqContractDetails(ibc, new ContractDetailsHandler());
		return _apiController.lastReqId();
	}

	////////////////////////////////////////////////////////////////
	// Life cycle and command processing
	////////////////////////////////////////////////////////////////
	@Override
	protected void _postConnected() {
		log("_postConnected");
		// Reset every cache status.
		// Contract detail cache does not need to be reset, always not changed.
		orderCacheHandler.resetStatus();
		
		// Then subscribe data.
		subscribeAccountMV();
		refreshLiveOrders();
		refreshCompletedOrders();
		subscribeTradeReport();
		restartMarketData();
	}

	@Override
	protected void _postDisconnected() {
		log("_postDisconnected");
		orderCacheHandler.teardownOMS("_postDisconnected()");
		orderCacheHandler.resetStatus();
	}

	private JedisPubSub commandProcessJedisPubSub = new JedisPubSub() {
		public void onMessage(String channel, String message) {
			JSONObject j = null;
			try {
				j = JSON.parseObject(message);
			} catch (Exception e) {
				err("<<< CMD " + message);
				err("Failed to parse command " + e.getMessage());
				return;
			}
			final Long id = j.getLong("id");
			info("<<< CMD " + id + " " + j.getString("cmd"));
			String errorMsg = null;
			int apiReqId = 0;
			try {
				switch(j.getString("cmd")) {
				case "SUB_ODBK":
					apiReqId = subscribeDepthData(new IBContract(j.getJSONObject("contract")));
					break;
				case "SUB_TOP":
					apiReqId = subscribeTopData(new IBContract(j.getJSONObject("contract")));
					break;
				case "RESET":
					_postConnected();
					break;
				case "FIND_CONTRACTS":
					apiReqId = queryContractListWithCache(j.getJSONObject("contract"));
					break;
				case "PLACE_ORDER":
					apiReqId = placeOrder(new IBOrder(j.getJSONObject("iborder")));
					break;
				case "CANCEL_ORDER":
					apiReqId = cancelOrder(j.getInteger("apiOrderId"));
					break;
				case "CANCEL_ALL":
					apiReqId = cancelAll();
					break;
				default:
					errorMsg = "Unknown cmd " + j.getString("cmd");
					err(errorMsg);
					break;
				}
			} catch (Exception e) {
				errorMsg = e.getMessage();
				e.printStackTrace();
			} finally { // Reply with id in boradcasting.
				info(">>> ACK " + id + " ibApiId " + apiReqId);
				JSONObject r = new JSONObject();
				r.put("type", "ack");
				r.put("reqId", id);
				r.put("ibApiId", apiReqId);
				if (errorMsg != null)
					r.put("err", errorMsg);
				Redis.pub(ackChannel, r);
			}
		}
	};

	private void listenCommand() throws Exception {
		while (true) {
			Redis.exec(new Consumer<Jedis>() {
				@Override
				public void accept(Jedis t) {
					String cmdChannel = "IBGateway:"+_name+":CMD";
					info("Command listening started at " + cmdChannel);
					t.subscribe(commandProcessJedisPubSub, cmdChannel);
				}
			});
			err("Restart command listening in 1 second");
			sleep(1000);
		}
	}

	////////////////////////////////////////////////////////////////
	// TWS Message processing
	////////////////////////////////////////////////////////////////
	@Override
	public void message(int id, int errorCode, String errorMsg) {
		log("id:" + id + ", code:" + errorCode + ", msg:" + errorMsg);
		JSONObject j = new JSONObject();
		j.put("type", "msg");
		j.put("ibApiId", id);
		j.put("code", errorCode);
		j.put("msg", errorMsg);
		switch (errorCode) {
		case 200: // No security/exchange has been found for the request
			Redis.pub(ackChannel, j);
			break;
		case 309: // Max number (3) of market depth requests has been reached
			String depthJobKey = _depthTaskByReqID.remove(id);
			if (depthJobKey != null) {
				err("Remove failed depth task " + depthJobKey);
				_depthTasks.remove(depthJobKey);
				j.put("type", "error");
				j.put("msg", "depth req failed " + depthJobKey);
				Redis.pub(ackChannel, j);
				break;
			}
			Redis.pub(ackChannel, j);
			break;
		case 317: // Market depth data has been RESET. Please empty deep book contents before applying any new entries.
			log("Initialise all data jobs again.");
			_postConnected();
			break;
		case 2103: // Market data farm connection is broken
			log("Initialise all data jobs again.");
			_postConnected();
			break;
		case 2105: // HMDS data farm connection is broken
			log("Initialise all data jobs again.");
			_postConnected();
			break;
		case 2108: // Market data farm connection is inactive but should be available upon demand
			log("Initialise all data jobs again.");
			_postConnected();
			break;
		default:
			super.message(id, errorCode, errorMsg);
			Redis.pub(ackChannel, j);
			break;
		}
	}
}
