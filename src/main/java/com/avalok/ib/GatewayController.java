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

import com.ib.client.*;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

public class GatewayController extends BaseIBController {
	public static void main(String[] args) throws Exception {
		new GatewayController().listenCommand();
	}
	
	public GatewayController() {
		ContractDetailsHandler.GW_CONTROLLER = this;
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				err("!!!! Shutdown signal received");
			}
		});
		// Keep updating working status '[true, timestamp]' in 
		// Redis/IBGateway:{name}:status every second.
		// If this status goes wrong, all other data could not be trusted.
		long liveStatusInvertal = 1000;
		final String liveStatusKey = "IBGateway:" + _name + ":status";
		new Timer("GatewayControllerLiveStatusWriter").scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				Redis.exec(new Consumer<Jedis>() {
					@Override
					public void accept(Jedis t) {
						String liveStatusData = "[" + isConnected() + "," + System.currentTimeMillis() + "]";
						// log("--> Write " + liveStatusKey + " " + liveStatusData);
						t.set(liveStatusKey, liveStatusData);
					}
				});
			}
		}, 0, liveStatusInvertal);
	}

	////////////////////////////////////////////////////////////////
	// Market data module
	////////////////////////////////////////////////////////////////
	private ConcurrentHashMap<String, DeepMktDataHandler> _depthTasks = new ConcurrentHashMap<>();
	private void subscribeMarketData(String exchange, String shownName) {
		try {
			subscribeMarketData(new IBContract(exchange, shownName));
		} catch (Exception e) {
			log("Failed to subscribeMarketData " + exchange + " " + shownName + "\n" + e.getMessage());
			return;
		}
	}

	private void subscribeMarketData(IBContract contract) {
		String jobKey = contract.exchange() + "/" + contract.shownName();
		if (_depthTasks.get(jobKey) != null) {
			err("Task dulicated, abort subscribing depth data for " + jobKey);
			return;
		}
		log("Subscribe depth data for " + jobKey);
		int numOfRows = 10;
		boolean isSmartDepth = false;
		DeepMktDataHandler handler = new DeepMktDataHandler(contract, true);
		_apiController.reqDeepMktData(contract, numOfRows, isSmartDepth, handler);
		_depthTasks.put(jobKey, handler);
	}

	private void unsubscribeMarketData(IBContract contract) {
		String jobKey = contract.exchange() + "/" + contract.shownName();
		DeepMktDataHandler handler = _depthTasks.get(jobKey);
		if (handler == null) {
			err("Task not exist, abort unsubscribing depth data for " + jobKey);
			return;
		}
		log("Unsubscribe depth data for " + jobKey);
		boolean isSmartDepth = false;
		_apiController.cancelDeepMktData(isSmartDepth, null);
		_depthTasks.remove(jobKey);
	}
	
	private void restartMarketData() {
		info("Re-subscribe all odbk data");
		Collection<DeepMktDataHandler> handlers = _depthTasks.values();
		for (DeepMktDataHandler h : handlers) {
			unsubscribeMarketData(h.contract());
			subscribeMarketData(h.contract());
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
	// Order & trades
	////////////////////////////////////////////////////////////////
	protected AllOrderHandler orderCacheHandler = new AllOrderHandler();
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
	protected void cancelOrder(int orderId) {
		_apiController.cancelOrder(orderId);
	}
	protected void cancelAll() {
		_apiController.cancelAllOrders();
	}
	
	////////////////////////////////////////////////////////////////
	// Contract details query.
	////////////////////////////////////////////////////////////////
	public void queryContractList(IBContract contractWithLimitInfo) {
		_apiController.reqContractDetails(contractWithLimitInfo, new ContractDetailsHandler());
	}

	////////////////////////////////////////////////////////////////
	// Life cycle and command processing
	////////////////////////////////////////////////////////////////
	@Override
	protected void _postConnected() {
		log("_postConnected");
		subscribeAccountMV();
		refreshLiveOrders();
		refreshCompletedOrders();
		subscribeTradeReport();
		restartMarketData();
	}

	private JedisPubSub commandProcessJedisPubSub = new JedisPubSub() {
		public void onMessage(String channel, String message) {
			info("<-- " + message);
			JSONObject j = null;
			try {
				j = JSON.parseObject(message);
			} catch (Exception e) {
				err("Failed to parse command " + e.getMessage());
				return;
			}
			final Integer id = j.getInteger("id");
			boolean success = true;
			try {
				switch(j.getString("cmd")) {
				case "SUB_ODBK":
					subscribeMarketData(j.getString("exchange"), j.getString("shownName"));
					break;
				case "RESET":
					_postConnected();
					break;
				case "FIND_CONTRACTS":
					queryContractList(new IBContract(j.getJSONObject("params")));
					break;
				default:
					success = false;
					err("Unknown cmd " + j.getString("cmd"));
					break;
				}
			} catch (Exception e) {
				success = false;
				e.printStackTrace();
			} finally { // Reply with id in boradcasting.
				final boolean replySuccess = success;
				Redis.exec(new Consumer<Jedis>() {
					@Override
					public void accept(Jedis t) {
						String replyChannel = "IBGateway:"+_name+":ACK";
						t.publish(replyChannel, "["+id+","+replySuccess+"]");
					}
				});
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
		switch (errorCode) {
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
				break;
		}
	}
}
