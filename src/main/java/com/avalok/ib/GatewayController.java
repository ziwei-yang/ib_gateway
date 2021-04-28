package com.avalok.ib;

import static com.bitex.util.DebugUtil.*;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.avalok.ib.controller.BaseIBController;
import com.avalok.ib.handler.DeepMktDataHandler;
import com.bitex.util.Redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

public class GatewayController extends BaseIBController {
	public static void main(String[] args) throws Exception {
		new GatewayController().listenCommand();
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

	protected void _subscribeExample() {
		try {
//			subscribeMarketData(new IBContract("TSE", "USD-BTCC.U"));
//			subscribeMarketData(new IBContract("ICECRYPTO", "USD-BAKKT@202105"));
//			subscribeMarketData(new IBContract("CMECRYPTO", "USD-BRR@202105@5"));
		} catch (Exception e) {
			log("Failed to init IBContract " + e.getMessage());
			return;
		}
	}

	////////////////////////////////////////////////////////////////
	// Life cycle and command processing
	////////////////////////////////////////////////////////////////
	@Override
	protected void _postConnected() {
		log("_postConnected");
		restartMarketData();
	}

	private JedisPubSub commandProcessJedisPubSub = new JedisPubSub() {
		public void onMessage(String channel, String message) {
			info("<-- " + message);
			JSONObject j = null;
			try {
				j = JSON.parseObject(message);
				switch(j.getString("cmd")) {
				case "SUB_ODBK":
					subscribeMarketData(j.getString("exchange"), j.getString("shownName"));
					break;
				case "SUB_ODBK_RESTART":
					restartMarketData();
					break;
				default:
					err("Unknown cmd " + j.getString("cmd"));
					return;
				}
			} catch (Exception e) {
				err("Failed to parse command " + e.getMessage());
				return;
			}
		}
	};

	private void listenCommand() throws Exception {
		while (true) {
			Redis.exec(new Consumer<Jedis>() {
				@Override
				public void accept(Jedis t) {
					String cmdChannel = "URANUS:IBGateway:"+_name+":CMD";
					log("Command listening started at " + cmdChannel);
					t.subscribe(commandProcessJedisPubSub, cmdChannel);
				}
			});
			log("Restart command listening in 1 second");
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
