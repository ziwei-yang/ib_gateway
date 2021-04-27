package com.avalok.ib;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.ib.controller.*;
import com.ib.controller.ApiController.*;

import com.alibaba.fastjson.*;

import static com.bitex.util.DebugUtil.*;

/**
 * All TWS Controller classes are based on this.
 */
public abstract class BaseIBController implements IConnectionHandler {
	protected IBApiController _apiController;

	//////////////////////////////////////////////////////
	// Callback methods, with active handler detection.
	//////////////////////////////////////////////////////
	protected IConnectionHandler activeIBConnectionHandler;
	protected IConnectionHandler assignNewIConnectionHandler() {
		IConnectionHandler handler = new IConnectionHandler() {
			public void connected() {
				if (this != activeIBConnectionHandler) {
					log("Ignore obsolete IBConnectionHandler event: connected");
					return;
				}
				BaseIBController.this.connected();
			}
			public void disconnected() {
				if (this != activeIBConnectionHandler) {
					log("Ignore obsolete IBConnectionHandler event: disconnected");
					return;
				}
				BaseIBController.this.disconnected();
			}
			public void accountList(List<String> list) {
				if (this != activeIBConnectionHandler) {
					log("Ignore obsolete IBConnectionHandler event: accountList");
					return;
				}
				BaseIBController.this.accountList(list);
			}
			public void error(Exception e) {
				if (this != activeIBConnectionHandler) {
					log("Ignore obsolete IBConnectionHandler event: error");
					return;
				}
				BaseIBController.this.error(e);
			}
			public void message(int id, int errorCode, String errorMsg) {
				if (this != activeIBConnectionHandler) {
					log("Ignore obsolete IBConnectionHandler event: message");
					return;
				}
				BaseIBController.this.message(id, errorCode, errorMsg);
			}
			public void show(String string) {
				if (this != activeIBConnectionHandler) {
					log("Ignore obsolete IBConnectionHandler event: show");
					return;
				}
				BaseIBController.this.show(string);
			}
		};
		activeIBConnectionHandler = handler;
		return handler;
	}

	protected String name = "unamed";
	public String name(){ return name; }
	
	//////////////////////////////////////////////////////
	// Cache historical messages.
	//////////////////////////////////////////////////////
	protected final Queue<JSONObject> _ib_messages = new ConcurrentLinkedQueue<>();
	protected final int MAX_HIS_MSG = 1000;
	protected long connectedTimestamp = 0;
	protected long initConnectTimestamp = 0;
	public BaseIBController() {
		for (int i = 0; i < MAX_HIS_MSG; i++)
			_ib_messages.add(new JSONObject());
		initConnectTimestamp = System.currentTimeMillis();
		connect();
	}

	protected JSONObject recordMessage(int orderID, int errorCode, String errorMsg) {
		JSONObject msg = new JSONObject();
		msg.put("timestamp", System.currentTimeMillis());
		msg.put("order_id", orderID);
		msg.put("error_code", errorCode);
		msg.put("error_msg", errorMsg);
		_ib_messages.add(msg);
		// Keep size of message history.
		_ib_messages.poll();
		return msg;
	}

	public JSONObject[] messageHistory(int size) {
		if (size <= 0) return new JSONObject[0];
		int from = MAX_HIS_MSG - size;
		if (from < 0) from = 0;
		int to = MAX_HIS_MSG;
		Object[] msgs = Arrays.copyOfRange(_ib_messages.toArray(), from, to);
		JSONObject[] results = new JSONObject[size];
		for (int i = 0; i < size; i++)
			results[i] = (JSONObject)(msgs[size - 1 - i]);
		return results;
	}

	//////////////////////////////////////////////////////
	// Initialise ENV and connect() in background thread
	//////////////////////////////////////////////////////
	public final static String TWS_API_ADDR = System.getenv("TWS_API_ADDR");
	public final static int TWS_API_PORT = Integer.parseInt(System.getenv("TWS_API_PORT"));
	public final static String TWS_NAME = TWS_API_ADDR + "_" + TWS_API_PORT;
	protected int _ibGatewayID = Integer.parseInt(System.getenv("TWS_API_CLIENTID"));
	protected synchronized void connect() {
		// DebugUtil.printStackInfo();
		if (initConnectTimestamp <= 0) {
			log("initConnectTimestamp <= 0, abort connect()");
			return;
		}
		new Thread(new Runnable() {
			public void run() {
				int retry_ct = 0;
				log("Connect thread started.");
				while (true) {
					try {
						log("Connecting gateway " + TWS_API_ADDR + " ID " + _ibGatewayID);
						// TODO this step might hang.
						IBApiController newController = new IBApiController(assignNewIConnectionHandler(), new NullIBLogger(), new NullIBLogger());
				        // make initial connection to local host, port 7496, client id 0, no connection options
						newController.connect(TWS_API_ADDR, TWS_API_PORT, _ibGatewayID, null);
						_apiController = newController;  // Only assign after connect()
						connectedTimestamp = System.currentTimeMillis();
						log("Gateway connected with client ID " + _ibGatewayID);
						break;
					} catch (StackOverflowError e) {
						log("StackOverflowError in connecting gateway with ID " + _ibGatewayID + " retry_ct:" + retry_ct);
					} catch (Exception e) {
						log("Error in connecting gateway with ID " + _ibGatewayID + " retry_ct:" + retry_ct);
						log(e);
					} finally {
						retry_ct += 1;
						_ibGatewayID++;
						sleep(50);
					}
				}
				log("Connect thread finished.");
			}
		}).start();
	}

	//////////////////////////////////////////////////////
	// Connectivity & Status Set/Get functions
	//////////////////////////////////////////////////////
	protected boolean _api_connected = false, _tws_server_connected = false;

	protected void markTWSDisconnect() {
		_tws_server_connected = false;
		log("twsDisconnect() _api_connected:" + _api_connected + " _tws_server_connected:" + _tws_server_connected);
		// DebugUtil.printStackInfo();
	}
	protected void markTWSConnect(boolean dataLost) {
		_api_connected = true;
		_tws_server_connected = true;
		log("twsConnect() dataLost:" + dataLost + " _api_connected:" + _api_connected + " _tws_server_connected:" + _tws_server_connected);
		// DebugUtil.printStackInfo();
	}
	public boolean checkStatus(){
		return _api_connected && _tws_server_connected;
	}

	////////////////////////////////////////////////////////////////
	// IConnectionHandler <-> TWS connectivity
	////////////////////////////////////////////////////////////////
	public boolean isConnected() {
		return _api_connected && _tws_server_connected;
	}

	protected void preDisconnect() {}

	protected final long RECONNECT_DELAY = 20_000;
	protected synchronized void markDisconnected() {
		markDisconnected(RECONNECT_DELAY);
	}
	protected synchronized void markDisconnected(long reconnectDelay) {
		// DebugUtil.printStackInfo();
		preDisconnect();
		// Mark flag as disconnected.
		_api_connected = false;
		log("_api_connected:" + _api_connected + " _tws_server_connected:" + _tws_server_connected);

		if (initConnectTimestamp <= 0)
			initConnectTimestamp = System.currentTimeMillis() + reconnectDelay;

		if (_apiController == null)
			return;
		try {
			log("Remove and disconnect old APIController...");
			ApiController old_controller = _apiController;
			_apiController = null;
			old_controller.disconnect();
		} catch (Exception e1) {
		}
	}

	//////////////////////////////////////////////////////
	// Events
	//////////////////////////////////////////////////////
	@Override
	public void connected() {
		_api_connected = true;
		_tws_server_connected = true;
		// Abort futher reconnect attempt.
		initConnectTimestamp = 0l;
		recordMessage(0, 0, "IB APIController connected");
		log("connected() _api_connected:" + _api_connected + " _tws_server_connected:" + _tws_server_connected);
		// DebugUtil.printStackInfo();
		postConnected();
	}

	protected void postConnected() {
	}

	@Override
	public void disconnected() {
		recordMessage(0, 0, "IB API disconnected");
		log("disconnected()");
		// Set controller NULL then mark flags as disconnected.
		_apiController = null;
		markDisconnected();

		// Reconnect automatically.
		if (initConnectTimestamp <= 0)
			initConnectTimestamp = System.currentTimeMillis() + RECONNECT_DELAY;
		connect();
	}
	
	protected final List<String> accList = new ArrayList<String>();

	@Override
	public void accountList(List<String> list) {
		log("Received account list: " + JSON.toJSONString(list));
		if (accList != null) {
			accList.clear();
			accList.addAll(list);
		}
	}

	@Override
	public void error(Exception e) {
		log("IB error received");
		recordMessage(0, 0, "IB Error received:" + e.toString());
		e.printStackTrace();
		if (e instanceof java.io.EOFException) {
			long timeElapsed = System.currentTimeMillis() - connectedTimestamp; // ignore EOF error in 5 seconds after connected.
			log("TWS EOFException after connectedTimestamp:" + timeElapsed);
			if (timeElapsed > 5000)
				markDisconnected();
		} else if (e instanceof java.net.SocketException) {
			log("Seems like TWS is not ready.");
			markDisconnected();
		}
	}

	public int lastAckErrorCode = 0, lastAckErrorID = 0;
	public String lastAckErrorMsg = "";
	@Override
	public void message(int id, int errorCode, String errorMsg) {
		switch (errorCode) {
			case 200: // No security definition has been found for the request
				StringBuilder sb = new StringBuilder();
				String[] his = _apiController.lastOperationHistory();
				for (String s : his) {
					if (s == null) continue;
					sb.append(s);
					sb.append('\n');
				}
				log("Request failed, id:" + id + ", code:" + errorCode + ", msg:" + errorMsg + "\nLast api operation:\n" + sb.toString());
				break;
			case 502: // Couldn't connect to TWS. Confirm that API is enabled in TWS via the Configure>API menu command.
				// TWS gateway might be down, retry in longer time.
				log("id:" + id + ", code:" + errorCode + ", msg:" + errorMsg);
				disconnected();
				break;
			case 504: // TWS not connected, retry in short time.
				log("id:" + id + ", code:" + errorCode + ", msg:" + errorMsg);
				disconnected();
				break;
			case 510: // Request Market Data Sending Error - java.net.SocketException: Broken pipe
				log("id:" + id + ", code:" + errorCode + ", msg:" + errorMsg);
				disconnected();
				break;
			case 1100: // Connectivity between IB and TWS has been lost.
				log("id:" + id + ", code:" + errorCode + ", msg:" + errorMsg);
				_tws_server_connected = false;
				markDisconnected();
				break;
			case 1101: // Connectivity between IB and TWS has been restored- data lost.
				log("id:" + id + ", code:" + errorCode + ", msg:" + errorMsg);
				_tws_server_connected = true;
				markTWSConnect(true);
				break;
			case 1102: // Connectivity between IB and TWS has been restored- data maintained.
				log("id:" + id + ", code:" + errorCode + ", msg:" + errorMsg);
				_tws_server_connected = true;
				markTWSConnect(false);
				break;
			case 1300: // Socket port has been reset and this connection is being dropped. Please reconnect on the new port -4002
				log(errorMsg);
				// String[] segs = errorMsg.split("-");
				// TWS_API_PORT = Integer.parseInt(segs[segs.length - 1]); // Should not be changed.
				disconnected();
				break;
			case 2103: // Market data farm connection is broken
				break;
			case 2104: // Market data farm connection is OK
				break;
			case 2105: // HMDS data farm connection is broken
				break;
			case 2106: // HMDS data farm connection is OK
				break;
			case 2107: // HMDS data farm connection is inactive but should be available upon demand.hthmds
				break;
			case 2108: // Market data farm connection is inactive but should be available upon demand.usfarm
				break;
			case 2110: // Connectivity between Trader Workstation and server is broken. It will be restored automatically.
				log("id:" + id + ", code:" + errorCode + ", msg:" + errorMsg);
				markDisconnected();
				// twsDisconnect(); // Sometimes it wont be restored automatically. WTF!
				break;
			default:
				if (lastAckErrorCode != errorCode || lastAckErrorMsg.equals(errorMsg) == false)
					log("Unhandled Message: id:" + id + ", code:" + errorCode + ", msg:" + errorMsg);
				break;
		}
	}

	@Override
	public void show(String string) {
		log("IB show: " + string);
		recordMessage(0, 0, "IB message received:" + string);
	}
	
	//////////////////////////////////////////////////////
	// Internal utilities
	//////////////////////////////////////////////////////	
	protected void waitUntilStatusReady() {
		int ct = 0;
		while(true)
			try {
				if (checkStatus())
					break;
				if (ct == 0) {
					log("Waiting for status ready: _api_connected:" + _api_connected + " _tws_server_connected:" + _tws_server_connected);
					// DebugUtil.printStackInfo();
				}
				ct += 1;
				sleep(500);
			} catch (Exception e) {}
		if (ct > 0)
			log("Status ready");
	}
}