package com.avalok.ib.controller;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.ib.controller.ApiController.*;

import com.alibaba.fastjson.*;
import com.avalok.ib.logger.NullIBLogger;

import static com.bitex.util.DebugUtil.*;

/**
 * All TWS Controller classes are based on this.
 */
public abstract class BaseIBController implements IConnectionHandler {
	protected IBApiController _apiController;

	//////////////////////////////////////////////////////
	// Callback methods, with active handler detection.
	//////////////////////////////////////////////////////
	protected IConnectionHandler _activeIBConnectionHandler;
	protected IConnectionHandler _assignNewIConnectionHandler() {
		IConnectionHandler handler = new IConnectionHandler() {
			public void connected() {
				if (this != _activeIBConnectionHandler) {
					log("Ignore obsolete IBConnectionHandler event: connected");
					return;
				}
				BaseIBController.this.connected();
			}
			public void disconnected() {
				if (this != _activeIBConnectionHandler) {
					log("Ignore obsolete IBConnectionHandler event: disconnected");
					return;
				}
				BaseIBController.this.disconnected();
			}
			public void accountList(List<String> list) {
				if (this != _activeIBConnectionHandler) {
					log("Ignore obsolete IBConnectionHandler event: accountList");
					return;
				}
				BaseIBController.this.accountList(list);
			}
			public void error(Exception e) {
				if (this != _activeIBConnectionHandler) {
					log("Ignore obsolete IBConnectionHandler event: error");
					return;
				}
				BaseIBController.this.error(e);
			}
			public void message(int id, int errorCode, String errorMsg) {
				if (this != _activeIBConnectionHandler) {
					log("Ignore obsolete IBConnectionHandler event: message");
					return;
				}
				BaseIBController.this.message(id, errorCode, errorMsg);
			}
			public void show(String string) {
				if (this != _activeIBConnectionHandler) {
					log("Ignore obsolete IBConnectionHandler event: show");
					return;
				}
				BaseIBController.this.show(string);
			}
		};
		_activeIBConnectionHandler = handler;
		return handler;
	}

	protected String _name = System.getenv("TWS_GATEWAY_NAME");
	public String name(){ return _name; }
	
	//////////////////////////////////////////////////////
	// Cache historical messages.
	//////////////////////////////////////////////////////
	protected final Queue<JSONObject> _ibMsgs = new ConcurrentLinkedQueue<>();
	protected static final int MAX_HIS_MSG = 1000;
	protected long _connectedTS = 0;
	protected long _initConnTS = System.currentTimeMillis(); // Prepare to connect in initialisation.
	public BaseIBController() {
		for (int i = 0; i < MAX_HIS_MSG; i++)
			_ibMsgs.add(new JSONObject());
		_connect();
	}

	protected JSONObject recordMessage(int orderID, int errorCode, String errorMsg) {
		log("recordMessage() orderID " + orderID + " errorCode " + errorCode + " " + errorMsg);
		JSONObject msg = new JSONObject();
		msg.put("timestamp", System.currentTimeMillis());
		msg.put("order_id", orderID);
		msg.put("error_code", errorCode);
		msg.put("error_msg", errorMsg);
		_ibMsgs.add(msg);
		// Keep size of message history.
		_ibMsgs.poll();
		return msg;
	}

	public JSONObject[] messageHistory(int size) {
		if (size <= 0) return new JSONObject[0];
		int from = MAX_HIS_MSG - size;
		if (from < 0) from = 0;
		int to = MAX_HIS_MSG;
		Object[] msgs = Arrays.copyOfRange(_ibMsgs.toArray(), from, to);
		JSONObject[] results = new JSONObject[size];
		for (int i = 0; i < size; i++)
			results[i] = (JSONObject)(msgs[size - 1 - i]);
		return results;
	}

	//////////////////////////////////////////////////////
	// Initialise ENV and _connect() in background thread
	//////////////////////////////////////////////////////
	public final static String TWS_API_ADDR = System.getenv("TWS_API_ADDR");
	public final static int TWS_API_PORT = Integer.parseInt(System.getenv("TWS_API_PORT"));
	public final static String TWS_NAME = TWS_API_ADDR + "_" + TWS_API_PORT;
	protected int _apiClientID = Integer.parseInt(System.getenv("TWS_API_CLIENTID")); // Only the default client (i.e 0) can auto bind orders
	protected synchronized void _connect() {
		// DebugUtil.printStackInfo();
		if (isConnected()) {
			log("status is still good, abort _connect()");
			return;
		} else if (_initConnTS <= 0) {
			log("_initConnTS <= 0, abort _connect()");
			return;
		} else if (_initConnTS >= System.currentTimeMillis()) {
			log("Sleep " + (_initConnTS - System.currentTimeMillis()) + "ms before _connect()");
			sleep(_initConnTS - System.currentTimeMillis());
		}
		new Thread(new Runnable() {
			public void run() {
				int retry_ct = 0;
				log("Connect thread started.");
				while (true) {
					try {
						log("Connecting gateway " + TWS_API_ADDR + " ID " + _apiClientID);
						// TODO this step might hang.
						IBApiController newController = new IBApiController(_assignNewIConnectionHandler(), new NullIBLogger(), new NullIBLogger());
						// make initial connection to local host, port 7496, client id 0, no connection options
						newController.connect(TWS_API_ADDR, TWS_API_PORT, _apiClientID, null);
						_apiController = newController;  // Only assign after _connect()
						_connectedTS = System.currentTimeMillis();
						log("Gateway connected with client ID " + _apiClientID);
						break;
					} catch (StackOverflowError e) {
						log("StackOverflowError in connecting gateway with ID " + _apiClientID + " retry_ct:" + retry_ct);
					} catch (Exception e) {
						log("Error in connecting gateway with ID " + _apiClientID + " retry_ct:" + retry_ct);
						log(e);
					} finally {
						retry_ct += 1;
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
	protected boolean _apiConnected = false, _twsConnected = false;

	protected void _markTWSServerDisconnected() {
		_twsConnected = false;
		log("_markTWSServerDisconnected() _apiConnected:" + _apiConnected + " _twsConnected:" + _twsConnected);
		// DebugUtil.printStackInfo();
	}
	protected void _markTWSServerConnected(boolean dataLost) {
		_apiConnected = true;
		_twsConnected = true;
		log("_markTWSServerConnected() dataLost:" + dataLost + " _apiConnected:" + _apiConnected + " _twsConnected:" + _twsConnected);
		// DebugUtil.printStackInfo();
	}

	////////////////////////////////////////////////////////////////
	// IConnectionHandler <-> TWS connectivity
	////////////////////////////////////////////////////////////////
	public boolean isConnected() {
		return _apiConnected && _twsConnected;
	}

	protected static final long RECONNECT_DELAY = 20_000;
	protected void _postDisconnected() {}
	protected synchronized void _markDisconnected() {
		_markDisconnected(RECONNECT_DELAY);
	}
	protected synchronized void _markDisconnected(long reconnectDelay) {
		// DebugUtil.printStackInfo();
		// Mark flag as disconnected.
		_apiConnected = false;
		log("_markDisconnected() _apiConnected:" + _apiConnected + " _twsConnected:" + _twsConnected);

		_initConnTS = System.currentTimeMillis() + reconnectDelay;

		if (_apiController == null)
			return;
		try {
			log("_markDisconnected() Remove and disconnect old APIController...");
			IBApiController old_controller = _apiController;
			_apiController = null;
			_postDisconnected();
			old_controller.disconnect(); // Dispose resource at last
		} catch (Exception e1) {
		}
	}

	//////////////////////////////////////////////////////
	// Events
	//////////////////////////////////////////////////////
	@Override
	public void connected() {
		_apiConnected = true;
		_twsConnected = true;
		recordMessage(0, 0, "IB APIController connected");
		log("connected() _apiConnected:" + _apiConnected + " _twsConnected:" + _twsConnected);
		// DebugUtil.printStackInfo();
		_postConnected();
	}

	protected void _postConnected() {
	}

	@Override
	public void disconnected() {
		recordMessage(0, 0, "IB API disconnected");
		log("disconnected()");
		// Set controller NULL then mark flags as disconnected.
		_apiController = null;
		_markDisconnected();
		// Reconnect automatically.
		_connect();
	}
	
	protected final List<String> accList = new ArrayList<String>();

	@Override
	public void accountList(List<String> list) {
		info("<-- account list: " + JSON.toJSONString(list));
		if (accList != null) {
			accList.clear();
			accList.addAll(list);
		}
	}

	@Override
	public void error(Exception e) {
		err("IB Error received:" + e.toString());
		recordMessage(0, 0, "IB Error received:" + e.toString());
		e.printStackTrace();
		if (e instanceof java.io.EOFException) {
			long timeElapsed = System.currentTimeMillis() - _connectedTS; // ignore EOF error in 5 seconds after connected.
			log("TWS EOFException after _connectedTS:" + timeElapsed);
			if (timeElapsed > 5000)
				_markDisconnected();
			else
				log("TWS EOFException ignored");
		} else if (e instanceof java.net.SocketException) {
			log("SocketException caught, seems TWS is not ready, _markDisconnected().");
			_markDisconnected();
		}
	}

	// If lastAckErrorID+lastAckErrorCode+lastAckErrorMsg has been set by other handlers
	// Unknown message will be suppressed.
	public int lastAckErrorCode = 0, lastAckErrorID = 0;
	public String lastAckErrorMsg = "";
	@Override
	public void message(int id, int errorCode, String errorMsg) {
		// TODO need error code reference webpage URL.
		switch (errorCode) {
			case 200: // No security definition has been found for the request
				log("id:" + id + ", code:" + errorCode + ", msg:" + errorMsg);
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
			case 507: // Bad Message Length null
				log("id:" + id + ", code:" + errorCode + ", msg:" + errorMsg);
				disconnected();
				break;
			case 510: // Request Market Data Sending Error - java.net.SocketException: Broken pipe
				log("id:" + id + ", code:" + errorCode + ", msg:" + errorMsg);
				disconnected();
				break;
			case 1100: // Connectivity between IB and TWS has been lost.
				log("id:" + id + ", code:" + errorCode + ", msg:" + errorMsg);
				_twsConnected = false;
				_markDisconnected();
				break;
			case 1101: // Connectivity between IB and TWS has been restored- data lost.
				log("id:" + id + ", code:" + errorCode + ", msg:" + errorMsg);
				_twsConnected = true;
				_markTWSServerConnected(true);
				break;
			case 1102: // Connectivity between IB and TWS has been restored- data maintained.
				log("id:" + id + ", code:" + errorCode + ", msg:" + errorMsg);
				_twsConnected = true;
				_markTWSServerConnected(false);
				break;
			case 1300: // Socket port has been reset and this connection is being dropped. Please reconnect on the new port -4002
				log("id:" + id + ", code:" + errorCode + ", msg:" + errorMsg);
				// String[] segs = errorMsg.split("-");
				// TWS_API_PORT = Integer.parseInt(segs[segs.length - 1]); // Should not be changed.
				disconnected();
				break;
			case 2103: // Market data farm connection is broken
				log("id:" + id + ", code:" + errorCode + ", msg:" + errorMsg);
				break;
			case 2104: // Market data farm connection is OK
				break;
			case 2105: // HMDS data farm connection is broken
				log("id:" + id + ", code:" + errorCode + ", msg:" + errorMsg);
				break;
			case 2106: // HMDS data farm connection is OK
				break;
			case 2107: // HMDS data farm connection is inactive but should be available upon demand.hthmds
				log("id:" + id + ", code:" + errorCode + ", msg:" + errorMsg);
				break;
			case 2108: // Market data farm connection is inactive but should be available upon demand.usfarm
				log("id:" + id + ", code:" + errorCode + ", msg:" + errorMsg);
				break;
			case 2110: // Connectivity between Trader Workstation and server is broken. It will be restored automatically.
				log("id:" + id + ", code:" + errorCode + ", msg:" + errorMsg);
				_markDisconnected();
				// twsDis_connect(); // Sometimes it wont be restored automatically. WTF!
				break;
			case 2158: // Sec-def data farm connection is OK:secdefhk
				break;
			default:
				if (lastAckErrorID != id || lastAckErrorCode != errorCode || lastAckErrorMsg.equals(errorMsg) == false)
					warn("Unhandled Message: id:" + id + ", code:" + errorCode + ", msg:" + errorMsg);
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
				if (isConnected())
					break;
				if (ct == 0) {
					warn("Waiting for status ready: _apiConnected:" + _apiConnected + " _twsConnected:" + _twsConnected);
					// DebugUtil.printStackInfo();
				}
				ct += 1;
				sleep(500);
			} catch (Exception e) {}
		if (ct > 0)
			log("Status ready");
	}
}
