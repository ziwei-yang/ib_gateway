package com.avalok.ib;

import static com.bitex.util.DebugUtil.*;

import com.avalok.ib.controller.BaseIBController;
import com.avalok.ib.handler.DeepMktDataHandler;

public class GatewayController extends BaseIBController {
	public static void main(String[] args) throws Exception {
		new GatewayController().work();
	}

	private void subscribeExample() {
		IBContract contract;
		try {
//			contract = new IBContract("TSE", "USD-BTCC.U");
			contract = new IBContract("ICECRYPTO", "USD-BAKKT@202105");
		} catch (Exception e) {
			log("Failed to init IBContract");
			return;
		}
		log(contract.shownName());
		int numOfRows = 10;
		boolean isSmartDepth = false;
		_apiController.reqDeepMktData(
				contract, numOfRows, isSmartDepth,
				new DeepMktDataHandler(contract, true));
	}

	@Override
	protected void _postConnected() {
		log("_postConnected");
		subscribeExample(); // TODO resubscribe all if have jobs
	}

	private void work() throws Exception {
		while (true)
			sleep(1000);
	}

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
