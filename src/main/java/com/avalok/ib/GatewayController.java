package com.avalok.ib;

import com.ib.client.Types.*;

import static com.bitex.util.DebugUtil.*;

public class GatewayController extends BaseIBController {
	public static void main(String[] args) throws Exception {
		new GatewayController().work();
	}

	private void subscribeExample() throws Exception {
//		IBContract contract = new IBContract("ICECRYPTO", "USD-BAKKT@202105");
		IBContract contract = new IBContract("TSE", "USD-BTCC.U");
		log(contract.shownName());
		int numOfRows = 10;
		boolean isSmartDepth = false;
		sleep(5000);
		_apiController.reqDeepMktData(contract, numOfRows, isSmartDepth, new DeepMktDataHandler(contract));
	}

	private void work() throws Exception {
		subscribeExample();
		while (true)
			sleep(1000);
	}
}
