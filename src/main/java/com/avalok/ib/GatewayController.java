package com.avalok.ib;

import static com.bitex.util.DebugUtil.*;

public class GatewayController extends BaseIBController {
	public static void main(String[] args) {
		new GatewayController().work();
	}

	private void work() {
		while (true)
			sleep(1000);
	}
}
