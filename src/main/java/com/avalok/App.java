package com.avalok;

import com.bitex.util.ReflectionUtil;
import static com.bitex.util.DebugUtil.*;

/**
 * Hello world!
 *
 */
public class App {
	public static void main( String[] args ) {
		long usedTime = System.currentTimeMillis();

		if (args.length < 1) {
			log("Usage: java -jar this.jar <ClassName> [args...]");
			return;
		}

		String className = args[0];
		String argContent = "";
		String[] stringArgs = new String[args.length - 1];
		for (int i = 1; i < args.length; i++) {
			stringArgs[i - 1] = args[i];
			argContent += "[" + (i - 1) + "]\t[" + args[i] + "]\n";
		}
		Object[] actualArgs = new Object[]{ stringArgs };

		log("Invoking static " + className + ".main(String[]) with below args:\n" + argContent);

		ReflectionUtil.invokeStatic(className, "main", actualArgs);
		usedTime = System.currentTimeMillis() - usedTime;
		log("Method:" + className + ".main(String[]) finished.");
		log("Time used:" + (usedTime / 1000) / 60 + " m " + (usedTime / 1000 % 60) + " s");
		log("Args:\n" + argContent);
	}
}
