package com.avalok;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.bitex.util.ReflectionUtil;

/**
 * Hello world!
 *
 */
public class App {
	private static final Logger logger= LogManager.getLogger(App.class);
	public static void main( String[] args ) {
		logger.error("Logger error level test");
		logger.info("Logger info level test");
		logger.debug("Logger debug level test");
		logger.trace("Logger trace level test");
		long usedTime = System.currentTimeMillis();

		if (args.length < 1) {
			System.out.println("Usage: java -jar this.jar <ClassName> [args...]");
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

		System.out.println("Invoking static " + className + ".main(String[]) with below args:\n" + argContent);

		ReflectionUtil.invokeStatic(className, "main", actualArgs);
		usedTime = System.currentTimeMillis() - usedTime;
		System.out.println("Method:" + className + ".main(String[]) finished.");
		System.out.println("Time used:" + (usedTime / 1000) / 60 + " m " + (usedTime / 1000 % 60) + " s");
		System.out.println("Args:\n" + argContent);
	}
}
