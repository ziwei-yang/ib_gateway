package com.bitex.util;

import java.util.function.Consumer;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Thread-safe redis pool
 * See: https://www.alibabacloud.com/help/doc-detail/98726.htm
 */
public class Redis {
	
	private static final JedisPool POOL;

	static {
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxIdle(5);
		config.setMaxTotal(100);
		config.setTestOnBorrow(false);
		config.setTestOnReturn(false);
		POOL = new JedisPool(
				config,
				System.getenv("REDIS_HOST"),
				Integer.parseInt(System.getenv("REDIS_PORT")),
				10_000,
				System.getenv("REDIS_PSWD"));
	}

	public static void exec(Consumer<Jedis> lambda) {
		Jedis jedis = null;
		try {
			jedis = POOL.getResource();
			lambda.accept(jedis);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
		    //In JedisPool >= 2.7 mode, the Jedis resource is returned to the resource pool.
			if (jedis != null)
				jedis.close();
		}
	}
}
