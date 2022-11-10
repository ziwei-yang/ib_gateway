package com.bitex.util;

import static com.bitex.util.DebugUtil.*;

import java.util.function.Consumer;

import com.alibaba.fastjson.JSON;

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

	public static boolean connectivityTest() {
		try {
			POOL.getResource();
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	////////////////////////////////////////////////////////////////
	// Jedis wrapper with pool.
	////////////////////////////////////////////////////////////////

	public static void pub(String channel, Object j) {
		pub(channel, JSON.toJSONString(j));
	}
	public static void pub(String channel, String msg) {
		exec(new Consumer<Jedis>() {
			@Override
			public void accept(Jedis t) {
				t.publish(channel, msg);
			}
		});
	}
	
	public static void set(String key, Object j) {
		set(key, JSON.toJSONString(j));
	}
	public static void set(String k, String v) {
		exec(new Consumer<Jedis>() {
			@Override
			public void accept(Jedis t) {
				t.set(k, v);
			}
		});
	}
	public static void del(String k) {
		exec(new Consumer<Jedis>() {
			@Override
			public void accept(Jedis t) {
				t.del(k);
			}
		});
	}

	public static void setex (String key, int seconds, Object j) {
		setex(key, seconds, JSON.toJSONString(j));
	}
	public static void setex(String k, int seconds, String v) {
		exec(new Consumer<Jedis>() {
			@Override
			public void accept(Jedis t) {
				t.setex(k, seconds, v);
			}
		});
	}
}
