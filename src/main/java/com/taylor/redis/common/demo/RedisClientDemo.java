/**
 * 
 */
package com.taylor.redis.common.demo;

import java.io.Serializable;

import com.taylor.redis.common.client.RedisClient;

/**
 * @author HaydenWang
 *
 */
public class RedisClientDemo implements Serializable {
	private int id = 0;
	/**
	 * 
	 */
	private static final long serialVersionUID = -5398820396995717310L;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		RedisClient client = getClient();
		int i = 0;
		while (i < 1000) {
			i++;
			try {
				String key = "test_" + i;
				if (i % 2 == 0) {
					client.set(key, 100l);
				} else {
					client.set(key, new RedisClientDemo());
				}
			} catch (Throwable e) {
				e.printStackTrace();
			}
		}

		for (i = 0; i < 1000; i++) {
			String key = "test_" + i;
			Object value = client.getObject(key);
			System.out.println(value);
		}
	}

	public static RedisClient getClient() {
		RedisClient client = new RedisClient();
		client.setHost("115.159.53.124");
		client.setInstanceId("e0879b32-5e36-4b3e-879c-d364568773ee");
		// client.setPassword("1q2w3e4r5A");
		client.setPort(6379);
		client.init();
		return client;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

}
