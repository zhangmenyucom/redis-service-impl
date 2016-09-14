package com.taylor.redis.service.impl;

import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.taylor.redis.common.client.RedisSimpleClientInfo;
import com.taylor.redis.common.shard.RedisSimplePool;
import com.taylor.redis.service.RedisClientFactory;

import lombok.Data;
import redis.clients.jedis.Protocol;

@Data
public class RedisSimpleClientFactory implements RedisClientFactory {

	private RedisSimpleClientInfo redisClientInfo;

	private RedisSimplePool redisPool;

	private RedisSimpleClient redisClient;

	private void createJedisPool(RedisSimpleClientInfo redisClientInfo) {
		GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
		if (PoolConfig.maxIdle >= 0) {
			poolConfig.setMaxIdle(PoolConfig.maxIdle);
		}
		poolConfig.setMaxWaitMillis(PoolConfig.maxWait);
		poolConfig.setTestOnBorrow(PoolConfig.testOnBorrow);
		poolConfig.setMinIdle(PoolConfig.minIdle);
		poolConfig.setMaxTotal(PoolConfig.maxActive);
		poolConfig.setTestOnReturn(PoolConfig.testOnReturn);
		poolConfig.setTestWhileIdle(PoolConfig.testWhileIdle);
		poolConfig.setTimeBetweenEvictionRunsMillis(PoolConfig.timeBetweenEvictionRunsMillis);
		poolConfig.setNumTestsPerEvictionRun(PoolConfig.numTestsPerEvictionRun);
		poolConfig.setMinEvictableIdleTimeMillis(PoolConfig.minEvictableIdleTimeMillis);
		poolConfig.setSoftMinEvictableIdleTimeMillis(PoolConfig.softMinEvictableIdleTimeMillis);
		poolConfig.setLifo(PoolConfig.lifo);

		redisPool = new RedisSimplePool(redisClientInfo, poolConfig);
		redisClient = new RedisSimpleClient(redisPool);
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		createJedisPool(redisClientInfo);
	}

	@Override
	public void destroy() throws Exception {
		redisPool.destoryAllResources();
		redisClientInfo = null;
	}

	@Override
	public Object getObject() throws Exception {
		return redisClient;
	}

	@Override
	public Class<RedisSimpleClient> getObjectType() {
		return RedisSimpleClient.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	public static class PoolConfig {
		public static int timeout = Protocol.DEFAULT_TIMEOUT;

		public static int maxIdle = GenericObjectPool.DEFAULT_MAX_IDLE;

		public static long maxWait = GenericObjectPool.DEFAULT_MAX_WAIT;

		public static boolean testOnBorrow = GenericObjectPool.DEFAULT_TEST_ON_BORROW;

		public static int minIdle = GenericObjectPool.DEFAULT_MIN_IDLE;

		public static int maxActive = GenericObjectPool.DEFAULT_MAX_ACTIVE;

		public static boolean testOnReturn = GenericObjectPool.DEFAULT_TEST_ON_RETURN;

		public static boolean testWhileIdle = GenericObjectPool.DEFAULT_TEST_WHILE_IDLE;

		public static long timeBetweenEvictionRunsMillis = GenericObjectPool.DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS;

		public static int numTestsPerEvictionRun = GenericObjectPool.DEFAULT_NUM_TESTS_PER_EVICTION_RUN;

		public static long minEvictableIdleTimeMillis = GenericObjectPool.DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS;

		public static long softMinEvictableIdleTimeMillis = GenericObjectPool.DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS;

		public static boolean lifo = GenericObjectPool.DEFAULT_LIFO;

		public static byte whenExhaustedAction = GenericObjectPool.WHEN_EXHAUSTED_BLOCK;
	}
}
