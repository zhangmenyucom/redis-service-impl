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

	private int timeout = Protocol.DEFAULT_TIMEOUT;

	private int maxIdle = GenericObjectPool.DEFAULT_MAX_IDLE;

	private long maxWait = GenericObjectPool.DEFAULT_MAX_WAIT;

	private boolean testOnBorrow = GenericObjectPool.DEFAULT_TEST_ON_BORROW;

	private int minIdle = GenericObjectPool.DEFAULT_MIN_IDLE;

	private int maxActive = GenericObjectPool.DEFAULT_MAX_ACTIVE;

	private boolean testOnReturn = GenericObjectPool.DEFAULT_TEST_ON_RETURN;

	private boolean testWhileIdle = GenericObjectPool.DEFAULT_TEST_WHILE_IDLE;

	private long timeBetweenEvictionRunsMillis = GenericObjectPool.DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS;

	private int numTestsPerEvictionRun = GenericObjectPool.DEFAULT_NUM_TESTS_PER_EVICTION_RUN;

	private long minEvictableIdleTimeMillis = GenericObjectPool.DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS;

	private long softMinEvictableIdleTimeMillis = GenericObjectPool.DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS;

	private boolean lifo = GenericObjectPool.DEFAULT_LIFO;

	private byte whenExhaustedAction = GenericObjectPool.WHEN_EXHAUSTED_BLOCK;

	private RedisSimpleClientInfo redisClientInfo;

	private RedisSimplePool redisPool;

	private RedisSimpleClient redisClient;

	private void createJedisPool(RedisSimpleClientInfo redisClientInfo) {
		GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
		if (this.maxIdle >= 0) {
			poolConfig.setMaxIdle(this.maxIdle);
		}
		poolConfig.setMaxWaitMillis(this.maxWait);
		poolConfig.setTestOnBorrow(this.testOnBorrow);
		poolConfig.setMinIdle(this.minIdle);
		poolConfig.setMaxTotal(this.maxActive);
		poolConfig.setTestOnReturn(this.testOnReturn);
		poolConfig.setTestWhileIdle(this.testWhileIdle);
		poolConfig.setTimeBetweenEvictionRunsMillis(this.timeBetweenEvictionRunsMillis);
		poolConfig.setNumTestsPerEvictionRun(this.numTestsPerEvictionRun);
		poolConfig.setMinEvictableIdleTimeMillis(this.minEvictableIdleTimeMillis);
		poolConfig.setSoftMinEvictableIdleTimeMillis(this.softMinEvictableIdleTimeMillis);
		poolConfig.setLifo(this.lifo);

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
}
