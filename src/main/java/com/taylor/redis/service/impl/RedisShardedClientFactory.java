package com.taylor.redis.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.taylor.common.exceptions.CommonRuntimeException;
import com.taylor.redis.common.shard.RedisMasterSlaverGroup;
import com.taylor.redis.common.shard.RedisShardInfo;
import com.taylor.redis.common.shard.RedisShardSplit;
import com.taylor.redis.common.shard.ShardSplit;
import com.taylor.redis.common.util.RedisMasterProber;
import com.taylor.redis.common.util.RedisMasterProber.Prober;
import com.taylor.redis.service.RedisClientFactory;

import lombok.Data;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Protocol;

@Data
public class RedisShardedClientFactory implements RedisClientFactory {

	private List<RedisShardInfo> shardInfos;

	private List<RedisMasterSlaverGroup> groups;

	private RedisShardedClient client;

	private RedisShardSplit<JedisPool, RedisMasterSlaverGroup> splitor;

	@Override
	public void afterPropertiesSet() throws Exception {

		/* 将所有redis分组放进groups里 */
		groups = new ArrayList<RedisMasterSlaverGroup>();
		if (CollectionUtils.isEmpty(shardInfos)) {
			return;
		}

		/* 存放<Groupid, RedisMasterSlaverGroup>键值对 */
		Map<Integer, RedisMasterSlaverGroup> groupMap = new HashMap<Integer, RedisMasterSlaverGroup>();

		for (RedisShardInfo shard : shardInfos) {

			shard.setTimeout(PoolConfig.timeout);

			Integer groupId = shard.getGroupId();

			RedisMasterSlaverGroup group = null;
			if (groupMap.get(groupId) == null) {
				group = new RedisMasterSlaverGroup();
				group.setId(groupId);
				groupMap.put(groupId, group);
			} else {
				group = groupMap.get(groupId);
			}
			/* 首先将所有配置的redis服务都当做从，接下来再选出主 */
			group.addSlaver(shard);
		}

		ExecutorService excutor = Executors.newFixedThreadPool(groupMap.size());
		try {
			ConcurrentLinkedQueue<Future<RedisMasterSlaverGroup>> probeResult = new ConcurrentLinkedQueue<Future<RedisMasterSlaverGroup>>();
			for (Entry<Integer, RedisMasterSlaverGroup> entry : groupMap.entrySet()) {
				if (entry.getValue() != null && null != entry.getValue()) {
					Prober probeTask = new RedisMasterProber.Prober(entry.getValue(), entry.getKey());
					probeResult.add(excutor.submit(probeTask));
				}
			}
			for (Future<RedisMasterSlaverGroup> future : probeResult) {
				groups.add(future.get());
			}
			createClient(groups);
		} catch (Throwable e) {
			throw new CommonRuntimeException("Initialize redis shard group failed", e);
		} finally {
			if (excutor != null && !excutor.isShutdown()) {
				excutor.shutdown();
			}
		}
	}

	private void createClient(List<RedisMasterSlaverGroup> groups) {

		GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
		if (PoolConfig.maxIdle >= 0) {
			poolConfig.setMaxIdle(PoolConfig.maxIdle);
		}
		poolConfig.setMaxWaitMillis(PoolConfig.maxWait);
		poolConfig.setTestOnBorrow(PoolConfig.testOnBorrow);
		poolConfig.setMinIdle(PoolConfig.minIdle);
		poolConfig.setTestOnReturn(PoolConfig.testOnReturn);
		poolConfig.setTestWhileIdle(PoolConfig.testWhileIdle);
		poolConfig.setTimeBetweenEvictionRunsMillis(PoolConfig.timeBetweenEvictionRunsMillis);
		poolConfig.setNumTestsPerEvictionRun(PoolConfig.numTestsPerEvictionRun);
		poolConfig.setMinEvictableIdleTimeMillis(PoolConfig.minEvictableIdleTimeMillis);
		poolConfig.setSoftMinEvictableIdleTimeMillis(PoolConfig.softMinEvictableIdleTimeMillis);
		poolConfig.setLifo(PoolConfig.lifo);

		splitor = ShardSplit.getInstance(groups, poolConfig);
		client = new RedisShardedClient(splitor);
	}

	public void destroy() throws Exception {
		splitor.destoryAllResources();
		client = null;
	}

	public Object getObject() throws Exception {

		return client;
	}

	public Class<RedisShardedClient> getObjectType() {
		return RedisShardedClient.class;
	}

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
