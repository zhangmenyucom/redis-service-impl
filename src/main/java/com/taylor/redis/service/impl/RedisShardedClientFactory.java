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

	private List<RedisShardInfo> shardInfos;

	private List<RedisMasterSlaverGroup> groups;

	private RedisShardedClient client;

	private RedisShardSplit<JedisPool, RedisMasterSlaverGroup> splitor;

	@Override
	public void afterPropertiesSet() throws Exception {

		groups = new ArrayList<RedisMasterSlaverGroup>();
		// initialize RedisMasterSlaverGroup list;
		if (CollectionUtils.isEmpty(shardInfos))
			return;

		Map<Integer, RedisMasterSlaverGroup> groupMap = new HashMap<Integer, RedisMasterSlaverGroup>();

		for (RedisShardInfo shard : shardInfos) {
			// set config:timeout
			shard.setTimeout(timeout);

			// find group
			Integer groupId = shard.getGroupId();

			RedisMasterSlaverGroup group = null;

			if (groupMap.get(groupId) == null) {
				group = new RedisMasterSlaverGroup();
				group.setId(groupId);
				groupMap.put(groupId, group);
			} else {
				group = groupMap.get(groupId);
			}

			// all as slaver firstly, discovery master by under prober
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
			if (excutor != null && !excutor.isShutdown())
				excutor.shutdown();
		}
	}

	private void createClient(List<RedisMasterSlaverGroup> groups) {

		GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
		if (this.maxIdle >= 0) {
			poolConfig.setMaxIdle(this.maxIdle);
		}
		poolConfig.setMaxWaitMillis(this.maxWait);
		poolConfig.setTestOnBorrow(this.testOnBorrow);
		poolConfig.setMinIdle(this.minIdle);
		poolConfig.setTestOnReturn(this.testOnReturn);
		poolConfig.setTestWhileIdle(this.testWhileIdle);
		poolConfig.setTimeBetweenEvictionRunsMillis(this.timeBetweenEvictionRunsMillis);
		poolConfig.setNumTestsPerEvictionRun(this.numTestsPerEvictionRun);
		poolConfig.setMinEvictableIdleTimeMillis(this.minEvictableIdleTimeMillis);
		poolConfig.setSoftMinEvictableIdleTimeMillis(this.softMinEvictableIdleTimeMillis);
		poolConfig.setLifo(this.lifo);

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
}
