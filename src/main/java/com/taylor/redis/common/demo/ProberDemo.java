package com.taylor.redis.common.demo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.taylor.redis.common.shard.RedisMasterSlaverGroup;
import com.taylor.redis.common.shard.RedisShardInfo;
import com.taylor.redis.common.util.RedisMasterProber;
import com.taylor.redis.common.util.RedisMasterProber.Prober;

import lombok.extern.log4j.Log4j2;
import redis.clients.jedis.Jedis;

@Log4j2
public class ProberDemo {

	public static void main(String[] args) {
		// *
		// * 主：10.48.22.70：6301 备：10.48.22.73：6301
		// * 主：10.48.22.70：6302 备：10.48.22.73：6302
		// * 主：10.48.22.70：6303 备：10.48.22.73：6303
		// * 主：10.48.22.70：6304 备：10.48.22.73：6304

		String host_m_1 = "10.48.22.70";
		int port_m_1 = 6301;
		String host_s_1 = "10.48.22.73";
		int port_s_1 = 6301;

		// 0. test ping
		Jedis jedis = new Jedis("10.48.22.70", 6301);
		log.debug(jedis.ping());

		log.debug(jedis.info());
		// 0.1 test info
		log.debug(RedisMasterProber.redisInfo(jedis, "role"));

		RedisShardInfo master_s = new RedisShardInfo();
		master_s.setGroupId(1);
		master_s.setHost(host_m_1);
		master_s.setPort(port_m_1);

		RedisShardInfo slaver_s = new RedisShardInfo();
		slaver_s.setGroupId(1);
		slaver_s.setHost(host_s_1);
		slaver_s.setPort(port_s_1);

		// 1. test init
		RedisMasterSlaverGroup shards = new RedisMasterSlaverGroup();
		shards.setId(1);
		shards.addSlaver(master_s);
		shards.addSlaver(slaver_s);

		// Integer groupId = 1;
		// Prober task = new Prober(shards, groupId);
		// ExecutorService executor = Executors.newFixedThreadPool(1);
		// Collection<Prober> tasks = new
		// ArrayList<WeimobRedisMasterProber.Prober>();
		// tasks.add(task);
		// try {
		// executor.invokeAll(tasks);
		// } catch (InterruptedException e) {
		// e.printStackTrace();
		// } finally {
		// executor.shutdown();
		// }

		// 2. test master is slaver

		RedisMasterSlaverGroup shards2 = new RedisMasterSlaverGroup();
		shards2.addSlaver(master_s);
		shards2.setMaster(slaver_s);

		Integer groupId = 1;
		Prober task2 = new Prober(shards2, groupId);
		ExecutorService executor = Executors.newFixedThreadPool(1);
		Collection<Prober> tasks = new ArrayList<RedisMasterProber.Prober>();
		tasks.add(task2);
		try {
			executor.invokeAll(tasks);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			executor.shutdown();
		}
		// log.debug(binaryJedis.configGet(SafeEncoder.encode("role")));
	}
}
