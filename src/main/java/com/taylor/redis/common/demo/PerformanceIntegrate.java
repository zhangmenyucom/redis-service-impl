package com.taylor.redis.common.demo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang3.StringUtils;

import com.taylor.redis.common.shard.RedisShardInfo;
import com.taylor.redis.service.impl.RedisShardedClient;
import com.taylor.redis.service.impl.RedisShardedClientFactory;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class PerformanceIntegrate {
	private static boolean finishFlag = false;
	private static LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<String>(100 * 100);

	private static final String PREFIX = "prefix_num";

	public static void main(String[] args) throws Exception {
		String host_m_1 = "10.48.49.41";
		int port_m_1 = 6301;
		String host_s_1 = "10.52.132.56";
		int port_s_1 = 6301;
		String host_m_2 = "10.52.132.56";
		int port_m_2 = 6302;
		String host_s_2 = "10.48.49.41";
		int port_s_2 = 6302;

		List<RedisShardInfo> shards = new ArrayList<RedisShardInfo>();
		RedisShardInfo shard = new RedisShardInfo();
		shard.setGroupId(1);
		shard.setHost(host_m_1);
		shard.setPort(port_m_1);
		shards.add(shard);

		shard = new RedisShardInfo();
		shard.setGroupId(1);
		shard.setHost(host_s_1);
		shard.setPort(port_s_1);
		shards.add(shard);

		shard = new RedisShardInfo();
		shard.setGroupId(2);
		shard.setHost(host_m_2);
		shard.setPort(port_m_2);
		shards.add(shard);

		shard = new RedisShardInfo();
		shard.setGroupId(2);
		shard.setHost(host_s_2);
		shard.setPort(port_s_2);
		shards.add(shard);

		RedisShardedClientFactory factory = new RedisShardedClientFactory();
		factory.setShardInfos(shards);
		factory.afterPropertiesSet();

		final int nThreads = 50;
		final RedisShardedClient client = (RedisShardedClient) factory.getObject();

		ExecutorService executor = Executors.newFixedThreadPool(nThreads);

		try {

			for (int i = 0; i < nThreads; i++) {

				executor.submit(new Callable<Boolean>() {

					public Boolean call() throws Exception {
						int line = 1;
						long start = System.currentTimeMillis();
						while (line < 100 * 100) {

							String data = line + System.currentTimeMillis() + "\tphonenum";
							// flushToRedis(client, data);
							flushToRedis_set(client, data);
							line++;

						}
						long end = System.currentTimeMillis();
						log.info("file " + " line " + line + " consume times(ms): " + (end - start));
						start = end;
						return null;
					}
				});
			}

		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			executor.shutdown();
		}
	}

	protected static void flushToRedis(RedisShardedClient client, String tempString) {

		if (StringUtils.isEmpty(tempString)) {
			return;
		}
		String[] strings = StringUtils.split(tempString, "\t");
		if (strings.length >= 2) {
			String custId = StringUtils.trim(strings[0]);
			String phone = StringUtils.trim(strings[1]);
			client.sadd(PREFIX + phone, custId);
		}
	}

	protected static void flushToRedis_set(RedisShardedClient client, String tempString) {

		if (StringUtils.isEmpty(tempString)) {
			return;
		}
		String[] strings = StringUtils.split(tempString, "\t");
		if (strings.length >= 2) {
			String custId = StringUtils.trim(strings[0]);
			String phone = StringUtils.trim(strings[1]);
			client.set(PREFIX + phone, custId);
		}
	}

	@SuppressWarnings("unused")
	private static void consumer(ExecutorService excutor, final RedisShardedClient client, int num) {

		for (int count = 0; count < num; count++) {

			excutor.submit(new Callable<Boolean>() {

				public Boolean call() throws Exception {
					int count = 1;
					long start = System.currentTimeMillis();
					try {
						while (true) {

							if (finishFlag && queue.isEmpty()) {
								return true;
							}

							String data = queue.take();
							flushToRedis(client, data);
							count++;
						}

					} catch (InterruptedException e) {
						log.error(e.getMessage(), e);
						return false;
					} finally {
						long end = System.currentTimeMillis();
						log.info(" count:  " + count + "  consume:  " + (end - start));
					}
				}
			});
		}

	}
}
