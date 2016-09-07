package com.taylor.redis.common.demo;

import java.util.ArrayList;
import java.util.List;

import com.taylor.redis.common.shard.RedisShardInfo;
import com.taylor.redis.service.impl.RedisShardedClient;
import com.taylor.redis.service.impl.RedisShardedClientFactory;

import redis.clients.util.MurmurHash;

public class RedisShardClientDemo {

	public static void main(String[] args) throws Exception {

		// master-10.48.49.41:6301 slaver-10.52.132.56:6301
		String host_m_1 = "115.159.110.85";
		int port_m_1 = 6379;
		String host_s_1 = "115.159.110.85";
		int port_s_1 = 6379;
		String host_m_2 = "115.159.53.124";
		int port_m_2 = 6379;
		String host_s_2 = "115.159.53.124";
		int port_s_2 = 6379;

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

		RedisShardedClient client = (RedisShardedClient) factory.getObject();

		int i = 0;
		while (true) {
			i++;
			try {
				String key = "test_" + i;
				System.out.println("put [test_" + i + "=value=" + ": " + (1 + Math.abs(new MurmurHash().hash(key) % 2)));
				client.set(key, "value");
				String value = client.echo("test_" + (i - 1));
				System.out.println("get [test_" + i + "=value=" + ": " + value);
			} catch (Throwable e) {
				e.printStackTrace();
			}
			Thread.sleep(1000);
		}

		// client.set("test", "10000");
		// System.out.println(client.exists("test"));
		// System.out.println(client.get("test"));
		// client.del("test");
		// System.out.println(client.exists("test"));
		// System.out.println(client);

	}

}
