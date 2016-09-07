package com.taylor.redis.service.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.taylor.common.exceptions.CommonRuntimeException;
import com.taylor.redis.common.client.JedisCallBack;
import com.taylor.redis.common.shard.RedisSimplePool;
import com.taylor.redis.exception.RedisShardConnectException;
import com.taylor.redis.service.RedisClientService;

import lombok.extern.log4j.Log4j2;
import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.BitPosParams;
import redis.clients.jedis.GeoCoordinate;
import redis.clients.jedis.GeoRadiusResponse;
import redis.clients.jedis.GeoUnit;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.params.geo.GeoRadiusParam;
import redis.clients.jedis.params.sortedset.ZAddParams;
import redis.clients.jedis.params.sortedset.ZIncrByParams;
import redis.clients.util.SafeEncoder;

@Log4j2
public class RedisSimpleClient implements RedisClientService {

	public RedisSimpleClient(RedisSimplePool redisPool) {
		this.redisPool = redisPool;
	}

	public RedisSimpleClient() {

	}

	private RedisSimplePool redisPool;

	public <E> E doOperation(String key, JedisCallBack<E> callback) {
		return this.doOperation(SafeEncoder.encode(key), callback);
	}

	public <E> E doOperation(byte[] key, JedisCallBack<E> callback) {
		Jedis jedis = null;
		try {
			jedis = redisPool.getResource();
			E rs = callback.doBiz(jedis);
			return rs;
		} catch (JedisConnectionException e) {
			log.error("jedis connection Exception", e);
			throw new RedisShardConnectException("Faild when execute operation  " + callback.getOperationName(), e);
		} catch (Exception e1) {
			log.error("Do operation failed for key :=" + key, e1);
			throw new CommonRuntimeException("Do operation failed for key :=" + key, e1);
		} finally {
			if (null != redisPool && null != jedis)
				redisPool.returnResource(jedis);
		}
	}

	public String setBatch(final int DBIndex, final Map<String, String> values) {
		return doOperation("batch", new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				j.select(DBIndex);
				Pipeline p = j.pipelined();
				Set<String> keys = values.keySet();
				Iterator<String> iterator = keys.iterator();
				while (iterator.hasNext()) {
					String key = iterator.next();
					p.set(key, values.get(key));
				}
				p.sync();
				return values.toString();
			}

			public String getOperationName() {
				return "setBatch";
			}
		});
	}

	public Map<String, String> getBatch(final int DBIndex, final List<String> keys) {
		return doOperation("batch", new JedisCallBack<Map<String, String>>() {

			public Map<String, String> doBiz(Jedis j) {
				Map<String, String> result = new HashMap<String, String>();
				Map<String, Response<String>> resultR = new HashMap<String, Response<String>>();
				j.select(DBIndex);
				Pipeline p = j.pipelined();
				for (int i = 0; i < keys.size(); i++) {
					resultR.put(keys.get(i), p.get(keys.get(i)));
				}
				p.sync();
				Set<String> sets = resultR.keySet();
				Iterator<String> iterator = sets.iterator();
				while (iterator.hasNext()) {
					String key = iterator.next();
					result.put(key, resultR.get(key).get());
				}
				return result;
			}

			public String getOperationName() {
				return "getBatch";
			}
		});
	}

	public String hsetBatch(final int DBIndex, final String key, final Map<String, String> values) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				j.select(DBIndex);
				Pipeline p = j.pipelined();
				Set<String> keys = values.keySet();
				Iterator<String> iterator = keys.iterator();
				while (iterator.hasNext()) {
					String fiedlKey = iterator.next();
					p.hset(key, fiedlKey, values.get(key));
				}
				p.sync();
				return values.toString();
			}

			public String getOperationName() {
				return "hsetBatch";
			}
		});
	}

	public String set(final String key, final String value) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				return j.set(key, value);
			}

			public String getOperationName() {
				return "set";
			}
		});
	}

	public String get(final String key) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				return j.get(key);
			}

			public String getOperationName() {
				return "get";
			}
		});
	}

	public Boolean exists(final String key) {
		return doOperation(key, new JedisCallBack<Boolean>() {

			public Boolean doBiz(Jedis j) {
				return j.exists(key);
			}

			public String getOperationName() {
				return "echo";
			}
		});
	}

	public String type(final String key) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				return j.type(key);
			}

			public String getOperationName() {
				return "type";
			}
		});
	}

	public Long expire(final String key, final int seconds) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.expire(key, seconds);
			}

			public String getOperationName() {
				return "expire";
			}
		});
	}

	public Long expireAt(final String key, final long unixTime) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.expireAt(key, unixTime);
			}

			public String getOperationName() {
				return "expireAt";
			}
		});
	}

	public Long ttl(final String key) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.ttl(key);
			}

			public String getOperationName() {
				return "ttl";
			}
		});
	}

	public Boolean setbit(final String key, final long offset, final boolean value) {
		return doOperation(key, new JedisCallBack<Boolean>() {

			public Boolean doBiz(Jedis j) {
				return j.setbit(key, offset, value);
			}

			public String getOperationName() {
				return "setbit";
			}
		});
	}

	public Boolean setbit(final String key, final long offset, final String value) {
		return doOperation(key, new JedisCallBack<Boolean>() {

			public Boolean doBiz(Jedis j) {
				return j.setbit(SafeEncoder.encode(key), offset, SafeEncoder.encode(value));
			}

			public String getOperationName() {
				return "setbit";
			}
		});
	}

	public Boolean getbit(final String key, final long offset) {
		return doOperation(key, new JedisCallBack<Boolean>() {

			public Boolean doBiz(Jedis j) {
				return j.getbit(key, offset);
			}

			public String getOperationName() {
				return "getbit";
			}
		});
	}

	public Long setrange(final String key, final long offset, final String value) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.setrange(key, offset, value);
			}

			public String getOperationName() {
				return "setrange";
			}
		});
	}

	public String getrange(final String key, final long startOffset, final long endOffset) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				return j.getrange(key, startOffset, endOffset);
			}

			public String getOperationName() {
				return "getrange";
			}
		});
	}

	public String getSet(final String key, final String value) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				return j.getSet(key, value);
			}

			public String getOperationName() {
				return "getSet";
			}
		});
	}

	public Long setnx(final String key, final String value) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.setnx(key, value);
			}

			public String getOperationName() {
				return "setnx";
			}
		});
	}

	public String setex(final String key, final int seconds, final String value) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				return j.setex(key, seconds, value);
			}

			public String getOperationName() {
				return "setex";
			}
		});
	}

	public Long decrBy(final String key, final long integer) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.decrBy(key, integer);
			}

			public String getOperationName() {
				return "decrBy";
			}
		});
	}

	public Long decr(final String key) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.decr(key);
			}

			public String getOperationName() {
				return "decr";
			}
		});
	}

	public Long incrBy(final String key, final long integer) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.incrBy(key, integer);
			}

			public String getOperationName() {
				return "incrBy";
			}
		});
	}

	public Long incr(final String key) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.incr(key);
			}

			public String getOperationName() {
				return "incr";
			}
		});
	}

	public Long append(final String key, final String value) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.append(key, value);
			}

			public String getOperationName() {
				return "append";
			}
		});
	}

	public String substr(final String key, final int start, final int end) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				return j.substr(key, start, end);
			}

			public String getOperationName() {
				return "substr";
			}
		});
	}

	public Long hset(final String key, final String field, final String value) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.hset(key, field, value);
			}

			public String getOperationName() {
				return "hset";
			}
		});
	}

	public String hget(final String key, final String field) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				return j.hget(key, field);
			}

			public String getOperationName() {
				return "hget";
			}
		});
	}

	public Long hsetnx(final String key, final String field, final String value) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.hsetnx(key, field, value);
			}

			public String getOperationName() {
				return "hsetnx";
			}
		});
	}

	public String hmset(final String key, final Map<String, String> hash) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				return j.hmset(key, hash);
			}

			public String getOperationName() {
				return "hmset";
			}
		});
	}

	public List<String> hmget(final String key, final String... fields) {
		return doOperation(key, new JedisCallBack<List<String>>() {

			public List<String> doBiz(Jedis j) {
				return j.hmget(key, fields);
			}

			public String getOperationName() {
				return "hmget";
			}
		});
	}

	public Long hincrBy(final String key, final String field, final long value) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.hincrBy(key, field, value);
			}

			public String getOperationName() {
				return "hincrBy";
			}
		});
	}

	public Boolean hexists(final String key, final String field) {
		return doOperation(key, new JedisCallBack<Boolean>() {

			public Boolean doBiz(Jedis j) {
				return j.hexists(key, field);
			}

			public String getOperationName() {
				return "hexists";
			}
		});
	}

	public Long del(final String key) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.del(key);
			}

			public String getOperationName() {
				return "del";
			}
		});
	}

	public Long hdel(final String key, final String... fields) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.hdel(key, fields);
			}

			public String getOperationName() {
				return "hdel";
			}
		});
	}

	public Long hlen(final String key) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.hlen(key);
			}

			public String getOperationName() {
				return "hlen";
			}
		});
	}

	public Set<String> hkeys(final String key) {
		return doOperation(key, new JedisCallBack<Set<String>>() {

			public Set<String> doBiz(Jedis j) {
				return j.hkeys(key);
			}

			public String getOperationName() {
				return "hkeys";
			}
		});
	}

	public List<String> hvals(final String key) {
		return doOperation(key, new JedisCallBack<List<String>>() {

			public List<String> doBiz(Jedis j) {
				return j.hvals(key);
			}

			public String getOperationName() {
				return "hvals";
			}
		});
	}

	public Map<String, String> hgetAll(final String key) {
		return doOperation(key, new JedisCallBack<Map<String, String>>() {

			public Map<String, String> doBiz(Jedis j) {
				return j.hgetAll(key);
			}

			public String getOperationName() {
				return "hgetAll";
			}
		});
	}

	public Long rpush(final String key, final String... strings) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.rpush(key, strings);
			}

			public String getOperationName() {
				return "rpush";
			}
		});
	}

	public Long lpush(final String key, final String... strings) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.lpush(key, strings);
			}

			public String getOperationName() {
				return "lpush";
			}
		});
	}

	public Long lpushx(final String key, final String string) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.lpushx(key, string);
			}

			public String getOperationName() {
				return "lpushx";
			}
		});
	}

	public Long strlen(final String key) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.strlen(key);
			}

			public String getOperationName() {
				return "strlen";
			}
		});
	}

	public Long move(final String key, final int dbIndex) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.move(key, dbIndex);
			}

			public String getOperationName() {
				return "move";
			}
		});
	}

	public Long rpushx(final String key, final String string) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.rpushx(key, string);
			}

			public String getOperationName() {
				return "rpushx";
			}
		});
	}

	public Long persist(final String key) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.persist(key);
			}

			public String getOperationName() {
				return "persist";
			}
		});
	}

	public Long llen(final String key) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.llen(key);
			}

			public String getOperationName() {
				return "llen";
			}
		});
	}

	public List<String> lrange(final String key, final long start, final long end) {
		return doOperation(key, new JedisCallBack<List<String>>() {

			public List<String> doBiz(Jedis j) {
				return j.lrange(key, start, end);
			}

			public String getOperationName() {
				return "lrange";
			}
		});
	}

	public String ltrim(final String key, final long start, final long end) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				return j.ltrim(key, start, end);
			}

			public String getOperationName() {
				return "ltrim";
			}
		});
	}

	public String lindex(final String key, final long index) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				return j.lindex(key, index);
			}

			public String getOperationName() {
				return "lindex";
			}
		});
	}

	public String lset(final String key, final long index, final String value) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				return j.lset(key, index, value);
			}

			public String getOperationName() {
				return "lset";
			}
		});
	}

	public Long lrem(final String key, final long count, final String value) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.lrem(key, count, value);
			}

			public String getOperationName() {
				return "lrem";
			}
		});
	}

	public String lpop(final String key) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				return j.lpop(key);
			}

			public String getOperationName() {
				return "lpop";
			}
		});
	}

	public String rpop(final String key) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				return j.rpop(key);
			}

			public String getOperationName() {
				return "rpop";
			}
		});
	}

	public Long sadd(final String key, final String... members) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.sadd(key, members);
			}

			public String getOperationName() {
				return "sadd";
			}
		});
	}

	public Set<String> smembers(final String key) {
		return doOperation(key, new JedisCallBack<Set<String>>() {

			public Set<String> doBiz(Jedis j) {
				return j.smembers(key);
			}

			public String getOperationName() {
				return "smembers";
			}
		});
	}

	public Long srem(final String key, final String... members) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.srem(key, members);
			}

			public String getOperationName() {
				return "srem";
			}
		});
	}

	public String spop(final String key) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				return j.spop(key);
			}

			public String getOperationName() {
				return "spop";
			}
		});
	}

	public Long scard(final String key) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.scard(key);
			}

			public String getOperationName() {
				return "scard";
			}
		});
	}

	public Boolean sismember(final String key, final String member) {
		return doOperation(key, new JedisCallBack<Boolean>() {

			public Boolean doBiz(Jedis j) {
				return j.sismember(key, member);
			}

			public String getOperationName() {
				return "sismember";
			}
		});
	}

	public String srandmember(final String key) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				return j.srandmember(key);
			}

			public String getOperationName() {
				return "srandmember";
			}
		});
	}

	public Long zadd(final String key, final double score, final String member) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.zadd(key, score, member);
			}

			public String getOperationName() {
				return "zadd";
			}
		});
	}

	public Long zadd(final String key, final Map<String, Double> scoreMembers) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.zadd(key, scoreMembers);
			}

			public String getOperationName() {
				return "zadd";
			}
		});
	}

	public Set<String> zrange(final String key, final long start, final long end) {
		return doOperation(key, new JedisCallBack<Set<String>>() {

			public Set<String> doBiz(Jedis j) {
				return j.zrange(key, start, end);
			}

			public String getOperationName() {
				return "zrange";
			}
		});
	}

	public Long zrem(final String key, final String... members) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.zrem(key, members);
			}

			public String getOperationName() {
				return "zrem";
			}
		});
	}

	public Double zincrby(final String key, final double score, final String member) {
		return doOperation(key, new JedisCallBack<Double>() {

			public Double doBiz(Jedis j) {
				return j.zincrby(key, score, member);
			}

			public String getOperationName() {
				return "zincrby";
			}
		});
	}

	public Long zrank(final String key, final String member) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.zrank(key, member);
			}

			public String getOperationName() {
				return "zrank";
			}
		});
	}

	public Long zrevrank(final String key, final String member) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.zrevrank(key, member);
			}

			public String getOperationName() {
				return "zrevrank";
			}
		});
	}

	public Set<String> zrevrange(final String key, final long start, final long end) {
		return doOperation(key, new JedisCallBack<Set<String>>() {

			public Set<String> doBiz(Jedis j) {
				return j.zrevrange(key, start, end);
			}

			public String getOperationName() {
				return "zrevrange";
			}
		});
	}

	public Set<Tuple> zrangeWithScores(final String key, final long start, final long end) {
		return doOperation(key, new JedisCallBack<Set<Tuple>>() {

			public Set<Tuple> doBiz(Jedis j) {
				return j.zrangeWithScores(key, start, end);
			}

			public String getOperationName() {
				return "zrangeWithScores";
			}
		});
	}

	public Set<Tuple> zrevrangeWithScores(final String key, final long start, final long end) {
		return doOperation(key, new JedisCallBack<Set<Tuple>>() {

			public Set<Tuple> doBiz(Jedis j) {
				return j.zrevrangeWithScores(key, start, end);
			}

			public String getOperationName() {
				return "zrevrangeWithScores";
			}
		});
	}

	public Long zcard(final String key) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.zcard(key);
			}

			public String getOperationName() {
				return "zcard";
			}
		});
	}

	public Double zscore(final String key, final String member) {
		return doOperation(key, new JedisCallBack<Double>() {

			public Double doBiz(Jedis j) {
				return j.zscore(key, member);
			}

			public String getOperationName() {
				return "zscore";
			}
		});
	}

	public List<String> sort(final String key) {
		return doOperation(key, new JedisCallBack<List<String>>() {

			public List<String> doBiz(Jedis j) {
				return j.sort(key);
			}

			public String getOperationName() {
				return "sort";
			}
		});
	}

	public List<String> sort(final String key, final SortingParams sortingParameters) {
		return doOperation(key, new JedisCallBack<List<String>>() {

			public List<String> doBiz(Jedis j) {
				return j.sort(key, sortingParameters);
			}

			public String getOperationName() {
				return "sort";
			}
		});
	}

	public Long zcount(final String key, final double min, final double max) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.zcount(key, min, max);
			}

			public String getOperationName() {
				return "zcount";
			}
		});
	}

	public Long zcount(final String key, final String min, final String max) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.zcount(key, min, max);
			}

			public String getOperationName() {
				return "zcount";
			}
		});
	}

	public Set<String> zrangeByScore(final String key, final double min, final double max) {
		return doOperation(key, new JedisCallBack<Set<String>>() {

			public Set<String> doBiz(Jedis j) {
				return j.zrangeByScore(key, min, max);
			}

			public String getOperationName() {
				return "zrangeByScore";
			}
		});
	}

	public Set<String> zrevrangeByScore(final String key, final double max, final double min) {
		return doOperation(key, new JedisCallBack<Set<String>>() {

			public Set<String> doBiz(Jedis j) {
				return j.zrevrangeByScore(key, max, min);
			}

			public String getOperationName() {
				return "zrevrangeByScore";
			}
		});
	}

	public Set<String> zrangeByScore(final String key, final double min, final double max, final int offset, final int count) {
		return doOperation(key, new JedisCallBack<Set<String>>() {

			public Set<String> doBiz(Jedis j) {
				return j.zrangeByScore(key, min, max, offset, count);
			}

			public String getOperationName() {
				return "zrangeByScore";
			}
		});
	}

	public Set<String> zrevrangeByScore(final String key, final double max, final double min, final int offset, final int count) {
		return doOperation(key, new JedisCallBack<Set<String>>() {

			public Set<String> doBiz(Jedis j) {
				return j.zrevrangeByScore(key, max, min, offset, count);
			}

			public String getOperationName() {
				return "zrevrangeByScore";
			}
		});
	}

	public Set<Tuple> zrangeByScoreWithScores(final String key, final double min, final double max) {
		return doOperation(key, new JedisCallBack<Set<Tuple>>() {

			public Set<Tuple> doBiz(Jedis j) {
				return j.zrangeByScoreWithScores(key, min, max);
			}

			public String getOperationName() {
				return "zrangeByScoreWithScores";
			}
		});
	}

	public Set<Tuple> zrevrangeByScoreWithScores(final String key, final double max, final double min) {
		return doOperation(key, new JedisCallBack<Set<Tuple>>() {

			public Set<Tuple> doBiz(Jedis j) {

				return j.zrevrangeByScoreWithScores(key, max, min);
			}

			public String getOperationName() {
				return "zrevrangeByScoreWithScores";
			}
		});
	}

	public Set<Tuple> zrangeByScoreWithScores(final String key, final double min, final double max, final int offset, final int count) {
		return doOperation(key, new JedisCallBack<Set<Tuple>>() {

			public Set<Tuple> doBiz(Jedis j) {
				return j.zrangeByScoreWithScores(key, min, max, offset, count);
			}

			public String getOperationName() {
				return "zrangeByScoreWithScores";
			}
		});
	}

	public Set<Tuple> zrevrangeByScoreWithScores(final String key, final double max, final double min, final int offset, final int count) {
		return doOperation(key, new JedisCallBack<Set<Tuple>>() {

			public Set<Tuple> doBiz(Jedis j) {
				return j.zrevrangeByScoreWithScores(key, max, min, offset, count);
			}

			public String getOperationName() {
				return "zrevrangeByScoreWithScores";
			}
		});
	}

	public Set<String> zrangeByScore(final String key, final String min, final String max) {
		return doOperation(key, new JedisCallBack<Set<String>>() {

			public Set<String> doBiz(Jedis j) {
				return j.zrangeByScore(key, min, max);
			}

			public String getOperationName() {
				return "zrangeByScore";
			}
		});
	}

	public Set<String> zrevrangeByScore(final String key, final String max, final String min) {
		return doOperation(key, new JedisCallBack<Set<String>>() {

			public Set<String> doBiz(Jedis j) {
				return j.zrevrangeByScore(key, max, min);
			}

			public String getOperationName() {
				return "zrevrangeByScore";
			}
		});
	}

	public Set<String> zrangeByScore(final String key, final String min, final String max, final int offset, final int count) {
		return doOperation(key, new JedisCallBack<Set<String>>() {

			public Set<String> doBiz(Jedis j) {
				return j.zrangeByScore(key, min, max, offset, count);
			}

			public String getOperationName() {
				return "zrangeByScore";
			}
		});
	}

	public Set<String> zrevrangeByScore(final String key, final String max, final String min, final int offset, final int count) {
		return doOperation(key, new JedisCallBack<Set<String>>() {

			public Set<String> doBiz(Jedis j) {
				return j.zrevrangeByScore(key, max, min, offset, count);
			}

			public String getOperationName() {
				return "zrevrangeByScore";
			}
		});
	}

	public Set<Tuple> zrangeByScoreWithScores(final String key, final String min, final String max) {
		return doOperation(key, new JedisCallBack<Set<Tuple>>() {

			public Set<Tuple> doBiz(Jedis j) {
				return j.zrangeByScoreWithScores(key, min, max);
			}

			public String getOperationName() {
				return "zrangeByScoreWithScores";
			}
		});
	}

	public Set<Tuple> zrevrangeByScoreWithScores(final String key, final String max, final String min) {
		return doOperation(key, new JedisCallBack<Set<Tuple>>() {

			public Set<Tuple> doBiz(Jedis j) {
				return j.zrevrangeByScoreWithScores(key, max, min);
			}

			public String getOperationName() {
				return "zrevrangeByScoreWithScores";
			}
		});
	}

	public Set<Tuple> zrangeByScoreWithScores(final String key, final String min, final String max, final int offset, final int count) {
		return doOperation(key, new JedisCallBack<Set<Tuple>>() {

			public Set<Tuple> doBiz(Jedis j) {
				return j.zrangeByScoreWithScores(key, min, max, offset, count);
			}

			public String getOperationName() {
				return "zrangeByScoreWithScores";
			}
		});
	}

	public Set<Tuple> zrevrangeByScoreWithScores(final String key, final String max, final String min, final int offset, final int count) {
		return doOperation(key, new JedisCallBack<Set<Tuple>>() {

			public Set<Tuple> doBiz(Jedis j) {
				return j.zrevrangeByScoreWithScores(key, max, min, offset, count);
			}

			public String getOperationName() {
				return "zrevrangeByScoreWithScores";
			}
		});
	}

	public Long zremrangeByRank(final String key, final long start, final long end) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.zremrangeByRank(key, start, end);
			}

			public String getOperationName() {
				return "zremrangeByRank";
			}
		});
	}

	public Long zremrangeByScore(final String key, final double start, final double end) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.zremrangeByScore(key, start, end);
			}

			public String getOperationName() {
				return "zremrangeByScore";
			}
		});
	}

	public Long zremrangeByScore(final String key, final String start, final String end) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.zremrangeByScore(key, start, end);
			}

			public String getOperationName() {
				return "zremrangeByScore";
			}
		});
	}

	public Long linsert(final String key, final LIST_POSITION where, final String pivot, final String value) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				return j.linsert(key, where, pivot, value);
			}

			public String getOperationName() {
				return "linsert";
			}
		});
	}

	/**
	 * ********* EXT API ***************
	 */
	public String set(final String key, final Object value) {

		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				byte[] byteValue = serialize(value);
				return j.set(SafeEncoder.encode(key), byteValue);
			}

			public String getOperationName() {
				return "set";
			}
		});

	}

	public Object getObject(final String key) {

		return doOperation(key, new JedisCallBack<Object>() {

			public Object doBiz(Jedis j) {
				byte[] result = j.get(SafeEncoder.encode(key));
				Object object = deserialize(result);
				return object;
			}

			public String getOperationName() {
				return "getObject";
			}
		});

	}

	public String info(final String key) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				return j.info(key);
			}

			public String getOperationName() {
				return "info";
			}
		});

	}

	public String set(final int DBIndex, final String key, final String value) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				j.select(DBIndex);
				return j.set(key, value);
			}

			public String getOperationName() {
				return "set";
			}
		});
	}

	public String set(final int DBIndex, final String key, final int seconds, final String value) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				j.select(DBIndex);
				return j.setex(key, seconds, value);
			}

			public String getOperationName() {
				return "set";
			}
		});
	}

	public String get(final int DBIndex, final String key) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				j.select(DBIndex);
				return j.get(key);
			}

			public String getOperationName() {
				return "get";
			}
		});
	}

	public Boolean exists(final int DBIndex, final String key) {
		return doOperation(key, new JedisCallBack<Boolean>() {

			public Boolean doBiz(Jedis j) {
				j.select(DBIndex);
				return j.exists(key);
			}

			public String getOperationName() {
				return "echo";
			}
		});
	}

	public String type(final int DBIndex, final String key) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				j.select(DBIndex);
				return j.type(key);
			}

			public String getOperationName() {
				return "type";
			}
		});
	}

	public Long expire(final int DBIndex, final String key, final int seconds) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.expire(key, seconds);
			}

			public String getOperationName() {
				return "expire";
			}
		});
	}

	public Long expireAt(final int DBIndex, final String key, final long unixTime) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.expireAt(key, unixTime);
			}

			public String getOperationName() {
				return "expireAt";
			}
		});
	}

	public Long ttl(final int DBIndex, final String key) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.ttl(key);
			}

			public String getOperationName() {
				return "ttl";
			}
		});
	}

	public Boolean setbit(final int DBIndex, final String key, final long offset, final boolean value) {
		return doOperation(key, new JedisCallBack<Boolean>() {

			public Boolean doBiz(Jedis j) {
				j.select(DBIndex);
				return j.setbit(key, offset, value);
			}

			public String getOperationName() {
				return "setbit";
			}
		});
	}

	public Boolean setbit(final int DBIndex, final String key, final long offset, final String value) {
		return doOperation(key, new JedisCallBack<Boolean>() {

			public Boolean doBiz(Jedis j) {
				j.select(DBIndex);
				return j.setbit(SafeEncoder.encode(key), offset, SafeEncoder.encode(value));
			}

			public String getOperationName() {
				return "setbit";
			}
		});
	}

	public Boolean getbit(final int DBIndex, final String key, final long offset) {
		return doOperation(key, new JedisCallBack<Boolean>() {

			public Boolean doBiz(Jedis j) {
				j.select(DBIndex);
				return j.getbit(key, offset);
			}

			public String getOperationName() {
				return "getbit";
			}
		});
	}

	public Long setrange(final int DBIndex, final String key, final long offset, final String value) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.setrange(key, offset, value);
			}

			public String getOperationName() {
				return "setrange";
			}
		});
	}

	public String getrange(final int DBIndex, final String key, final long startOffset, final long endOffset) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				j.select(DBIndex);
				return j.getrange(key, startOffset, endOffset);
			}

			public String getOperationName() {
				return "getrange";
			}
		});
	}

	public String getSet(final int DBIndex, final String key, final String value) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				j.select(DBIndex);
				return j.getSet(key, value);
			}

			public String getOperationName() {
				return "getSet";
			}
		});
	}

	public Long setnx(final int DBIndex, final String key, final String value) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.setnx(key, value);
			}

			public String getOperationName() {
				return "setnx";
			}
		});
	}

	public String setex(final int DBIndex, final String key, final int seconds, final String value) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				j.select(DBIndex);
				return j.setex(key, seconds, value);
			}

			public String getOperationName() {
				return "setex";
			}
		});
	}

	public Long decrBy(final int DBIndex, final String key, final long integer) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.decrBy(key, integer);
			}

			public String getOperationName() {
				return "decrBy";
			}
		});
	}

	public Long decr(final int DBIndex, final String key) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.decr(key);
			}

			public String getOperationName() {
				return "decr";
			}
		});
	}

	public Long incrBy(final int DBIndex, final String key, final long integer) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.incrBy(key, integer);
			}

			public String getOperationName() {
				return "incrBy";
			}
		});
	}

	public Long incr(final int DBIndex, final String key) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.incr(key);
			}

			public String getOperationName() {
				return "incr";
			}
		});
	}

	public Long append(final int DBIndex, final String key, final String value) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.append(key, value);
			}

			public String getOperationName() {
				return "append";
			}
		});
	}

	public String substr(final int DBIndex, final String key, final int start, final int end) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				j.select(DBIndex);
				return j.substr(key, start, end);
			}

			public String getOperationName() {
				return "substr";
			}
		});
	}

	public Long hset(final int DBIndex, final String key, final String field, final String value) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.hset(key, field, value);
			}

			public String getOperationName() {
				return "hset";
			}
		});
	}

	public String hget(final int DBIndex, final String key, final String field) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				j.select(DBIndex);
				return j.hget(key, field);
			}

			public String getOperationName() {
				return "hget";
			}
		});
	}

	public Long hsetnx(final int DBIndex, final String key, final String field, final String value) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.hsetnx(key, field, value);
			}

			public String getOperationName() {
				return "hsetnx";
			}
		});
	}

	public String hmset(final int DBIndex, final String key, final Map<String, String> hash) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				j.select(DBIndex);
				return j.hmset(key, hash);
			}

			public String getOperationName() {
				return "hmset";
			}
		});
	}

	public List<String> hmget(final int DBIndex, final String key, final String... fields) {
		return doOperation(key, new JedisCallBack<List<String>>() {

			public List<String> doBiz(Jedis j) {
				j.select(DBIndex);
				return j.hmget(key, fields);
			}

			public String getOperationName() {
				return "hmget";
			}
		});
	}

	public Long hincrBy(final int DBIndex, final String key, final String field, final long value) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.hincrBy(key, field, value);
			}

			public String getOperationName() {
				return "hincrBy";
			}
		});
	}

	public Boolean hexists(final int DBIndex, final String key, final String field) {
		return doOperation(key, new JedisCallBack<Boolean>() {

			public Boolean doBiz(Jedis j) {
				j.select(DBIndex);
				return j.hexists(key, field);
			}

			public String getOperationName() {
				return "hexists";
			}
		});
	}

	public Long del(final int DBIndex, final String key) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.del(key);
			}

			public String getOperationName() {
				return "del";
			}
		});
	}

	public Long hdel(final int DBIndex, final String key, final String... fields) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.hdel(key, fields);
			}

			public String getOperationName() {
				return "hdel";
			}
		});
	}

	public Long hlen(final int DBIndex, final String key) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.hlen(key);
			}

			public String getOperationName() {
				return "hlen";
			}
		});
	}

	public Set<String> hkeys(final int DBIndex, final String key) {
		return doOperation(key, new JedisCallBack<Set<String>>() {

			public Set<String> doBiz(Jedis j) {
				j.select(DBIndex);
				return j.hkeys(key);
			}

			public String getOperationName() {
				return "hkeys";
			}
		});
	}

	public List<String> hvals(final int DBIndex, final String key) {
		return doOperation(key, new JedisCallBack<List<String>>() {

			public List<String> doBiz(Jedis j) {
				j.select(DBIndex);
				return j.hvals(key);
			}

			public String getOperationName() {
				return "hvals";
			}
		});
	}

	public Map<String, String> hgetAll(final int DBIndex, final String key) {
		return doOperation(key, new JedisCallBack<Map<String, String>>() {

			public Map<String, String> doBiz(Jedis j) {
				j.select(DBIndex);
				return j.hgetAll(key);
			}

			public String getOperationName() {
				return "hgetAll";
			}
		});
	}

	public Long rpush(final int DBIndex, final String key, final String... strings) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.rpush(key, strings);
			}

			public String getOperationName() {
				return "rpush";
			}
		});
	}

	public Long lpush(final int DBIndex, final String key, final String... strings) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.lpush(key, strings);
			}

			public String getOperationName() {
				return "lpush";
			}
		});
	}

	public Long lpushx(final int DBIndex, final String key, final String string) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.lpushx(key, string);
			}

			public String getOperationName() {
				return "lpushx";
			}
		});
	}

	public Long strlen(final int DBIndex, final String key) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.strlen(key);
			}

			public String getOperationName() {
				return "strlen";
			}
		});
	}

	public Long move(final int DBIndex, final String key, final int dbIndex) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.move(key, dbIndex);
			}

			public String getOperationName() {
				return "move";
			}
		});
	}

	public Long rpushx(final int DBIndex, final String key, final String string) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.rpushx(key, string);
			}

			public String getOperationName() {
				return "rpushx";
			}
		});
	}

	public Long persist(final int DBIndex, final String key) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.persist(key);
			}

			public String getOperationName() {
				return "persist";
			}
		});
	}

	public Long llen(final int DBIndex, final String key) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.llen(key);
			}

			public String getOperationName() {
				return "llen";
			}
		});
	}

	public List<String> lrange(final int DBIndex, final String key, final long start, final long end) {
		return doOperation(key, new JedisCallBack<List<String>>() {

			public List<String> doBiz(Jedis j) {
				j.select(DBIndex);
				return j.lrange(key, start, end);
			}

			public String getOperationName() {
				return "lrange";
			}
		});
	}

	public String ltrim(final int DBIndex, final String key, final long start, final long end) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				j.select(DBIndex);
				return j.ltrim(key, start, end);
			}

			public String getOperationName() {
				return "ltrim";
			}
		});
	}

	public String lindex(final int DBIndex, final String key, final long index) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				j.select(DBIndex);
				return j.lindex(key, index);
			}

			public String getOperationName() {
				return "lindex";
			}
		});
	}

	public String lset(final int DBIndex, final String key, final long index, final String value) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				j.select(DBIndex);
				return j.lset(key, index, value);
			}

			public String getOperationName() {
				return "lset";
			}
		});
	}

	public Long lrem(final int DBIndex, final String key, final long count, final String value) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.lrem(key, count, value);
			}

			public String getOperationName() {
				return "lrem";
			}
		});
	}

	public String lpop(final int DBIndex, final String key) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				j.select(DBIndex);
				return j.lpop(key);
			}

			public String getOperationName() {
				return "lpop";
			}
		});
	}

	public String rpop(final int DBIndex, final String key) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				j.select(DBIndex);
				return j.rpop(key);
			}

			public String getOperationName() {
				return "rpop";
			}
		});
	}

	public Long sadd(final int DBIndex, final String key, final String... members) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.sadd(key, members);
			}

			public String getOperationName() {
				return "sadd";
			}
		});
	}

	public Set<String> smembers(final int DBIndex, final String key) {
		return doOperation(key, new JedisCallBack<Set<String>>() {

			public Set<String> doBiz(Jedis j) {
				j.select(DBIndex);
				return j.smembers(key);
			}

			public String getOperationName() {
				return "smembers";
			}
		});
	}

	public Long srem(final int DBIndex, final String key, final String... members) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.srem(key, members);
			}

			public String getOperationName() {
				return "srem";
			}
		});
	}

	public String spop(final int DBIndex, final String key) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				j.select(DBIndex);
				return j.spop(key);
			}

			public String getOperationName() {
				return "spop";
			}
		});
	}

	public Long scard(final int DBIndex, final String key) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.scard(key);
			}

			public String getOperationName() {
				return "scard";
			}
		});
	}

	public Boolean sismember(final int DBIndex, final String key, final String member) {
		return doOperation(key, new JedisCallBack<Boolean>() {

			public Boolean doBiz(Jedis j) {
				j.select(DBIndex);
				return j.sismember(key, member);
			}

			public String getOperationName() {
				return "sismember";
			}
		});
	}

	public String srandmember(final int DBIndex, final String key) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				j.select(DBIndex);
				return j.srandmember(key);
			}

			public String getOperationName() {
				return "srandmember";
			}
		});
	}

	public Long zadd(final int DBIndex, final String key, final double score, final String member) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.zadd(key, score, member);
			}

			public String getOperationName() {
				return "zadd";
			}
		});
	}

	public Long zadd(final int DBIndex, final String key, final Map<String, Double> scoreMembers) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.zadd(key, scoreMembers);
			}

			public String getOperationName() {
				return "zadd";
			}
		});
	}

	public Set<String> zrange(final int DBIndex, final String key, final long start, final long end) {
		return doOperation(key, new JedisCallBack<Set<String>>() {

			public Set<String> doBiz(Jedis j) {
				j.select(DBIndex);
				return j.zrange(key, start, end);
			}

			public String getOperationName() {
				return "zrange";
			}
		});
	}

	public Long zrem(final int DBIndex, final String key, final String... members) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.zrem(key, members);
			}

			public String getOperationName() {
				return "zrem";
			}
		});
	}

	public Double zincrby(final int DBIndex, final String key, final double score, final String member) {
		return doOperation(key, new JedisCallBack<Double>() {

			public Double doBiz(Jedis j) {
				j.select(DBIndex);
				return j.zincrby(key, score, member);
			}

			public String getOperationName() {
				return "zincrby";
			}
		});
	}

	public Long zrank(final int DBIndex, final String key, final String member) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.zrank(key, member);
			}

			public String getOperationName() {
				return "zrank";
			}
		});
	}

	public Long zrevrank(final int DBIndex, final String key, final String member) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.zrevrank(key, member);
			}

			public String getOperationName() {
				return "zrevrank";
			}
		});
	}

	public Set<String> zrevrange(final int DBIndex, final String key, final long start, final long end) {
		return doOperation(key, new JedisCallBack<Set<String>>() {

			public Set<String> doBiz(Jedis j) {
				j.select(DBIndex);
				return j.zrevrange(key, start, end);
			}

			public String getOperationName() {
				return "zrevrange";
			}
		});
	}

	public Set<Tuple> zrangeWithScores(final int DBIndex, final String key, final long start, final long end) {
		return doOperation(key, new JedisCallBack<Set<Tuple>>() {

			public Set<Tuple> doBiz(Jedis j) {
				j.select(DBIndex);
				return j.zrangeWithScores(key, start, end);
			}

			public String getOperationName() {
				return "zrangeWithScores";
			}
		});
	}

	public Set<Tuple> zrevrangeWithScores(final int DBIndex, final String key, final long start, final long end) {
		return doOperation(key, new JedisCallBack<Set<Tuple>>() {

			public Set<Tuple> doBiz(Jedis j) {
				j.select(DBIndex);
				return j.zrevrangeWithScores(key, start, end);
			}

			public String getOperationName() {
				return "zrevrangeWithScores";
			}
		});
	}

	public Long zcard(final int DBIndex, final String key) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.zcard(key);
			}

			public String getOperationName() {
				return "zcard";
			}
		});
	}

	public Double zscore(final int DBIndex, final String key, final String member) {
		return doOperation(key, new JedisCallBack<Double>() {

			public Double doBiz(Jedis j) {
				j.select(DBIndex);
				return j.zscore(key, member);
			}

			public String getOperationName() {
				return "zscore";
			}
		});
	}

	public List<String> sort(final int DBIndex, final String key) {
		return doOperation(key, new JedisCallBack<List<String>>() {

			public List<String> doBiz(Jedis j) {
				j.select(DBIndex);
				return j.sort(key);
			}

			public String getOperationName() {
				return "sort";
			}
		});
	}

	public List<String> sort(final int DBIndex, final String key, final SortingParams sortingParameters) {
		return doOperation(key, new JedisCallBack<List<String>>() {

			public List<String> doBiz(Jedis j) {
				j.select(DBIndex);
				return j.sort(key, sortingParameters);
			}

			public String getOperationName() {
				return "sort";
			}
		});
	}

	public Long zcount(final int DBIndex, final String key, final double min, final double max) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.zcount(key, min, max);
			}

			public String getOperationName() {
				return "zcount";
			}
		});
	}

	public Long zcount(final int DBIndex, final String key, final String min, final String max) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.zcount(key, min, max);
			}

			public String getOperationName() {
				return "zcount";
			}
		});
	}

	public Set<String> zrangeByScore(final int DBIndex, final String key, final double min, final double max) {
		return doOperation(key, new JedisCallBack<Set<String>>() {

			public Set<String> doBiz(Jedis j) {
				j.select(DBIndex);
				return j.zrangeByScore(key, min, max);
			}

			public String getOperationName() {
				return "zrangeByScore";
			}
		});
	}

	public Set<String> zrevrangeByScore(final int DBIndex, final String key, final double max, final double min) {
		return doOperation(key, new JedisCallBack<Set<String>>() {

			public Set<String> doBiz(Jedis j) {
				j.select(DBIndex);
				return j.zrevrangeByScore(key, max, min);
			}

			public String getOperationName() {
				return "zrevrangeByScore";
			}
		});
	}

	public Set<String> zrangeByScore(final int DBIndex, final String key, final double min, final double max, final int offset, final int count) {
		return doOperation(key, new JedisCallBack<Set<String>>() {

			public Set<String> doBiz(Jedis j) {
				j.select(DBIndex);
				return j.zrangeByScore(key, min, max, offset, count);
			}

			public String getOperationName() {
				return "zrangeByScore";
			}
		});
	}

	public Set<String> zrevrangeByScore(final int DBIndex, final String key, final double max, final double min, final int offset, final int count) {
		return doOperation(key, new JedisCallBack<Set<String>>() {

			public Set<String> doBiz(Jedis j) {
				j.select(DBIndex);
				return j.zrevrangeByScore(key, max, min, offset, count);
			}

			public String getOperationName() {
				return "zrevrangeByScore";
			}
		});
	}

	public Set<Tuple> zrangeByScoreWithScores(final int DBIndex, final String key, final double min, final double max) {
		return doOperation(key, new JedisCallBack<Set<Tuple>>() {

			public Set<Tuple> doBiz(Jedis j) {
				j.select(DBIndex);
				return j.zrangeByScoreWithScores(key, min, max);
			}

			public String getOperationName() {
				return "zrangeByScoreWithScores";
			}
		});
	}

	public Set<Tuple> zrevrangeByScoreWithScores(final int DBIndex, final String key, final double max, final double min) {
		return doOperation(key, new JedisCallBack<Set<Tuple>>() {

			public Set<Tuple> doBiz(Jedis j) {
				j.select(DBIndex);

				return j.zrevrangeByScoreWithScores(key, max, min);
			}

			public String getOperationName() {
				return "zrevrangeByScoreWithScores";
			}
		});
	}

	public Set<Tuple> zrangeByScoreWithScores(final int DBIndex, final String key, final double min, final double max, final int offset, final int count) {
		return doOperation(key, new JedisCallBack<Set<Tuple>>() {

			public Set<Tuple> doBiz(Jedis j) {
				j.select(DBIndex);
				return j.zrangeByScoreWithScores(key, min, max, offset, count);
			}

			public String getOperationName() {
				return "zrangeByScoreWithScores";
			}
		});
	}

	public Set<Tuple> zrevrangeByScoreWithScores(final int DBIndex, final String key, final double max, final double min, final int offset, final int count) {
		return doOperation(key, new JedisCallBack<Set<Tuple>>() {

			public Set<Tuple> doBiz(Jedis j) {
				j.select(DBIndex);
				return j.zrevrangeByScoreWithScores(key, max, min, offset, count);
			}

			public String getOperationName() {
				return "zrevrangeByScoreWithScores";
			}
		});
	}

	public Set<String> zrangeByScore(final int DBIndex, final String key, final String min, final String max) {
		return doOperation(key, new JedisCallBack<Set<String>>() {

			public Set<String> doBiz(Jedis j) {
				j.select(DBIndex);
				return j.zrangeByScore(key, min, max);
			}

			public String getOperationName() {
				return "zrangeByScore";
			}
		});
	}

	public Set<String> zrevrangeByScore(final int DBIndex, final String key, final String max, final String min) {
		return doOperation(key, new JedisCallBack<Set<String>>() {

			public Set<String> doBiz(Jedis j) {
				j.select(DBIndex);
				return j.zrevrangeByScore(key, max, min);
			}

			public String getOperationName() {
				return "zrevrangeByScore";
			}
		});
	}

	public Set<String> zrangeByScore(final int DBIndex, final String key, final String min, final String max, final int offset, final int count) {
		return doOperation(key, new JedisCallBack<Set<String>>() {

			public Set<String> doBiz(Jedis j) {
				j.select(DBIndex);
				return j.zrangeByScore(key, min, max, offset, count);
			}

			public String getOperationName() {
				return "zrangeByScore";
			}
		});
	}

	public Set<String> zrevrangeByScore(final int DBIndex, final String key, final String max, final String min, final int offset, final int count) {
		return doOperation(key, new JedisCallBack<Set<String>>() {

			public Set<String> doBiz(Jedis j) {
				j.select(DBIndex);
				return j.zrevrangeByScore(key, max, min, offset, count);
			}

			public String getOperationName() {
				return "zrevrangeByScore";
			}
		});
	}

	public Set<Tuple> zrangeByScoreWithScores(final int DBIndex, final String key, final String min, final String max) {
		return doOperation(key, new JedisCallBack<Set<Tuple>>() {

			public Set<Tuple> doBiz(Jedis j) {
				j.select(DBIndex);
				return j.zrangeByScoreWithScores(key, min, max);
			}

			public String getOperationName() {
				return "zrangeByScoreWithScores";
			}
		});
	}

	public Set<Tuple> zrevrangeByScoreWithScores(final int DBIndex, final String key, final String max, final String min) {
		return doOperation(key, new JedisCallBack<Set<Tuple>>() {

			public Set<Tuple> doBiz(Jedis j) {
				j.select(DBIndex);
				return j.zrevrangeByScoreWithScores(key, max, min);
			}

			public String getOperationName() {
				return "zrevrangeByScoreWithScores";
			}
		});
	}

	public Set<Tuple> zrangeByScoreWithScores(final int DBIndex, final String key, final String min, final String max, final int offset, final int count) {
		return doOperation(key, new JedisCallBack<Set<Tuple>>() {

			public Set<Tuple> doBiz(Jedis j) {
				j.select(DBIndex);
				return j.zrangeByScoreWithScores(key, min, max, offset, count);
			}

			public String getOperationName() {
				return "zrangeByScoreWithScores";
			}
		});
	}

	public Set<Tuple> zrevrangeByScoreWithScores(final int DBIndex, final String key, final String max, final String min, final int offset, final int count) {
		return doOperation(key, new JedisCallBack<Set<Tuple>>() {

			public Set<Tuple> doBiz(Jedis j) {
				j.select(DBIndex);
				return j.zrevrangeByScoreWithScores(key, max, min, offset, count);
			}

			public String getOperationName() {
				return "zrevrangeByScoreWithScores";
			}
		});
	}

	public Long zremrangeByRank(final int DBIndex, final String key, final long start, final long end) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.zremrangeByRank(key, start, end);
			}

			public String getOperationName() {
				return "zremrangeByRank";
			}
		});
	}

	public Long zremrangeByScore(final int DBIndex, final String key, final double start, final double end) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.zremrangeByScore(key, start, end);
			}

			public String getOperationName() {
				return "zremrangeByScore";
			}
		});
	}

	public Long zremrangeByScore(final int DBIndex, final String key, final String start, final String end) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.zremrangeByScore(key, start, end);
			}

			public String getOperationName() {
				return "zremrangeByScore";
			}
		});
	}

	public Long linsert(final int DBIndex, final String key, final LIST_POSITION where, final String pivot, final String value) {
		return doOperation(key, new JedisCallBack<Long>() {

			public Long doBiz(Jedis j) {
				j.select(DBIndex);
				return j.linsert(key, where, pivot, value);
			}

			public String getOperationName() {
				return "linsert";
			}
		});
	}

	/**
	 * ********* EXT API ***************
	 */
	public String set(final int DBIndex, final String key, final Object value) {

		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				j.select(DBIndex);
				byte[] byteValue = serialize(value);
				return j.set(SafeEncoder.encode(key), byteValue);
			}

			public String getOperationName() {
				return "set";
			}
		});

	}

	public String set(final int DBIndex, final String key, final int seconds, final Object value) {

		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				j.select(DBIndex);
				byte[] byteValue = serialize(value);
				return j.setex(SafeEncoder.encode(key), seconds, byteValue);
			}

			public String getOperationName() {
				return "set";
			}
		});

	}

	public Object getObject(final int DBIndex, final String key) {

		return doOperation(key, new JedisCallBack<Object>() {

			public Object doBiz(Jedis j) {
				j.select(DBIndex);
				byte[] result = j.get(SafeEncoder.encode(key));
				Object object = deserialize(result);
				return object;
			}

			public String getOperationName() {
				return "getObject";
			}
		});

	}

	public String info(final int DBIndex, final String key) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				j.select(DBIndex);
				return j.info(key);
			}

			public String getOperationName() {
				return "info";
			}
		});

	}

	public String echo(final int DBIndex, final String key) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				j.select(DBIndex);
				return j.echo(key);
			}

			public String getOperationName() {
				return "echo";
			}
		});
	}

	@Override
	public Long pexpireAt(String key, long millisecondsTimestamp) {
		return null;
	}

	@Override
	public Long zlexcount(String key, String min, String max) {
		return null;
	}

	@Override
	public Set<String> zrangeByLex(String key, String min, String max) {
		return null;
	}

	@Override
	public Set<String> zrangeByLex(String key, String min, String max, int offset, int count) {
		return null;
	}

	@Override
	public Set<String> zrevrangeByLex(String key, String max, String min) {
		return null;
	}

	@Override
	public Set<String> zrevrangeByLex(String key, String max, String min, int offset, int count) {
		return null;
	}

	@Override
	public Long zremrangeByLex(String key, String min, String max) {
		return null;
	}

	@Override
	public Long lpushx(String key, String... string) {
		return null;
	}

	@Override
	public Long rpushx(String key, String... string) {
		return null;
	}

	@Override
	public List<String> blpop(String arg) {
		return null;
	}

	@Override
	public List<String> blpop(int timeout, String key) {
		return null;
	}

	@Override
	public List<String> brpop(String arg) {
		return null;
	}

	@Override
	public List<String> brpop(final int timeout, final String key) {
		return doOperation(key, new JedisCallBack<List<String>>() {

			public List<String> doBiz(Jedis j) {
				return j.brpop(timeout, key);
			}

			public String getOperationName() {
				return "brpop";
			}
		});
	}

	@Override
	public String echo(final String key) {
		return doOperation(key, new JedisCallBack<String>() {

			public String doBiz(Jedis j) {
				return j.echo(key);
			}

			public String getOperationName() {
				return "echo";
			}
		});
	}

	@Override
	public Long bitcount(String key) {
		return null;
	}

	@Override
	public Long bitcount(String key, long start, long end) {
		return null;
	}

	@Override
	public ScanResult<Entry<String, String>> hscan(String key, int cursor) {
		return null;
	}

	@Override
	public ScanResult<String> sscan(String key, int cursor) {
		return null;
	}

	@Override
	public ScanResult<Tuple> zscan(String key, int cursor) {
		return null;
	}

	@Override
	public ScanResult<Entry<String, String>> hscan(String key, String cursor) {
		return null;
	}

	@Override
	public ScanResult<String> sscan(String key, String cursor) {
		return null;
	}

	@Override
	public ScanResult<Tuple> zscan(String key, String cursor) {
		return null;
	}

	@Override
	public Long pfadd(String key, String... elements) {
		return null;
	}

	@Override
	public long pfcount(String key) {
		return 0;
	}

	protected byte[] serialize(Object o) {
		if (o == null) {
			return new byte[0];
		}
		byte[] rv = null;
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream os = new ObjectOutputStream(bos);
			os.writeObject(o);
			os.close();
			bos.close();
			rv = bos.toByteArray();
		} catch (IOException e) {
			throw new IllegalArgumentException("Non-serializable object", e);
		}
		return rv;
	}

	/**
	 * Get the object represented by the given serialized bytes.
	 *
	 * @param in
	 * @return
	 * @see bjf :
	 *      406@/rigel-tcom2/modules/tcom-cache/src/main/java/com/baidu/rigel
	 *      /service/cache/redis/RedisClient.java
	 */
	protected Object deserialize(byte[] in) {
		Object rv = null;
		try {
			if (in != null) {
				ByteArrayInputStream bis = new ByteArrayInputStream(in);
				ObjectInputStream is = new ObjectInputStream(bis);
				rv = is.readObject();
				is.close();
				bis.close();
			}
		} catch (IOException e) {
			log.warn("Caught IOException decoding %d bytes of data", e);
		} catch (ClassNotFoundException e) {
			log.warn("Caught CNFE decoding %d bytes of data", e);
		}
		return rv;
	}

	@Override
	public String set(String key, String value, String nxxx, String expx, long time) {
		return null;
	}

	@Override
	public Long pexpire(String key, long milliseconds) {
		return null;
	}

	@Override
	public Double incrByFloat(String key, double value) {
		return null;
	}

	@Override
	public Set<String> spop(String key, long count) {
		return null;
	}

	@Override
	public List<String> srandmember(String key, int count) {
		return null;
	}

	@Override
	public List<Long> bitfield(String arg0, String... arg1) {
		return null;
	}

	@Override
	public Long bitpos(String arg0, boolean arg1) {
		return null;
	}

	@Override
	public Long bitpos(String arg0, boolean arg1, BitPosParams arg2) {
		return null;
	}

	@Override
	public Long geoadd(String arg0, Map<String, GeoCoordinate> arg1) {
		return null;
	}

	@Override
	public Long geoadd(String arg0, double arg1, double arg2, String arg3) {
		return null;
	}

	@Override
	public Double geodist(String arg0, String arg1, String arg2) {
		return null;
	}

	@Override
	public Double geodist(String arg0, String arg1, String arg2, GeoUnit arg3) {
		return null;
	}

	@Override
	public List<String> geohash(String arg0, String... arg1) {
		return null;
	}

	@Override
	public List<GeoCoordinate> geopos(String arg0, String... arg1) {
		return null;
	}

	@Override
	public List<GeoRadiusResponse> georadius(String arg0, double arg1, double arg2, double arg3, GeoUnit arg4) {
		return null;
	}

	@Override
	public List<GeoRadiusResponse> georadius(String arg0, double arg1, double arg2, double arg3, GeoUnit arg4, GeoRadiusParam arg5) {
		return null;
	}

	@Override
	public List<GeoRadiusResponse> georadiusByMember(String arg0, String arg1, double arg2, GeoUnit arg3) {
		return null;
	}

	@Override
	public List<GeoRadiusResponse> georadiusByMember(String arg0, String arg1, double arg2, GeoUnit arg3, GeoRadiusParam arg4) {
		return null;
	}

	@Override
	public Double hincrByFloat(String arg0, String arg1, double arg2) {
		return null;
	}

	@Override
	public ScanResult<Entry<String, String>> hscan(String arg0, String arg1, ScanParams arg2) {
		return null;
	}

	@Override
	public String psetex(String arg0, long arg1, String arg2) {
		return null;
	}

	@Override
	public Long pttl(String arg0) {
		return null;
	}

	@Override
	public String set(String arg0, String arg1, String arg2) {
		return null;
	}

	@Override
	public ScanResult<String> sscan(String arg0, String arg1, ScanParams arg2) {
		return null;
	}

	@Override
	public Long zadd(String arg0, Map<String, Double> arg1, ZAddParams arg2) {
		return null;
	}

	@Override
	public Long zadd(String arg0, double arg1, String arg2, ZAddParams arg3) {
		return null;
	}

	@Override
	public Double zincrby(String arg0, double arg1, String arg2, ZIncrByParams arg3) {
		return null;
	}

	@Override
	public ScanResult<Tuple> zscan(String arg0, String arg1, ScanParams arg2) {
		return null;
	}

}
