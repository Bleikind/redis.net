using Redis.NET.Internal.Commands;
using Redis.NET.Internal.IO;
using Redis.NET.Internal.Utilities;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Redis.NET
{
    static class RedisCommands
    {
        #region Connection
        public static RedisStatus Auth(string password) => new RedisStatus("AUTH", password);
        public static RedisString Echo(string message) => new RedisString("ECHO", message);
        public static RedisStatus Ping() => new RedisStatus("PING");
        public static RedisStatus Quit() => new RedisStatus("QUIT");
        public static RedisStatus Select(int index) => new RedisStatus("SELECT", index);
        #endregion

        #region Keys
        public static RedisInt Del(params string[] keys) => new RedisInt("DEL", keys);
        public static RedisBytes Dump(string key) => new RedisBytes("DUMP", key);
        public static RedisBool Exists(string key) => new RedisBool("EXISTS", key);
        public static RedisBool Expire(string key, TimeSpan expiration) => new RedisBool("EXPIRE", key, (int)expiration.TotalSeconds);
        public static RedisBool Expire(string key, int seconds) => new RedisBool("EXPIRE", key, seconds);
        public static RedisBool ExpireAt(string key, DateTime expirationDate) => ExpireAt(key, (int)RedisDate.ToTimestamp(expirationDate).TotalSeconds);
        public static RedisBool ExpireAt(string key, int timestamp) => new RedisBool("EXPIREAT", key, timestamp);
        public static RedisArray.Strings Keys(string pattern) => new RedisArray.Strings("KEYS", pattern);
        public static RedisStatus Migrate(string host, int port, string key, int destinationDb, int timeoutMilliseconds) => new RedisStatus("MIGRATE", host, port, key, destinationDb, timeoutMilliseconds);
        public static RedisStatus Migrate(string host, int port, string key, int destinationDb, TimeSpan timeout) => Migrate(host, port, key, destinationDb, (int)timeout.TotalMilliseconds);
        public static RedisBool Move(string key, int database) => new RedisBool("MOVE", key, database);
        public static RedisString ObjectEncoding(params string[] arguments) => new RedisString("OBJECT", RedisArgs.Concat("ENCODING", arguments));
        public static RedisInt.Nullable Object(RedisObjectSubCommand subCommand, params string[] arguments) => new RedisInt.Nullable("OBJECT", RedisArgs.Concat(subCommand.ToString().ToUpperInvariant(), arguments));
        public static RedisBool Persist(string key) => new RedisBool("PERSIST", key);
        public static RedisBool PExpire(string key, TimeSpan expiration) => new RedisBool("PEXPIRE", key, (int)expiration.TotalMilliseconds);
        public static RedisBool PExpire(string key, long milliseconds) => new RedisBool("PEXPIRE", key, milliseconds);
        public static RedisBool PExpireAt(string key, DateTime date) => PExpireAt(key, (long)RedisDate.ToTimestamp(date).TotalMilliseconds);
        public static RedisBool PExpireAt(string key, long timestamp) => new RedisBool("PEXPIREAT", key, timestamp);
        public static RedisInt PTtl(string key) => new RedisInt("PTTL", key);
        public static RedisString RandomKey() => new RedisString("RANDOMKEY");
        public static RedisStatus Rename(string key, string newKey) => new RedisStatus("RENAME", key, newKey);
        public static RedisBool RenameNx(string key, string newKey) => new RedisBool("RENAMENX", key, newKey);
        public static RedisStatus Restore(string key, long ttl, string serializedValue) => new RedisStatus("RESTORE", key, ttl, serializedValue);
        public static RedisArray.Generic<Dictionary<string, string>> Sort(string key, long? offset = null, long? count = null, string by = null, RedisSortDir? dir = null, bool? isAlpha = null, bool? isHash = null, params string[] get)
        {
            var args = new List<string> { key };
            if (by != null)
            {
                args.AddRange(new[] { "BY", by });
            }

            if (offset.HasValue && count.HasValue)
            {
                args.AddRange(new[] { "LIMIT", offset.Value.ToString(), count.Value.ToString() });
            }

            foreach (var pattern in get)
            {
                args.AddRange(new[] { "GET", pattern });
            }

            if (dir.HasValue)
            {
                args.Add(dir.ToString().ToUpperInvariant());
            }

            if (isAlpha.HasValue && isAlpha.Value)
            {
                args.Add("ALPHA");
            }

            return new RedisArray.Generic<Dictionary<string,string>>(new RedisHash("SORT", args.ToArray()));
        }
        public static RedisArray.Strings Sort(string key, long? offset = null, long? count = null, string by = null, RedisSortDir? dir = null, bool? isAlpha = null, params string[] get)
        {
            List<string> args = new List<string> { key };
            if (by != null)
            {
                args.AddRange(new[] { "BY", by });
            }

            if (offset.HasValue && count.HasValue)
            {
                args.AddRange(new[] { "LIMIT", offset.Value.ToString(), count.Value.ToString() });
            }

            foreach (var pattern in get)
            {
                args.AddRange(new[] { "GET", pattern });
            }

            if (dir.HasValue)
            {
                args.Add(dir.ToString().ToUpperInvariant());
            }

            if (isAlpha.HasValue && isAlpha.Value)
            {
                args.Add("ALPHA");
            }

            return new RedisArray.Strings("SORT", args.ToArray());
        }
        public static RedisInt SortAndStore(string key, string destination, long? offset = null, long? count = null, string by = null, RedisSortDir? dir = null, bool? isAlpha = null, params string[] get)
        {
            List<string> args = new List<string> { key };
            if (by != null)
            {
                args.AddRange(new[] { "BY", by });
            }

            if (offset.HasValue && count.HasValue)
            {
                args.AddRange(new[] { "LIMIT", offset.Value.ToString(), count.Value.ToString() });
            }

            foreach (var pattern in get)
            {
                args.AddRange(new[] { "GET", pattern });
            }

            if (dir.HasValue)
            {
                args.Add(dir.ToString().ToUpperInvariant());
            }

            if (isAlpha.HasValue && isAlpha.Value)
            {
                args.Add("ALPHA");
            }

            args.AddRange(new[] { "STORE", destination });
            return new RedisInt("SORT", args.ToArray());
        }
        public static RedisInt Ttl(string key) => new RedisInt("TTL", key);
        public static RedisStatus Type(string key) => new RedisStatus("TYPE", key);
        public static RedisScanCommand<string> Scan(long cursor, string pattern = null, long? count = null)
        {
            var args = new List<object> { cursor };
            if (pattern != null)
            {
                args.AddRange(new[] { "MATCH", pattern });
            }

            if (count != null)
            {
                args.AddRange(new object[] { "COUNT", count });
            }

            return new RedisScanCommand<string>(new RedisArray.Strings("SCAN", args.ToArray()));
        }
        #endregion

        #region Hashes
        public static RedisInt HDel(string key, params string[] fields) => new RedisInt("HDEL", RedisArgs.Concat(key, fields));
        public static RedisBool HExists(string key, string field) => new RedisBool("HEXISTS", key, field);
        public static RedisString HGet(string key, string field) => new RedisString("HGET", key, field);
        public static RedisHash.Generic<T> HGetAll<T>(string key) where T : class => new RedisHash.Generic<T>("HGETALL", key);
        public static RedisHash HGetAll(string key) => new RedisHash("HGETALL", key);
        public static RedisInt HIncrBy(string key, string field, long increment) => new RedisInt("HINCRBY", key, field, increment);
        public static RedisFloat HIncrByFloat(string key, string field, double increment) => new RedisFloat("HINCRBYFLOAT", key, field, increment);
        public static RedisArray.Strings HKeys(string key) => new RedisArray.Strings("HKEYS", key);
        public static RedisInt HLen(string key) => new RedisInt("HLEN", key);
        public static RedisArray.Strings HMGet(string key, params string[] fields) => new RedisArray.Strings("HMGET", RedisArgs.Concat(key, fields));
        public static RedisStatus HMSet(string key, Dictionary<string, string> dict)
        {
            List<object> args = new List<object> { key };
            args.AddRange(RedisArgs.FromDict(dict));
            return new RedisStatus("HMSET", args.ToArray());
        }
        public static RedisStatus HMSet<T>(string key, T obj) where T : class
        {
            List<object> args = new List<object> { key };
            args.AddRange(RedisArgs.FromObject(obj));
            return new RedisStatus("HMSET", args.ToArray());
        }
        public static RedisStatus HMSet(string key, params string[] keyValues)
        {
            List<object> args = new List<object> { key };
            for (int i = 0; i < keyValues.Length; i += 2)
            {
                if (keyValues[i] != null && keyValues[i + 1] != null)
                {
                    args.AddRange(new[] { keyValues[i], keyValues[i + 1] });
                }
            }
            return new RedisStatus("HMSET", args.ToArray());
        }
        public static RedisBool HSet(string key, string field, object value) => new RedisBool("HSET", key, field, value);
        public static RedisBool HSetNx(string key, string field, object value) => new RedisBool("HSETNX", key, field, value);
        public static RedisArray.Strings HVals(string key) => new RedisArray.Strings("HVALS", key);
        public static RedisScanCommand<Tuple<string, string>> HScan(string key, long cursor, string pattern = null, long? count = null)
        {
            var args = new List<object> { key, cursor };
            if (pattern != null)
            {
                args.AddRange(new[] { "MATCH", pattern });
            }

            if (count != null)
            {
                args.AddRange(new object[] { "COUNT", count });
            }

            return new RedisScanCommand<Tuple<string, string>>(new RedisArray.WeakPairs<string, string>("HSCAN", args.ToArray()));
                //new RedisArray.Generic<Tuple<string, string>>(
                    //new RedisTuple.Generic<string, string>.Bulk("HSCAN", args.ToArray())));
        }
        #endregion

        #region Lists
        public static RedisTuple BLPopWithKey(int timeout, params string[] keys) => new RedisTuple("BLPOP", RedisArgs.Concat(keys, new object[] { timeout }));
        public static RedisTuple BLPopWithKey(TimeSpan timeout, params string[] keys) => BLPopWithKey((int)timeout.TotalSeconds, keys);
        public static RedisArray.IndexOf<string> BLPop(int timeout, params string[] keys) => new RedisArray.IndexOf<string>(new RedisString("BLPOP", RedisArgs.Concat(keys, new object[] { timeout })), 1);
        public static RedisArray.IndexOf<string> BLPop(TimeSpan timeout, params string[] keys) => BLPop((int)timeout.TotalSeconds, keys);
        public static RedisTuple BRPopWithKey(int timeout, params string[] keys) => new RedisTuple("BRPOP", RedisArgs.Concat(keys, new object[] { timeout }));
        public static RedisTuple BRPopWithKey(TimeSpan timeout, params string[] keys) => BRPopWithKey((int)timeout.TotalSeconds, keys);
        public static RedisArray.IndexOf<string> BRPop(int timeout, params string[] keys) => new RedisArray.IndexOf<string>(new RedisString("BRPOP", RedisArgs.Concat(keys, new object[] { timeout })), 1);
        public static RedisArray.IndexOf<string> BRPop(TimeSpan timeout, params string[] keys) => BRPop((int)timeout.TotalSeconds, keys);
        public static RedisString.Nullable BRPopLPush(string source, string destination, int timeout) => new RedisString.Nullable("BRPOPLPUSH", source, destination, timeout);
        public static RedisString.Nullable BRPopLPush(string source, string destination, TimeSpan timeout) => BRPopLPush(source, destination, (int)timeout.TotalSeconds);
        public static RedisString LIndex(string key, long index) => new RedisString("LINDEX", key, index);
        public static RedisInt LInsert(string key, RedisInsert insertType, string pivot, object value) => new RedisInt("LINSERT", key, insertType.ToString().ToUpperInvariant(), pivot, value);
        public static RedisInt LLen(string key) => new RedisInt("LLEN", key);
        public static RedisString LPop(string key) => new RedisString("LPOP", key);
        public static RedisInt LPush(string key, params object[] values) => new RedisInt("LPUSH", RedisArgs.Concat(new[] { key }, values));
        public static RedisInt LPushX(string key, object value) => new RedisInt("LPUSHX", key, value);
        public static RedisArray.Strings LRange(string key, long start, long stop) => new RedisArray.Strings("LRANGE", key, start, stop);
        public static RedisInt LRem(string key, long count, object value) => new RedisInt("LREM", key, count, value);
        public static RedisStatus LSet(string key, long index, object value) => new RedisStatus("LSET", key, index, value);
        public static RedisStatus LTrim(string key, long start, long stop) => new RedisStatus("LTRIM", key, start, stop);
        public static RedisString RPop(string key) => new RedisString("RPOP", key);
        public static RedisString RPopLPush(string source, string destination) => new RedisString("RPOPLPUSH", source, destination);
        public static RedisInt RPush(string key, params object[] values) => new RedisInt("RPUSH", RedisArgs.Concat(key, values));
        public static RedisInt RPushX(string key, params object[] values) => new RedisInt("RPUSHX", RedisArgs.Concat(key, values));
        #endregion

        #region Sets
        public static RedisInt SAdd(string key, params object[] members)
        {
            object[] args = RedisArgs.Concat(key, members);
            return new RedisInt("SADD", args);
        }
        public static RedisInt SCard(string key)
        {
            return new RedisInt("SCARD", key);
        }
        public static RedisArray.Strings SDiff(params string[] keys)
        {
            return new RedisArray.Strings("SDIFF", keys);
        }
        public static RedisInt SDiffStore(string destination, params string[] keys)
        {
            object[] args = RedisArgs.Concat(destination, keys);
            return new RedisInt("SDIFFSTORE", args);
        }
        public static RedisArray.Strings SInter(params string[] keys)
        {
            return new RedisArray.Strings("SINTER", keys);
        }
        public static RedisInt SInterStore(string destination, params string[] keys)
        {
            object[] args = RedisArgs.Concat(destination, keys);
            return new RedisInt("SINTERSTORE", args);
        }
        public static RedisBool SIsMember(string key, object member) 
        {
            return new RedisBool("SISMEMBER", key, member);
        }
        public static RedisArray.Strings SMembers(string key)
        {
            return new RedisArray.Strings("SMEMBERS", key);
        }
        public static RedisBool SMove(string source, string destination, object member)
        {
            return new RedisBool("SMOVE", source, destination, member);
        }
        public static RedisString SPop(string key)
        {
            return new RedisString("SPOP", key);
        }
        public static RedisString SRandMember(string key)
        {
            return new RedisString("SRANDMEMBER", key);
        }
        public static RedisArray.Strings SRandMember(string key, long count)
        {
            return new RedisArray.Strings("SRANDMEMBER", key, count);
        }
        public static RedisInt SRem(string key, params object[] members) 
        {
            object[] args = RedisArgs.Concat(key, members);
            return new RedisInt("SREM", args);
        }
        public static RedisArray.Strings SUnion(params string[] keys)
        {
            return new RedisArray.Strings("SUNION", keys);
        }
        public static RedisInt SUnionStore(string destination, params string[] keys)
        {
            string[] args = RedisArgs.Concat(destination, keys);
            return new RedisInt("SUNIONSTORE", args);
        }
        public static RedisScanCommand<string> SScan(string key, long cursor, string pattern = null, long? count = null)
        {
            var args = new List<object> { key, cursor };
            if (pattern != null)
            {
                args.AddRange(new[] { "MATCH", pattern });
            }

            if (count != null)
            {
                args.AddRange(new object[] { "COUNT", count });
            }

            return new RedisScanCommand<string>(new RedisArray.Strings("SSCAN", args.ToArray()));
        }
        #endregion

        #region Sorted Sets
        public static RedisInt ZAdd<TScore, TMember>(string key, params Tuple<TScore, TMember>[] memberScores) => new RedisInt("ZADD", RedisArgs.Concat(key, RedisArgs.GetTupleArgs(memberScores)));
        public static RedisInt ZAdd(string key, params string[] memberScores) => new RedisInt("ZADD", RedisArgs.Concat(key, memberScores));
        public static RedisInt ZCard(string key) => new RedisInt("ZCARD", key);
        public static RedisInt ZCount(string key, double min, double max, bool exclusiveMin = false, bool exclusiveMax = false) => ZCount(key, RedisArgs.GetScore(min, exclusiveMin), RedisArgs.GetScore(max, exclusiveMax));
        public static RedisInt ZCount(string key, string min, string max) => new RedisInt("ZCOUNT", key, min, max);
        public static RedisFloat ZIncrBy(string key, double increment, string member) => new RedisFloat("ZINCRBY", key, increment, member);
        public static RedisInt ZInterStore(string destination, double[] weights = null, RedisAggregate? aggregate = null, params string[] keys)
        {
            List<object> args = new List<object> { destination, keys.Length };
            args.AddRange(keys);
            if (weights != null && weights.Length > 0)
            {
                args.Add("WEIGHTS");
                args.AddRange((IEnumerable<object>) (from weight in weights select weight));
            }

            if (aggregate != null)
            {
                args.Add("AGGREGATE");
                args.Add(aggregate.ToString().ToUpperInvariant());
            }

            return new RedisInt("ZINTERSTORE", args.ToArray());
        }
        public static RedisArray.Strings ZRange(string key, long start, long stop, bool withScores = false) => new RedisArray.Strings("ZRANGE", withScores
                ? new[] { key, start.ToString(), stop.ToString(), "WITHSCORES" }
                : new[] { key, start.ToString(), stop.ToString() });
        public static RedisArray.WeakPairs<string, double> ZRangeWithScores(string key, long start, long stop) => new RedisArray.WeakPairs<string, double>("ZRANGE", key, start, stop, "WITHSCORES");
        public static RedisArray.Strings ZRangeByScore(string key, double min, double max, bool withScores = false, bool exclusiveMin = false, bool exclusiveMax = false, long? offset = null, long? count = null) => ZRangeByScore(key, RedisArgs.GetScore(min, exclusiveMin), RedisArgs.GetScore(max, exclusiveMax), withScores, offset, count);
        public static RedisArray.WeakPairs<string, double> ZRangeByScoreWithScores(string key, double min, double max, bool exclusiveMin = false, bool exclusiveMax = false, long? offset = null, long? count = null) => ZRangeByScoreWithScores(key, RedisArgs.GetScore(min, exclusiveMin), RedisArgs.GetScore(max, exclusiveMax), offset, count);
        public static RedisArray.Strings ZRangeByScore(string key, string min, string max, bool withScores = false, long? offset = null, long? count = null)
        {
            string[] args = new[] { key, min, max};
            if (withScores)
            {
                args = RedisArgs.Concat(args, new[] { "WITHSCORES" });
            }

            if (offset.HasValue && count.HasValue)
            {
                args = RedisArgs.Concat(args, new[] { "LIMIT", offset.Value.ToString(), count.Value.ToString() });
            }

            return new RedisArray.Strings("ZRANGEBYSCORE", args);
        }
        public static RedisArray.WeakPairs<string, double> ZRangeByScoreWithScores(string key, string min, string max, long? offset = null, long? count = null) 
        {
            string[] args = new[] { key, min, max, "WITHSCORES" };
            if (offset.HasValue && count.HasValue)
            {
                args = RedisArgs.Concat(args, new[] { "LIMIT", offset.Value.ToString(), count.Value.ToString() });
            }

            return new RedisArray.WeakPairs<string, double>("ZRANGEBYSCORE", args);
        }
        public static RedisInt.Nullable ZRank(string key, string member) => new RedisInt.Nullable("ZRANK", key, member);
        public static RedisInt ZRem(string key, params object[] members) => new RedisInt("ZREM", RedisArgs.Concat(new[] { key }, members));
        public static RedisInt ZRemRangeByRank(string key, long start, long stop) => new RedisInt("ZREMRANGEBYRANK", key, start, stop);
        public static RedisInt ZRemRangeByScore(string key, double min, double max, bool exclusiveMin = false, bool exclusiveMax = false) => new RedisInt("ZREMRANGEBYSCORE", key, RedisArgs.GetScore(min, exclusiveMin), RedisArgs.GetScore(max, exclusiveMax));
        public static RedisArray.Strings ZRevRange(string key, long start, long stop, bool withScores = false) => new RedisArray.Strings("ZREVRANGE", withScores
                ? new[] { key, start.ToString(), stop.ToString(), "WITHSCORES" }
                : new[] { key, start.ToString(), stop.ToString() });
        public static RedisArray.WeakPairs<string, double> ZRevRangeWithScores(string key, long start, long stop) => new RedisArray.WeakPairs<string, double>("ZREVRANGE", key, start.ToString(), stop.ToString(), "WITHSCORES");
        public static RedisArray.Strings ZRevRangeByScore(string key, double max, double min, bool withScores = false, bool exclusiveMax = false, bool exclusiveMin = false, long? offset = null, long? count = null) => ZRevRangeByScore(key, RedisArgs.GetScore(max, exclusiveMax), RedisArgs.GetScore(min, exclusiveMin), withScores, offset, count);
        public static RedisArray.Strings ZRevRangeByScore(string key, string max, string min, bool withScores = false, long? offset = null, long? count = null)
        {
            var args = new[] { key, max, min };
            if (withScores)
            {
                args = RedisArgs.Concat(args, new[] { "WITHSCORES" });
            }

            if (offset.HasValue && count.HasValue)
            {
                args = RedisArgs.Concat(args, new[] { "LIMIT", offset.Value.ToString(), count.Value.ToString() });
            }

            return new RedisArray.Strings("ZREVRANGEBYSCORE", args);
        }
        public static RedisArray.WeakPairs<string, double> ZRevRangeByScoreWithScores(string key, double max, double min, bool exclusiveMax = false, bool exclusiveMin = false, long? offset = null, long? count = null) => ZRevRangeByScoreWithScores(key, RedisArgs.GetScore(max, exclusiveMax), RedisArgs.GetScore(min, exclusiveMin), offset, count);
        public static RedisArray.WeakPairs<string, double> ZRevRangeByScoreWithScores(string key, string max, string min, long? offset = null, long? count = null)
        {
            var args = new[] { key, max, min, "WITHSCORES" };
            if (offset.HasValue && count.HasValue)
            {
                args = RedisArgs.Concat(args, new[] { "LIMIT", offset.Value.ToString(), count.Value.ToString() });
            }

            return new RedisArray.WeakPairs<string, double>("ZREVRANGEBYSCORE", args);
        }
        public static RedisInt.Nullable ZRevRank(string key, string member) => new RedisInt.Nullable("ZREVRANK", key, member);
        public static RedisFloat.Nullable ZScore(string key, string member) => new RedisFloat.Nullable("ZSCORE", key, member);
        public static RedisInt ZUnionStore(string destination, double[] weights = null, RedisAggregate? aggregate = null, params string[] keys)
        {
            List<object> args = new List<object> { destination, keys.Length };
            args.AddRange(keys);
            if (weights != null && weights.Length > 0)
            {
                args.Add("WEIGHTS");
                args.AddRange((IEnumerable<object>) (from weight in weights select weight));
            }
            if (aggregate != null)
            {
                args.Add("AGGREGATE");
                args.Add(aggregate.ToString().ToUpperInvariant());
            }
            return new RedisInt("ZUNIONSTORE", args.ToArray());
        }
        public static RedisScanCommand<Tuple<string, double>> ZScan(string key, long cursor, string pattern = null, long? count = null)
        {
            var args = new List<object> { key, cursor };
            if (pattern != null)
            {
                args.AddRange(new[] { "MATCH", pattern });
            }

            if (count != null)
            {
                args.AddRange(new object[] { "COUNT", count });
            }

            return new RedisScanCommand<Tuple<string, double>>(new RedisArray.WeakPairs<string, double>("ZSCAN", args.ToArray()));
                    //<Tuple<string, double>>(
                    //new RedisTuple.Generic<string, double>.Bulk("ZSCAN", args.ToArray())));
        }
        public static RedisArray.Strings ZRangeByLex(string key, string min, string max, long? offset = null, long? count = null)
        {
            var args = new List<object> { key, min, max };
            if (offset != null && count != null)
            {
                args.AddRange(new object[] { "LIMIT", offset, count });
            }

            return new RedisArray.Strings("ZRANGEBYLEX", args.ToArray());
        }
        public static RedisInt ZRemRangeByLex(string key, string min, string max) => new RedisInt("ZREMRANGEBYLEX", key, min, max);
        public static RedisInt ZLexCount(string key, string min, string max) => new RedisInt("ZLEXCOUNT", key, min, max);
        #endregion

        #region PubSub
        public static RedisSubscription PSubscribe(params string[] channelPatterns) => new RedisSubscription("PSUBSCRIBE", channelPatterns);
        public static RedisInt Publish(string channel, string message) => new RedisInt("PUBLISH", channel, message);
        public static RedisArray.Strings PubSubChannels(string pattern = null)
        {
            var args = new List<string> { "CHANNELS" };
            if (pattern != null)
            {
                args.Add(pattern);
            }

            return new RedisArray.Strings("PUBSUB", args.ToArray());
        }
        public static RedisArray.StrongPairs<string, long> PubSubNumSub(params string[] channels) => new RedisArray.StrongPairs<string, long>(
                new RedisString(null), new RedisInt(null), "PUBSUB", RedisArgs.Concat("NUMSUB", channels));
        public static RedisInt PubSubNumPat() => new RedisInt("PUBSUB", "NUMPAT");
        public static RedisSubscription PUnsubscribe(params string[] channelPatterns) => new RedisSubscription("PUNSUBSCRIBE", channelPatterns);
        public static RedisSubscription Subscribe(params string[] channels) => new RedisSubscription("SUBSCRIBE", channels);
        public static RedisSubscription Unsubscribe(params string[] channels) => new RedisSubscription("UNSUBSCRIBE", channels);
        #endregion

        #region Scripting
        public static RedisObject.Strings Eval(string script, string[] keys, params string[] arguments) => new RedisObject.Strings("EVAL", RedisArgs.Concat(new object[] { script, keys.Length }, keys, arguments));
        public static RedisObject.Strings EvalSHA(string sha1, string[] keys, params string[] arguments) => new RedisObject.Strings("EVALSHA", RedisArgs.Concat(new object[] { sha1, keys.Length }, keys, arguments));
        public static RedisArray.Generic<bool> ScriptExists(params string[] scripts) => new RedisArray.Generic<bool>(new RedisBool("SCRIPT EXISTS", scripts));
        public static RedisStatus ScriptFlush() => new RedisStatus("SCRIPT FLUSH");
        public static RedisStatus ScriptKill() => new RedisStatus("SCRIPT KILL");
        public static RedisString ScriptLoad(string script) => new RedisString("SCRIPT LOAD", script);
        #endregion

        #region Strings
        public static RedisInt Append(string key, object value) => new RedisInt("APPEND", key, value);
        public static RedisInt BitCount(string key, long? start = null, long? end = null) => new RedisInt("BITCOUNT", start.HasValue && end.HasValue
                ? new[] { key, start.Value.ToString(), end.Value.ToString() }
                : new[] { key });
        public static RedisInt BitOp(RedisBitOp operation, string destKey, params string[] keys) => new RedisInt("BITOP", RedisArgs.Concat(new[] { operation.ToString().ToUpperInvariant(), destKey }, keys));
        public static RedisInt BitPos(string key, bool bit, long? start = null, long? end = null)
        {
            var args = new List<object> { key, bit ? "1" : "0" };

            if (start != null)
            {
                args.Add(start);
                if (end != null)
                {
                    args.Add(end);
                }
            }
            return new RedisInt("BITPOS", args.ToArray());
        }
        public static RedisInt Decr(string key) => new RedisInt("DECR", key);
        public static RedisInt DecrBy(string key, long decrement) => new RedisInt("DECRBY", key, decrement);
        public static RedisString Get(string key) => new RedisString("GET", key);
        public static RedisBool GetBit(string key, uint offset) => new RedisBool("GETBIT", key, offset);
        public static RedisString GetRange(string key, long start, long end) => new RedisString("GETRANGE", key, start, end);
        public static RedisString GetSet(string key, object value) => new RedisString("GETSET", key, value);
        public static RedisInt Incr(string key) => new RedisInt("INCR", key);
        public static RedisInt IncrBy(string key, long increment) => new RedisInt("INCRBY", key, increment);
        public static RedisFloat IncrByFloat(string key, double increment) => new RedisFloat("INCRBYFLOAT", key, increment);
        public static RedisArray.Strings MGet(params string[] keys) => new RedisArray.Strings("MGET", keys);
        public static RedisStatus MSet(params Tuple<string, string>[] keyValues) => new RedisStatus("MSET", RedisArgs.GetTupleArgs(keyValues));
        public static RedisStatus MSet(params string[] keyValues) => new RedisStatus("MSET", keyValues);
        public static RedisBool MSetNx(params Tuple<string, string>[] keyValues) => new RedisBool("MSETNX", RedisArgs.GetTupleArgs(keyValues));
        public static RedisBool MSetNx(params string[] keyValues) => new RedisBool("MSETNX", keyValues);
        public static RedisStatus PSetEx(string key, long milliseconds, object value) => new RedisStatus("PSETEX", key, milliseconds, value);
        public static RedisStatus Set(string key, object value) => new RedisStatus("SET", key, value);
        public static RedisStatus.Nullable Set(string key, object value, TimeSpan expiration, RedisExistence? condition = null) => Set(key, value, (long)expiration.TotalMilliseconds, condition);
        public static RedisStatus.Nullable Set(string key, object value, int? expirationSeconds = null, RedisExistence? condition = null) => Set(key, value, expirationSeconds, null, condition);
        public static RedisStatus.Nullable Set(string key, object value, long? expirationMilliseconds = null, RedisExistence? condition = null) => Set(key, value, null, expirationMilliseconds, condition);
        private static RedisStatus.Nullable Set(string key, object value, int? expirationSeconds = null, long? expirationMilliseconds = null, RedisExistence? exists = null)
        {
            var args = new List<string> { key, value.ToString() };
            if (expirationSeconds != null)
            {
                args.AddRange(new[] { "EX", expirationSeconds.ToString() });
            }

            if (expirationMilliseconds != null)
            {
                args.AddRange(new[] { "PX", expirationMilliseconds.ToString() });
            }

            if (exists != null)
            {
                args.AddRange(new[] { exists.ToString().ToUpperInvariant() });
            }

            return new RedisStatus.Nullable("SET", args.ToArray());
        }
        public static RedisBool SetBit(string key, uint offset, bool value) => new RedisBool("SETBIT", key, offset, value ? "1" : "0");
        public static RedisStatus SetEx(string key, long seconds, object value) => new RedisStatus("SETEX", key, seconds, value);
        public static RedisBool SetNx(string key, object value) => new RedisBool("SETNX", key, value);
        public static RedisInt SetRange(string key, uint offset, object value) => new RedisInt("SETRANGE", key, offset, value);
        public static RedisInt StrLen(string key) => new RedisInt("STRLEN", key);
        #endregion

        #region Server
        public static RedisStatus BgRewriteAof() => new RedisStatus("BGREWRITEAOF");
        public static RedisStatus BgSave() => new RedisStatus("BGSAVE");
        public static RedisString ClientGetName() => new RedisString("CLIENT GETNAME");
        public static RedisStatus ClientKill(string ip, int port) => new RedisStatus("CLIENT KILL", ip, port);
        public static RedisInt ClientKill(string addr = null, string id = null, string type = null, bool? skipMe = null)
        {
            var args = new List<string>();
            if (addr != null)
            {
                args.AddRange(new[] { "ADDR", addr });
            }

            if (id != null)
            {
                args.AddRange(new[] { "ID", id });
            }

            if (type != null)
            {
                args.AddRange(new[] { "TYPE", type });
            }

            if (skipMe != null)
            {
                args.AddRange(new[] { "SKIPME", skipMe.Value ? "yes" : "no" });
            }

            return new RedisInt("CLIENT KILL", args.ToArray());
        }
        public static RedisString ClientList() => new RedisString("CLIENT LIST");
        public static RedisStatus ClientPause(TimeSpan timeout) => ClientPause((int)timeout.TotalMilliseconds);
        public static RedisStatus ClientPause(int milliseconds) => new RedisStatus("CLIENT PAUSE", milliseconds);
        public static RedisStatus ClientSetName(string connectionName) => new RedisStatus("CLIENT SETNAME", connectionName);
        public static RedisArray.WeakPairs<string, string> ConfigGet(string parameter) => new RedisArray.WeakPairs<string, string>("CONFIG GET", parameter);
        public static RedisStatus ConfigResetStat() => new RedisStatus("CONFIG RESETSTAT");
        public static RedisStatus ConfigRewrite() => new RedisStatus("CONFIG REWRITE");
        public static RedisStatus ConfigSet(string parameter, string value) => new RedisStatus("CONFIG SET", parameter, value);
        public static RedisInt DbSize() => new RedisInt("DBSIZE");
        public static RedisStatus DebugSegFault() => new RedisStatus("DEBUG SEGFAULT");
        public static RedisStatus FlushAll() => new RedisStatus("FLUSHALL");
        public static RedisStatus FlushDb() => new RedisStatus("FLUSHDB");
        public static RedisString Info(string section = null) => new RedisString("INFO", section == null ? new string[0] : new[] { section });
        public static RedisDate LastSave() => new RedisDate("LASTSAVE");
        public static RedisStatus Monitor() => new RedisStatus("MONITOR");
        public static RedisRoleCommand Role() => new RedisRoleCommand("ROLE");
        public static RedisStatus Save() => new RedisStatus("SAVE");
        public static RedisStatus.Empty Shutdown(bool? save = null) => new RedisStatus.Empty("SHUTDOWN", save.HasValue && save.Value ? (new[] { "SAVE" }) : save.HasValue && !save.Value ? (new[] { "NOSAVE" }) : (new string[0]));
        public static RedisStatus SlaveOf(string host, int port) => new RedisStatus("SLAVEOF", host, port);
        public static RedisStatus SlaveOfNoOne() => new RedisStatus("SLAVEOF", "NO", "ONE");
        public static RedisArray.Generic<RedisSlowLogEntry> SlowLogGet(long? count = null)
        {
            var args = new List<object> { "GET" };
            if (count.HasValue)
            {
                args.Add(count.Value);
            }

            return new RedisArray.Generic<RedisSlowLogEntry>(
                new RedisSlowLogCommand("SLOWLOG", args.ToArray()));
        }
        public static RedisInt SlowLogLen() => new RedisInt("SLOWLOG", "LEN");
        public static RedisStatus SlowLogReset() => new RedisStatus("SLOWLOG", "RESET");
        public static RedisBytes Sync() => new RedisBytes("SYNC");
        public static RedisDate.Micro Time() => new RedisDate.Micro("TIME");
        #endregion

        #region Transactions
        public static RedisStatus Discard() => new RedisStatus("DISCARD");
        public static RedisArray Exec() => new RedisArray("EXEC");
        public static RedisStatus Multi() => new RedisStatus("MULTI");
        public static RedisStatus Unwatch() => new RedisStatus("UNWATCH");
        public static RedisStatus Watch(params string[] keys) => new RedisStatus("WATCH", keys);
        #endregion

        #region HyperLogLog
        public static RedisBool PfAdd(string key, params object[] elements) => new RedisBool("PFADD", RedisArgs.Concat(key, elements));
        public static RedisInt PfCount(params string[] keys) => new RedisInt("PFCOUNT", keys);
        public static RedisStatus PfMerge(string destKey, params string[] sourceKeys) => new RedisStatus("PFMERGE", RedisArgs.Concat(destKey, sourceKeys));
        #endregion

        public static class Sentinel
        {
            public static RedisArray.Generic<RedisSentinelInfo> Sentinels(string masterName) => new RedisArray.Generic<RedisSentinelInfo>(new RedisHash.Generic<RedisSentinelInfo>("SENTINEL", "sentinels", masterName));
            public static RedisArray.Generic<RedisMasterInfo> Masters() => new RedisArray.Generic<RedisMasterInfo>(new RedisHash.Generic<RedisMasterInfo>("SENTINEL", "masters"));
            public static RedisHash.Generic<RedisMasterInfo> Master(string masterName) => new RedisHash.Generic<RedisMasterInfo>("SENTINEL", "master", masterName);
            public static RedisArray.Generic<RedisSlaveInfo> Slaves(string masterName) => new RedisArray.Generic<RedisSlaveInfo>(new RedisHash.Generic<RedisSlaveInfo>("SENTINEL", "slaves", masterName));
            public static RedisIsMasterDownByAddrCommand IsMasterDownByAddr(string ip, int port, long currentEpoch, string runId) => new RedisIsMasterDownByAddrCommand("SENTINEL", "is-master-down-by-addr", ip, port, currentEpoch, runId);
            public static RedisTuple.Generic<string, int>.Single GetMasterAddrByName(string masterName) => new RedisTuple.Generic<string, int>.Single(
                    new RedisString(null), new RedisString.Integer(null), "SENTINEL", new[] { "get-master-addr-by-name", masterName });
            public static RedisInt Reset(string pattern) => new RedisInt("SENTINEL", "reset", pattern);
            public static RedisStatus Failover(string masterName) => new RedisStatus("SENTINEL", "failover", masterName);
            public static RedisStatus Monitor(string name, int port, int quorum) => new RedisStatus("SENTINEL", "MONITOR", name, port, quorum);
            public static RedisStatus Remove(string name) => new RedisStatus("SENTINEL", "REMOVE", name);
            public static RedisStatus Set(string masterName, string option, string value) => new RedisStatus("SENTINEL", "SET", masterName, option, value);
            public static RedisArray PendingScripts() => new RedisArray("SENTINEL", "pending-scripts");
        }

        public static RedisObject Call(string command, params string[] args) => new RedisObject(command, args);

        public static RedisStatus AsTransaction<T>(RedisCommand<T> command) => new RedisStatus(command.Command, command.Arguments);
    }

    class RedisCommand
    {
        readonly string _command;
        readonly object[] _args;

        public string Command { get => _command; }
        public object[] Arguments { get => _args; }

        protected RedisCommand(string command, params object[] args)
        {
            _command = command;
            _args = args;
        }
    }

    abstract class RedisCommand<T> : RedisCommand
    {
        protected RedisCommand(string command, params object[] args) : base (command,args)
        { }

        public abstract T Parse(RedisReader reader);

        public override string ToString() => $"{Command} {string.Join(" ", Arguments)}";
    }
}