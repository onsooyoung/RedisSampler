package heisenbug.redis.datastruts;

import heisenbug.redis.RedisTest;
import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * https://www.baeldung.com/jedis-java-redis-client-library
 */
public class DataStruts extends RedisTest {


    @Test
    public void StringSet(){

        jedis.set("events/city/rome", "32,15,223,828");
        String cachedResponse = jedis.get("events/city/rome");

        assertEquals(cachedResponse, "32,15,223,828");

    }

    @Test
    public void ListsPut(){//LIFO

        jedis.lpush("queue#tasks", "firstTask");
        jedis.lpush("queue#tasks", "secondTask");

        assertEquals(jedis.rpop("queue#tasks"), "secondTask");
        assertEquals(jedis.rpop("queue#tasks"), "firstTask");

    }

    @Test
    public void SetAdd(){

        jedis.sadd("nicknames", "nickname#1");
        jedis.sadd("nicknames", "nickname#2");
        jedis.sadd("nicknames", "nickname#1");//중복
        jedis.sadd("nicknames", "nickname#3");
        jedis.sadd("nicknames", "nickname#4");

        Set<String> nicknames = jedis.smembers("nicknames");

        Iterator<String> i= nicknames.iterator();
        while (i.hasNext()){
            System.out.printf("value: %s\n",  i.next());
        }

        assertEquals(nicknames.size(), 4);
        assertEquals(nicknames.contains("nickname#1"), true);
        assertEquals(nicknames.contains("nickname#2"), true);

    }

    @Test
    public void HashSet(){

        jedis.hset("user#1", "name", "Peter");
        jedis.hset("user#1", "job", "politician");

        Map<String, String> fields = jedis.hgetAll("user#1");

        assertEquals(jedis.hget("user#1", "job"), "politician");
        assertEquals(fields.get("name"), "Peter");

    }

    @Test
    public void SortedSetPut(){

        Map<String, Double> scores = new HashMap<String, Double>();

        scores.put("PlayerOne", 3000.0);
        scores.put("PlayerTwo", 1500.0);
        scores.put("PlayerThree", 8200.0);
        scores.put("PlayerFour", 8400.0);
        scores.put("PlayerFive", 8600.0);

        scores.entrySet().forEach(playerScore -> {
            jedis.zadd("ranking", playerScore.getValue(), playerScore.getKey());
        });

        Set<String> player = jedis.zrevrange("ranking", 0, 10);//ZREVRANGE key min max : 랭킹에 따라 범위를 지정하여 조회
        Iterator<String> i = player.iterator();
        while (i.hasNext()){
            System.out.printf("player  %s\n", i.next());
        }
        long rank = jedis.zrevrank("ranking", "PlayerThree");//0->1->...

        assertEquals(rank, 2);

    }

    /**
        WHAT IS A BASIC TRANSACTION IN REDIS?
        In Redis, a basic transaction involving MULTI and EXEC is meant to provide the opportunity for one client to execute multiple commands A, B, C, ... without other clients
        being able to interrupt them. This isn’t the same as a relational database transaction, -- Redis-in-Action, page 57
     */
    @Test
    public void multi() {

        // Redis는 Single Thread로 동작하여 동기처리 듯, 처리도중 간섭불가하나(다른 클라이언트에서는 간섭 가능), 다른 루틴 대기상태라 성능이슈 발생
        Transaction t = jedis.multi();

        t.set("foo", "bar");
        t.smembers("foo");
        t.get("foo");

        try {
            System.out.printf("execGetResponse before exec and get 1 %s\n", jedis.get("foo"));

        } catch (JedisDataException e) {//exec 하기전에는 set, smembers, get 되 실행되지 않는다.
            e.printStackTrace();
        }

        List<Response<?>> lr = t.execGetResponse();

        System.out.printf("execGetResponse after exec and get 2 %s\n", lr.get(2).get());

        assertEquals("bar", lr.get(2).get());
    }

    @Test
    public void Pipelining(){
        // 파이프 라이닝
        // 대용량 삽입을 위하여 pipeline에 add 혹은 set 후 sync 처리/
        // 비동기 처리 되는 듯, 처리도중 간섭이 가능하여 멀티스레드 환경에서 위험
        // 이것은 redis에는 없고 library에서 지원하는 것인가?
        // 네트워크 IO를 줄이기 위해 일괄전송처리 그러나 실제 네트워크 전송시 TCP패킷이 나뉘어 처리되기 때문에 도중에 에러 발생시 원자성 보장이 안됨.
        String userOneId = "4352523";
        String userTwoId = "4849888";

        Pipeline p = jedis.pipelined();
        p.sadd("searched#" + userOneId, "paris");
        p.zadd("pranking", 126, userOneId);
        p.zadd("pranking", 325, userTwoId);
        Response<Boolean> pipeExists = p.sismember("searched#" + userOneId, "paris");
        Response<Set<String>> pipeRanking = p.zrange("pranking", 0, -1);
        p.sync();

        Boolean exists = pipeExists.get();
        Set<String> ranking = pipeRanking.get();

        Assert.assertTrue(exists);

        Iterator<String> i = ranking.iterator();
        while (i.hasNext()){
            System.out.printf("Pipelining  %s\n", i.next());
        }
    }




}
