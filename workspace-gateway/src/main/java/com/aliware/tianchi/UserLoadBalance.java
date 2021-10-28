package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.apache.dubbo.rpc.cluster.loadbalance.ConsistentHashLoadBalance;
import org.apache.dubbo.rpc.cluster.loadbalance.LeastActiveLoadBalance;
import org.apache.dubbo.rpc.cluster.loadbalance.RandomLoadBalance;
import org.apache.dubbo.rpc.cluster.loadbalance.RoundRobinLoadBalance;
import org.apache.dubbo.rpc.cluster.loadbalance.ShortestResponseLoadBalance;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 负载均衡扩展接口
 * 必选接口，核心接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 选手需要基于此类实现自己的负载均衡算法
 */
public class UserLoadBalance implements LoadBalance {

    private static final String IsPreheat = "isPreheat";
    public static final long preheatDeadline = System.currentTimeMillis() + 50000;
    public static AtomicInteger index = new AtomicInteger();

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        if (invokers == null || invokers.isEmpty())
            return null;
        // 如果 invokers 列表中仅有一个 Invoker，直接返回即可，无需进行负载均衡
        if (invokers.size() == 1)
            return invokers.get(0);

        // 调用 doSelect 方法进行负载均衡
//        return doSelectPreheat(invokers, url, invocation);
//        return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
//        return doSelect(invokers, url, invocation, "local_random_balance");
        return roundSelect(invokers, url, invocation);
    }

    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation, String type) {
        switch (type) {
            case "local_random_balance":
                return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
            case "random_load_balance":
                return new RandomLoadBalance().select(invokers, url, invocation);
            case "least_active_load_balance":
                return new LeastActiveLoadBalance().select(invokers, url, invocation);
            case "consistent_hash_load_balance":
                return new ConsistentHashLoadBalance().select(invokers, url, invocation);
            case "round_robin_load_balance":
                return new RoundRobinLoadBalance().select(invokers, url, invocation);
            case "shortest_response_load_balance":
                return new ShortestResponseLoadBalance().select(invokers, url, invocation);
//            case "my_shortest_response_load_balance":
//                return new MyShortestResponseLoadBalance().select(invokers, url, invocation);
        }
        return null;
    }

    protected <T> Invoker<T> doSelectPreheat(List<Invoker<T>> invokers, URL url, Invocation invocation) {

        if (System.currentTimeMillis() <= preheatDeadline) {
            return doSelect(invokers, url, invocation, "local_random_balance");
        }
        return doSelect(invokers, url, invocation, "shortest_response_load_balance");
    }

    protected <T> Invoker<T> roundSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        int size = invokers.size();
        Invoker invoker = invokers.get(index.get() % size);
        index.incrementAndGet();
        if (index.get() > Integer.MAX_VALUE - 100) {
            index.set(index.get() % size);
        }
        return invoker;
    }
}
