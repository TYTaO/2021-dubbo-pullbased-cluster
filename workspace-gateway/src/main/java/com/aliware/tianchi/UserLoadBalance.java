package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcStatus;
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
    public static int DEFAULT_CONCURRENT = 10;

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
//        return roundSelect(invokers, url, invocation);
//        return randomWeightSelect(invokers, url, invocation);
        return doSelectFromInfo(invokers, url, invocation);
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

    protected <T> Invoker<T> randomWeightSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        int total = 0;
        int size = invokers.size();
        int[] weights = new int[size];

        for (int i = 0; i < size; i++) {
            weights[i] = getWeight(invokers.get(i));
            total += weights[i];
        }

        int select = size - 1;
        int random = ThreadLocalRandom.current().nextInt(total);
        for (int i = 0; i < size - 1; i++) {
            random -= weights[i];
            if (random < 0) {
                select = i;
                break;
            }
        }
        return invokers.get(select);
    }

    // Do select from info.
    protected <T> Invoker<T> doSelectFromInfo(List<Invoker<T>> invokers, URL url, Invocation invocation) {

//        System.out.println("RPC_QUEUE size: " + MyRpcStatus.RPC_QUEUE.size());
        boolean allNodeInit = false;

        for (Invoker tinvoker : invokers) {
            allNodeInit = MyRpcStatus.getStatus(tinvoker.getUrl()).isInit.get();
            if (!allNodeInit) {
                break;
            }
        }

        if (System.currentTimeMillis() <= preheatDeadline && !allNodeInit) {
            return doSelect(invokers, url, invocation, "local_random_balance");
        }

        RpcRequest  request;
        while ((request = MyRpcStatus.select()) == null) {
            for(Invoker tinvoke : invokers) {
                MyRpcStatus.initQueue(tinvoke.getUrl(), 1);
            }
        }

        Invoker invoker = null;
        for (Invoker tinvoke : invokers) {
            if (tinvoke.getUrl().toIdentityString().compareTo(request.url) == 0) {
                invoker = tinvoke;
                break;
            }
        }
        return invoker;
    }


    private int getWeight(Invoker invoker) {
        URL url = invoker.getUrl();
        int maxConcurrent = MyRpcStatus.getStatus(url).maxConcurrent.get();
        return maxConcurrent == 0 ? DEFAULT_CONCURRENT : maxConcurrent;
    }
}
