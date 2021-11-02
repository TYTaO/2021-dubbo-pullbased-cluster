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

    // 预热时间
    public static final long preheatDeadline = System.currentTimeMillis() + 50000;  // todo 预热时间和服务端保持一致


    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        if (invokers == null || invokers.isEmpty())
            return null;
        // 如果 invokers 列表中仅有一个 Invoker，直接返回即可，无需进行负载均衡
        if (invokers.size() == 1)
            return invokers.get(0);

        // 调用 doSelect 方法进行负载均衡
        return doSelect(invokers, url, invocation, "concurrent_load_balance");
    }


    /**
     * 选择负载均衡策略
     *
     * @param invokers invoker列表
     * @param url
     * @param invocation
     * @param type
     * @param <T>
     * @return 经过负载均衡选择的节点
     */
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
            case "concurrent_load_balance":
                return doSelectFromConcurrentLoad(invokers, url, invocation);
        }
        return null;
    }


    /**
     * 通过线程负载率选择不同的Provider进行负载均衡。
     *
     * @param invokers invoker列表
     * @param url
     * @param invocation
     * @param <T>
     * @return invoker 经过负载均衡（线程负载率最小）选择的节点
     */
    protected <T> Invoker<T> doSelectFromConcurrentLoad(List<Invoker<T>> invokers, URL url, Invocation invocation) {

        // 判断所有的Provider是否都预热完成
        boolean allNodeInit = false;
        for (Invoker tinvoker : invokers) {
            allNodeInit = MyRpcStatus.getStatus(tinvoker.getUrl()).isInit.get();
            if (!allNodeInit) {
                break;
            }
        }

        // 预热阶段使用随机策略选择Provider。
        if (System.currentTimeMillis() <= preheatDeadline && !allNodeInit) {
            return doSelect(invokers, url, invocation, "local_random_balance");
        }

        // 选择负载最小的Provider，从优先队列（MyRpcStatus.RPC_QUEUE）中选择。
        RpcRequest  request;
        while ((request = MyRpcStatus.select()) == null) {
            // 若选择时候队列为空，则继续给队列加初始化的值。
            for(Invoker tinvoke : invokers) {
                MyRpcStatus.initQueue(tinvoke.getUrl(), 1); // todo 初始化的次数默认为1
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
}
