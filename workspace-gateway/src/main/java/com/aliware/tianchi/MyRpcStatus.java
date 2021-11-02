package com.aliware.tianchi;

import org.apache.dubbo.common.URL;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class MyRpcStatus {
    private static final ConcurrentMap<String, MyRpcStatus> MY_SERVICE_STATISTICS = new ConcurrentHashMap<String,
            MyRpcStatus>();

    // 优先队列，存储节点的负载信息。
    public static ConcurrentSkipListSet<RpcRequest> RPC_QUEUE = new ConcurrentSkipListSet<>(new RpcRequest.Compartor());

    public static final int defaultWeight = 10;  // todo 初始化队列时候默认的权重
    private static final int initTimeout = 160;
    public final AtomicInteger LastElapsed = new AtomicInteger();
    public AtomicInteger maxConcurrent = new AtomicInteger();
    public AtomicBoolean isInit = new AtomicBoolean(false);

    private MyRpcStatus() {
    }

    public static MyRpcStatus getStatus(URL url) {
        String uri = url.toIdentityString();
        return MY_SERVICE_STATISTICS.computeIfAbsent(uri, key -> new MyRpcStatus());
    }

    public static void endCount(URL url, long elapsed, boolean succeeded) {
        MyRpcStatus status = getStatus(url);
        if (succeeded) {
            status.LastElapsed.set((int) elapsed);
        } else {
            if (elapsed > status.LastElapsed.get()) {
                status.LastElapsed.set((int) elapsed);
            }
        }
    }

    public static Integer getTimeout(URL url) {
        MyRpcStatus status = getStatus(url);
        int lastElapsed = status.LastElapsed.get();
        if (lastElapsed == 0) {
            return initTimeout;
        }
        int max = (int) MyCount.getCount(url).getSucceededAverageElapsed() * 3;
        if (lastElapsed > max) {
            return max;
        }
        return lastElapsed + 3; // magic num
    }

    /**
     * 在优先队列中取得队头，为负载率最小的节点。
     *
     * @return RpcRequest 可标识负载率最小的节点（其中的url字段）。
     */
    public static RpcRequest select() {
        return RPC_QUEUE.pollFirst();
    }

    /**
     * 将节点负载信息入队，在OnResponse,OnError及队列初始化时使用。
     *
     * @param url 表示不同的Provider。
     * @param active 该Provider的当前正在运行的请求数。
     */
    public static void record(URL url, int active) {
        String uri = url.toIdentityString();
        if (active != -1) {
            int maxActive = MyRpcStatus.getStatus(url).maxConcurrent.get();
            RPC_QUEUE.add(new RpcRequest(uri, (active + ThreadLocalRandom.current().nextDouble()) / (maxActive + 1)));
        } else {
            RPC_QUEUE.add(new RpcRequest(uri, 1.0 + ThreadLocalRandom.current().nextDouble()));
        }
        if (RPC_QUEUE.size() > 300) {
            RPC_QUEUE.pollLast();
        }
    }

    /**
     * 初始化优先队列
     *
     * @param url
     * @param maxLength
     */
    public static void initQueue(URL url, int maxLength) {
        for (int i = 0; i < maxLength; i++) {
            record(url, defaultWeight);
        }
    }
}
