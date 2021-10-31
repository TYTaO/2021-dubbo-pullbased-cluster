package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.RpcStatus;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MyRpcStatus {
    private static final ConcurrentMap<String, MyRpcStatus> MY_SERVICE_STATISTICS = new ConcurrentHashMap<String,
            MyRpcStatus>();
    public static ConcurrentSkipListSet<RpcRequest> RPC_QUEUE = new ConcurrentSkipListSet<>(new RpcRequest.Compartor());

    public static final int defaultWeight = 10;  // todo
    private static final int initTimeout = 160;
    public final AtomicInteger LastElapsed = new AtomicInteger();
    //    public static AtomicLong initCount = new AtomicLong(1000);
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

    // Priority queue

    // select first element.
    public static RpcRequest select() {
        RpcRequest request = RPC_QUEUE.pollFirst();
//        if (request != null) {
//            System.out.println("URL: " + request.url + "  Load: " + request.weight);
//        }
        return request;
    }

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

    public static void initQueue(URL url, int maxLength) {
        for (int i = 0; i < maxLength; i++) {
            record(url, defaultWeight);
        }
    }
}
