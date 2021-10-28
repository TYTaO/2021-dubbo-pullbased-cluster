package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.RpcStatus;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MyRpcStatus {
    private static final ConcurrentMap<String, MyRpcStatus> MY_SERVICE_STATISTICS = new ConcurrentHashMap<String,
            MyRpcStatus>();

    private static final int initTimeout = 30;
    public final AtomicInteger LastElapsed = new AtomicInteger();
//    public static AtomicLong initCount = new AtomicLong(1000);

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
//        int max = (int) MyCount.getCount(url).getSucceededAverageElapsed() * 3;
        int max = initTimeout;
        if (lastElapsed > max) {
            return max;
        }
        return lastElapsed + 3; // magic num
    }
}
