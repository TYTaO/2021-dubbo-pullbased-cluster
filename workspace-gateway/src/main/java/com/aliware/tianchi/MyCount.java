package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.RpcStatus;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MyCount {

    private static final ConcurrentMap<String, MyCount> SERVICE_STATISTICS = new ConcurrentHashMap<String,
            MyCount>();

    private final AtomicInteger active = new AtomicInteger();
    private final AtomicLong succeeded = new AtomicLong();
    private final AtomicInteger failed = new AtomicInteger();
    private final AtomicLong totalSuccElapsed = new AtomicLong();

    // 预热结束后的微调
    private final AtomicLong succeededAfterPreheat = new AtomicLong();
    private final AtomicLong succElapsedAfterPreheat = new AtomicLong();
    // compute Avg AfterPreheat ticks
    private final static int preheatOfAfterPreheat = 2000; // todo
    private final AtomicInteger ticks = new AtomicInteger(0);
    private final AtomicLong elapsedInPreheatOfAfterPreheat = new AtomicLong(0);
    public final AtomicInteger fineTune = new AtomicInteger(0);


    private MyCount() {
    }

    public static MyCount getCount(URL url) {
        String uri = url.toIdentityString();
        return SERVICE_STATISTICS.computeIfAbsent(uri, key -> new MyCount());
    }

    public int getActive() {
        return active.get();
    }

    public long getSucceeded() {
        return succeeded.longValue();
    }

    public long getFailed() {
        return failed.longValue();
    }

    public static boolean beginCount(URL url, int max) {
        max = (max <= 0) ? Integer.MAX_VALUE : max;
        MyCount count = getCount(url);
        if (count.active.get() == Integer.MAX_VALUE) {
            return false;
        }
        for (int i; ; ) {
            i = count.active.get();
            if (i == Integer.MAX_VALUE || i + 1 > max) {
                return false;
            }
            if (count.active.compareAndSet(i, i + 1)) {
                break;
            }
        }
        return true;
    }

    public static void endCount(URL url, long elapsed, boolean succeeded) {
        MyCount count = getCount(url);
        count.active.decrementAndGet();

        if (succeeded) {
            count.succeeded.incrementAndGet();
            count.totalSuccElapsed.addAndGet(elapsed);
        } else {
            count.failed.incrementAndGet();
        }
    }

    public long getSucceededAverageElapsed() {
        long succeeded = getSucceeded();
        if (succeeded == 0) {
            return 0;
        }
        return totalSuccElapsed.get() / succeeded;
    }

    public static void endCountAfterPreheat(URL url, long elapsed, boolean succeeded) {
        MyCount count = getCount(url);
        count.active.decrementAndGet();

        if (succeeded) {
            count.succeededAfterPreheat.incrementAndGet();
            count.succElapsedAfterPreheat.addAndGet(elapsed);
            count.ticks.incrementAndGet();
            count.elapsedInPreheatOfAfterPreheat.addAndGet(elapsed);
        }

        if (count.ticks.get() > preheatOfAfterPreheat) {
            synchronized (count) {
                if (count.ticks.get() > preheatOfAfterPreheat) {
                    long avgElapsed = count.elapsedInPreheatOfAfterPreheat.get() / count.ticks.get();
                    System.out.println("thisAvgElapsed: " + avgElapsed + " totalElapsed: " + count.getSuccElapsedAvgAfterPreheat());
                    if (avgElapsed > count.getSuccElapsedAvgAfterPreheat()) {
                        // dec max
                        count.fineTune.set(-1);
                    } else if (elapsed < count.getSuccElapsedAvgAfterPreheat()) {
                        // inc max
                        count.fineTune.set(1);
                    }
                    count.ticks.set(0);
                    count.elapsedInPreheatOfAfterPreheat.set(0);
                }
            }
        }
    }

    public long getSuccElapsedAvgAfterPreheat() {
        long succeeded = succeededAfterPreheat.get();
        if (succeeded == 0) {
            return Long.MAX_VALUE - 100;
        }
        return succElapsedAfterPreheat.get() / succeeded;
    }
}
