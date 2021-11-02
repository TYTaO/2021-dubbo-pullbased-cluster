package com.aliware.tianchi;

import org.apache.dubbo.common.URL;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ServerRpcStatus {

    private static final ConcurrentMap<String, ServerRpcStatus> SERVICE_STATISTICS = new ConcurrentHashMap<String,
            ServerRpcStatus>();

    private static final int initConcurrentLimit = 35;
    private static final long interval = 2000; // 2 s
    private static final long preheatInterval = 600; // 0.6 s 单次统计预热
    private static final long preheatIntervalSum = 50000; // 5 s 预热
    private static final long preheatSumDdl = System.currentTimeMillis() + preheatIntervalSum;
    private static final int fixChange = 4; // 预热阶段对concurrentLimit的调整

    private final AtomicInteger active = new AtomicInteger(0);
    private final AtomicLong succeeded = new AtomicLong(0);
    private final AtomicInteger failed = new AtomicInteger(0);
    private final AtomicInteger lastReq = new AtomicInteger(0); // 上一次统计的正确请求数
    private final AtomicInteger bestReq = new AtomicInteger(0); // 最后的一次正确请求数
    public final AtomicInteger bestConcurrentLimit = new AtomicInteger(0);
    private final AtomicInteger lastConcurrentLimit = new AtomicInteger(0);
    private final AtomicInteger thisReqTmp = new AtomicInteger(0); // 临时统计的正确请求数，用于记录每次统计
    private final AtomicInteger thisConcurrentLimit = new AtomicInteger(initConcurrentLimit); // 当前的并发限制
    private final AtomicLong lastUpdateTime = new AtomicLong(System.currentTimeMillis());


    private ServerRpcStatus() {
    }

    public static ServerRpcStatus getStatus(URL url) {
        String uri = url.toIdentityString();
        return SERVICE_STATISTICS.computeIfAbsent(uri, key -> new ServerRpcStatus());
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

    private boolean isPreheatOver() {
        return System.currentTimeMillis() > preheatSumDdl;
    }

    public static boolean beginCount(URL url, int max) {
        max = (max <= 0) ? Integer.MAX_VALUE : max;
        ServerRpcStatus status = getStatus(url);
        if (status.active.get() == Integer.MAX_VALUE) {
            return false;
        }
        for (int i; ; ) {
            i = status.active.get();
            if (i == Integer.MAX_VALUE || i + 1 > max) {
                return false;
            }
            if (status.active.compareAndSet(i, i + 1)) {
                break;
            }
        }
        return true;
    }

    public static void endCount(URL url, boolean succeeded) {
        ServerRpcStatus status = getStatus(url);
        status.active.decrementAndGet();

        if (succeeded) {
            status.succeeded.incrementAndGet();
            if (!status.isPreheatOver()) {
                status.updateThisReq();
                status.tryTimedUpdateConcurrentLimit();
            }
        } else {
            status.failed.incrementAndGet();
        }
    }

    private void updateThisReq() {
        if (System.currentTimeMillis() > lastUpdateTime.get() + preheatInterval) {
            thisReqTmp.incrementAndGet();
        }
    }

    private void tryTimedUpdateConcurrentLimit() {
        if (System.currentTimeMillis() > lastUpdateTime.get() + interval) {
            synchronized (this) { // 参考单例模式
                if (System.currentTimeMillis() > lastUpdateTime.get() + interval) {
                    lastUpdateTime.set(System.currentTimeMillis());
                    if (thisReqTmp.get() < 100) { // 统计期间请求量过小，不算入统计
                        thisReqTmp.set(0);
                        return;
                    }
                    int thisReq = thisReqTmp.get();
                    int nextMax = getNextConcurrentLimit(lastReq.get(), thisReq, lastConcurrentLimit.get(), thisConcurrentLimit.get());

                    // update
                    updateBestConcurrentLimit(thisConcurrentLimit.get(), thisReq);
                    System.out.println("max: " + thisConcurrentLimit.get() + " req: " + thisReq + " bestMax: " + bestConcurrentLimit + " bestReq: " + bestReq);

                    lastReq.set(thisReq);
                    lastConcurrentLimit.set(thisConcurrentLimit.get());

                    if (nextMax < 1) { // 极端
                        nextMax = 2;
                        thisConcurrentLimit.set(nextMax);
                    } else {
                        thisConcurrentLimit.set(nextMax);
                    }

                    thisReqTmp.set(0);
                }
            }
        }
    }

    private void updateBestConcurrentLimit(int thisMax, int thisReq) {
        if (thisReq > bestReq.get()) {
            bestReq.set(thisReq);
            bestConcurrentLimit.set(thisMax);
        }
    }

    /**
     * 依据前后两次不同并发限制期间统计的正确请求数来得到下次的并发限制
     *
     * @param lastReq
     * @param thisReq
     * @param lastMax
     * @param thisMax
     * @return
     */
    private int getNextConcurrentLimit(int lastReq, int thisReq, int lastMax, int thisMax) {
        boolean isAdd = (thisReq - lastReq) * (thisMax - lastMax) > 0; // 得到梯度的正负

        if (isAdd) {
            return thisMax + fixChange;
        } else {
            return thisMax - fixChange;
        }
    }

    /**
     * 预热阶段返回当前尝试的并发限制，预热结束后就返回历史最佳并发限制
     *
     * @return
     */
    public int getConcurrentLimit() {
        if (isPreheatOver()) {
            return this.bestConcurrentLimit.get();
        }
        return this.thisConcurrentLimit.get();
    }

    /**
     * 预热阶段返回0代表还在预热，预热结束后就返回历史最佳并发限制
     *
     * @return
     */
    public int getConcurrentLimitToClient() {
        if (isPreheatOver()) {
            return this.bestConcurrentLimit.get();
        }
        return 0;
    }
}
