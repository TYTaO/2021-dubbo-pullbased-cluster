package com.aliware.tianchi;

import java.util.Comparator;

/**
 * 记录Rpc请求的返回。
 * url标识不同的Provider, weight表示当前请求运行时的线程负载率。
 * 此类会被MyRpcStatus.RPC_QUEUE使用，作为优先队列中的节点，weight越小越靠前。
 */
public class RpcRequest {
    public final String url;
    public Double weight;

    /**
     * 返回的Rpc请求，通过OnResponse和OnError时记录
     *
     * @param url 标识不同的Provider
     * @param weight 线程负载率
     */
    public RpcRequest(String url, double weight) {
        this.url = url;
        this.weight = weight;
    }

    /**
     * 自定义比较类，选择weight(线程负载率)小的。
     */
    static class Compartor implements Comparator<RpcRequest> {
        @Override
        public int compare(RpcRequest rpc1, RpcRequest rpc2) {
            return rpc1.weight.compareTo(rpc2.weight);
        }
    }
}
