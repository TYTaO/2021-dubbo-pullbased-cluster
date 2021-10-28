package com.aliware.tianchi;

import java.util.Comparator;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author xkyang
 * @package com.aliware.tianchi
 * @date 2021/10/28/19:44
 */
public class RpcRequest {
    public final String url;
    public Double weight;

    public RpcRequest(String url, double weight) {
        this.url = url;
        this.weight = weight + ThreadLocalRandom.current().nextDouble();
    }

    static class Compartor implements Comparator<RpcRequest> {

        @Override
        public int compare(RpcRequest rpc1, RpcRequest rpc2) {
            return rpc1.weight.compareTo(rpc2.weight);
        }
    }
}
