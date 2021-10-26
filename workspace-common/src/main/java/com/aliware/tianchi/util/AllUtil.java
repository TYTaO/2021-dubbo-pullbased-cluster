package com.aliware.tianchi.util;

import java.util.concurrent.CompletableFuture;
import org.apache.dubbo.rpc.AppResponse;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.FutureContext;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.InvokeMode;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;

public class AllUtil {

    public static Result buildFastFailResult(Invocation invocation, String msg) {
        CompletableFuture<AppResponse> future = new CompletableFuture<>();
        RpcInvocation rpcInvocation = (RpcInvocation)invocation;
        rpcInvocation.setInvokeMode(InvokeMode.FUTURE);
        FutureContext.getContext().setCompatibleFuture(future);
        future.completeExceptionally(new RpcException(msg));
        return new AsyncRpcResult(future, invocation);
    }

    public static Result buildErrorResult(Invocation invocation, String msg) {
        CompletableFuture<AppResponse> future = new CompletableFuture<>();
        future.completeExceptionally(new RpcException(msg));
        return new AsyncRpcResult(future, invocation);
    }
}
