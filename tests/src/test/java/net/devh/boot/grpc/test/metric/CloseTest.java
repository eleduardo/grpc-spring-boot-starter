/*
 * Copyright (c) 2016-2020 Michael Zhang <yidongnan@gmail.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
 * Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package net.devh.boot.grpc.test.metric;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import com.google.protobuf.Empty;

import io.grpc.ForwardingServerCall.SimpleForwardingServerCall;
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.test.proto.TestServiceGrpc;
import net.devh.boot.grpc.test.proto.TestServiceGrpc.TestServiceStub;
import net.devh.boot.grpc.test.server.TestServiceImpl;

class CloseTest implements ServerInterceptor {

    private final CountDownLatch callClosed = new CountDownLatch(1);
    private final CountDownLatch listenerClosed = new CountDownLatch(1);

    @Test
    void testCallClosed() throws IOException, InterruptedException, ExecutionException {
        final Server server = InProcessServerBuilder.forName("self")
                .addService(ServerInterceptors.intercept(new TestServiceImpl(), this))
                .build();
        server.start();

        final ManagedChannel channel = InProcessChannelBuilder.forName("self")
                .usePlaintext()
                .build();

        final TestServiceStub stub = TestServiceGrpc.newStub(channel);

        final CountDownLatch counter = new CountDownLatch(1);
        final AtomicReference<Throwable> error = new AtomicReference<>();

        final ClientCallStreamObserver<Empty> observer = (ClientCallStreamObserver<Empty>) stub
                .echo(new StreamObserver<Empty>() {

                    @Override
                    public void onNext(final Empty value) {
                        // Do nothing
                    }

                    @Override
                    public void onError(final Throwable t) {
                        error.set(t);
                        counter.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        counter.countDown();
                    }

                });

        observer.cancel("Cancelled", null);
        counter.await();
        assertNotNull(error.get());
        assertEquals(Code.CANCELLED, ((StatusRuntimeException) error.get()).getStatus().getCode());

        assertTrue(this.listenerClosed.await(5, TimeUnit.SECONDS), "Listener closed");
        assertTrue(this.callClosed.await(5, TimeUnit.SECONDS), "Call closed");

        channel.shutdown();
        server.shutdown();
    }

    @Override
    public <ReqT, RespT> Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> call, final Metadata headers,
            final ServerCallHandler<ReqT, RespT> next) {
        final ServerCall<ReqT, RespT> metricsCall = new SimpleForwardingServerCall<>(call) {

            @Override
            public void close(final Status status, final Metadata trailers) {
                CloseTest.this.callClosed.countDown();
                super.close(status, trailers);
            }

        };
        return new SimpleForwardingServerCallListener<>(next.startCall(metricsCall, headers)) {

            public void onCancel() {
                CloseTest.this.listenerClosed.countDown();
                super.onCancel();
            }

        };
    }

}
