/*******************************************************************************
 * Copyright (c) 2016, 2018 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 *******************************************************************************/
package org.eclipse.hono.tests.amqp;
import java.util.Objects;
import java.util.UUID;

import org.apache.qpid.proton.amqp.messaging.Target;
import org.eclipse.hono.client.MessageConsumer;
import org.eclipse.hono.tests.IntegrationTestSupport;
import org.eclipse.hono.tests.client.ClientTestBase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.core.net.SelfSignedCertificate;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonClientOptions;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonQoS;
import io.vertx.proton.ProtonSender;
import io.vertx.proton.sasl.impl.ProtonSaslExternalImpl;
import io.vertx.proton.sasl.impl.ProtonSaslPlainImpl;

/**
 * Base class for the AMQP adapter integration tests.
 */
public abstract class AmqpAdapterTestBase extends ClientTestBase {

    /**
     * Support outputting current test's name.
     */
    @Rule
    public TestName testName = new TestName();

    /**
     * A helper for accessing the AMQP 1.0 Messaging Network and
     * for managing tenants/devices/credentials.
     */
    protected static IntegrationTestSupport helper;

    /**
     * The connection established between the device and the AMQP adapter.
     */
    protected ProtonConnection connection;

    protected ProtonSender sender;
    protected MessageConsumer consumer;

    protected static Vertx vertx;
    protected Context context;

    protected static ProtonClientOptions defaultOptions;
    protected SelfSignedCertificate deviceCert;

    /**
     * Create a HTTP client for accessing the device registry (for registering devices and credentials) and
     * an AMQP 1.0 client for consuming messages from the messaging network.
     * 
     * @param ctx The Vert.x test context.
     */
    @BeforeClass
    public static void setup(final TestContext ctx) {
        vertx = Vertx.vertx();
        helper = new IntegrationTestSupport(vertx);
        helper.init(ctx);

        defaultOptions = new ProtonClientOptions()
                .setTrustOptions(new PemTrustOptions().addCertPath(IntegrationTestSupport.TRUST_STORE_PATH))
                .setHostnameVerificationAlgorithm("")
                .setSsl(true);
    }

    /**
     * Shut down the client connected to the messaging network.
     * 
     * @param ctx The Vert.x test context.
     */
    @AfterClass
    public static void disconnect(final TestContext ctx) {
        helper.disconnect(ctx);
    }

    /**
     * Logs a message before running a test case.
     */
    @Before
    public void before() {
        log.info("running {}", testName.getMethodName());
        deviceCert = SelfSignedCertificate.create(UUID.randomUUID().toString());
    }

    /**
     * Disconnect the AMQP 1.0 client connected to the AMQP Adapter and close senders and consumers.
     * Also delete all random tenants and devices generated during the execution of a test case.
     * 
     * @param context The Vert.x test context.
     */
    @After
    public void after(final TestContext context) {
        helper.deleteObjects(context);
        if (deviceCert != null) {
            deviceCert.delete();
        }
        close(context);
    }

    protected Future<ProtonConnection> connectToAdapter(final String username, final String password) {

        final Future<ProtonConnection> result = Future.future();
        final ProtonClient client = ProtonClient.create(vertx);

        defaultOptions.addEnabledSaslMechanism(ProtonSaslPlainImpl.MECH_NAME);
        client.connect(
                defaultOptions,
                IntegrationTestSupport.AMQP_HOST,
                IntegrationTestSupport.AMQPS_PORT,
                username,
                password,
                conAttempt -> handleConnectionAttemptResult(conAttempt, result.completer()));
        return result;
    }

    protected Future<ProtonConnection> connectToAdapter(final SelfSignedCertificate clientCertificate) {

        final Future<ProtonConnection> result = Future.future();
        final ProtonClient client = ProtonClient.create(vertx);

        final ProtonClientOptions secureOptions = new ProtonClientOptions(defaultOptions);
        secureOptions.setKeyCertOptions(clientCertificate.keyCertOptions());
        secureOptions.addEnabledSaslMechanism(ProtonSaslExternalImpl.MECH_NAME);
        client.connect(
                secureOptions,
                IntegrationTestSupport.AMQP_HOST,
                IntegrationTestSupport.AMQPS_PORT,
                conAttempt -> handleConnectionAttemptResult(conAttempt, result.completer()));
        return result;
    }

    /**
     * Creates a test specific message sender.
     * 
     * @param target   The tenant to create the sender for.
     * @return    A future succeeding with the created sender.
     * 
     * @throws NullPointerException if the target or connection is null.
     */
    protected Future<ProtonSender> createProducer(final Target target) {

        Objects.requireNonNull(target, "Target cannot be null");
        if (context == null) {
            throw new IllegalStateException("not connected");
        }

        final Future<ProtonSender>  result = Future.future();
        context.runOnContext(go -> {
            final ProtonSender sender = connection.createSender(target.getAddress());
            sender.setQoS(ProtonQoS.AT_LEAST_ONCE);
            sender.openHandler(remoteAttach -> {
                if (remoteAttach.succeeded()) {
                    result.complete(sender);
                } else {
                    result.fail(remoteAttach.cause());
                }
            });
            sender.open();
        });
        return result;
    }

    private void handleConnectionAttemptResult(final AsyncResult<ProtonConnection> conAttempt, final Handler<AsyncResult<ProtonConnection>> handler) {
        if (conAttempt.failed()) {
            handler.handle(Future.failedFuture(conAttempt.cause()));
        } else {
            this.context = Vertx.currentContext();
            this.connection = conAttempt.result();
            connection.openHandler(remoteOpen -> {
                if (remoteOpen.succeeded()) {
                    handler.handle(Future.succeededFuture(connection));
                } else {
                    handler.handle(Future.failedFuture(remoteOpen.cause()));
                }
            });
            connection.closeHandler(remoteClose -> {
                connection.close();
            });
            connection.open();
        }

    }

    private void close(final TestContext ctx) {
        final Async shutdown = ctx.async();
        final Future<ProtonConnection> connectionTracker = Future.future();
        final Future<ProtonSender> senderTracker = Future.future();
        final Future<Void> receiverTracker = Future.future();

        if (sender == null) {
            senderTracker.complete();
        } else {
            context.runOnContext(go -> {
                sender.closeHandler(senderTracker);
                sender.close();
            });
        }

        if (consumer == null) {
            receiverTracker.complete();
        } else {
            consumer.close(receiverTracker);
        }

        if (connection == null || connection.isDisconnected()) {
            connectionTracker.complete();
        } else {
            context.runOnContext(go -> {
                connection.closeHandler(connectionTracker);
                connection.close();
            });
        }

        CompositeFuture.join(connectionTracker, senderTracker, receiverTracker).setHandler(c -> {
           context = null;
           shutdown.complete();
        });
        shutdown.await();
    }
}
