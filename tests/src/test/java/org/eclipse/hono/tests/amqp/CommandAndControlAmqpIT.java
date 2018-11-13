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

import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.message.Message;
import org.eclipse.hono.client.MessageConsumer;
import org.eclipse.hono.tests.IntegrationTestSupport;
import org.eclipse.hono.util.CommandConstants;
import org.eclipse.hono.util.EventConstants;
import org.eclipse.hono.util.MessageHelper;
import org.eclipse.hono.util.TenantObject;
import org.eclipse.hono.util.TimeUntilDisconnectNotification;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonHelper;
import io.vertx.proton.ProtonMessageHandler;
import io.vertx.proton.ProtonQoS;
import io.vertx.proton.ProtonReceiver;

/**
 * Command and Control Integration tests for the AMQP adapter.
 */
@RunWith(VertxUnitRunner.class)
public class CommandAndControlAmqpIT extends AmqpAdapterTestBase {

    /**
     * TODO.
     */
    @Before
    public void setUp() {
        tenantId = helper.getRandomTenantId();
        deviceId = helper.getRandomDeviceId(tenantId);
        username = IntegrationTestSupport.getUsername(deviceId, tenantId);
        tenant = TenantObject.from(tenantId, true);
    }

    /**
     * TODO.
     * @param ctx The Vert.x test context.
     * @throws InterruptedException if not all commands and responses are exchanged in time.
     */
    @Test
    public void testSendCommandSucceeds(final TestContext ctx) throws InterruptedException {

        final Async setup = ctx.async();
        final Async notificationReceived = ctx.async();

        connectToAdapter(() -> createConsumer(tenantId, msg -> {
            // expect empty notification with TTD -1
            ctx.assertEquals(EventConstants.CONTENT_TYPE_EMPTY_NOTIFICATION, msg.getContentType());
            final TimeUntilDisconnectNotification notification = TimeUntilDisconnectNotification.fromMessage(msg).orElse(null);
            ctx.assertNotNull(notification);
            log.info("received notification [{}]", notification);
            if (notification.getTtd() == -1) {
                notificationReceived.complete();
            }
        })).compose(conn -> createCommandReceiver(getEndpointName(), (delivery, message) -> {
            // command message received
            createProducer(new Target()).map(s -> {
                sender = s;
                final String to = String.format("%s", getEndpointName());

                final Message response = ProtonHelper.message(to, message.getSubject() + ": ok");
                response.setCorrelationId(message.getCorrelationId());
                final Map<String, Object> value = new HashMap<>();
                value.put(MessageHelper.APP_PROPERTY_STATUS, String.valueOf(HttpURLConnection.HTTP_OK));
                final ApplicationProperties props = new ApplicationProperties(value);
                response.setApplicationProperties(props);
                final Async delivered = ctx.async();
                sender.send(response, del -> {
                    final DeliveryState rs = del.getRemoteState();
                    if (Accepted.class.isInstance(rs)) {
                        log.info("accepted");
                    } else if (Rejected.class.isInstance(rs)) {
                        log.info("rejected");
                    } else {
                        log.info("oops!");
                    }
                    delivered.complete();
                });
                delivered.await();
                return Future.succeededFuture();
            }).otherwise(t -> {
                log.info("fail to create a command receiver for receiving command messages");
                return Future.failedFuture(t);
            });
        })).map(r -> receiver = r).setHandler(ctx.asyncAssertSuccess(ok -> setup.complete()));
        setup.await();
        notificationReceived.await();

        final int totalNoOfCommandsToSend = 1;
        final CountDownLatch responsesReceived = new CountDownLatch(totalNoOfCommandsToSend);
        final AtomicInteger commandsSent = new AtomicInteger(0);
        final AtomicLong lastReceivedTimestamp = new AtomicLong();
        final long start = System.currentTimeMillis();

        while (commandsSent.get() < totalNoOfCommandsToSend) {
            final Async commandSent = ctx.async();
            context.runOnContext(action -> {
                final Buffer payload = Buffer.buffer("value: " + commandsSent.getAndIncrement());
                //final Map<String, Object> value = new HashMap<>();
                //final ApplicationProperties properties = new ApplicationProperties(value);
                helper.sendCommand(tenantId, deviceId, "setValue", "text/plain", payload, null, 200).setHandler(sendAttempt -> {
                    if (sendAttempt.failed()) {
                        log.debug("error sending command {}", commandsSent.get(), sendAttempt.cause());
                    } else {
                        lastReceivedTimestamp.set(System.currentTimeMillis());
                        responsesReceived.countDown();
                        if (responsesReceived.getCount() % 20 == 0) {
                            log.info("commands sent: " + commandsSent.get());
                        }
                        if (responsesReceived.getCount() % 20 == 0) {
                            log.info("commands sent: " + commandsSent.get());
                        }
                        commandSent.complete();
                    }
                });
            });
            commandSent.await();
        }

        final long timeToWait = totalNoOfCommandsToSend * 200;
        if (!responsesReceived.await(timeToWait, TimeUnit.MILLISECONDS)) {
            log.info("Timeout of {} milliseconds reached, stop waiting to receive command responses.", timeToWait);
        }
        final long messagesReceived = totalNoOfCommandsToSend - responsesReceived.getCount();
        log.info("sent {} commands and received {} responses in {} milliseconds",
                commandsSent.get(), messagesReceived, lastReceivedTimestamp.get() - start);
        if (messagesReceived != commandsSent.get()) {
            ctx.fail("did not receive a response for each command sent");
        }
    }

    //-------------------------------------------------< AmqpAdapterTestBase >---
    @Override
    protected Future<MessageConsumer> createConsumer(final String tenantId, final Consumer<Message> messageConsumer) {
        return helper.honoClient.createEventConsumer(tenantId, messageConsumer, closeHandler -> {});
    }

    @Override
    protected String getEndpointName() {
        return CommandConstants.COMMAND_ENDPOINT;
    }

    //-------------------------------------------------< private methods >---

    private Future<ProtonConnection> connectToAdapter(final Supplier<Future<MessageConsumer>> consumerFactory) {
        return helper.registry
                 .addDeviceForTenant(tenant, deviceId, DEVICE_PASSWORD)
                 .compose(ok -> consumerFactory.get())
                 .compose(ok -> connectToAdapter(username, DEVICE_PASSWORD));
    }

    private Future<ProtonReceiver> createCommandReceiver(final String sourceAddress, final ProtonMessageHandler msgHandler) {
        if (context == null) {
            throw new IllegalStateException("not connected");
        }
        final Future<ProtonReceiver> result = Future.future();
        context.runOnContext(action -> {
            final ProtonReceiver receiver = connection.createReceiver(sourceAddress);
            receiver.setQoS(ProtonQoS.AT_LEAST_ONCE);
            receiver.closeHandler(remoteClose -> {
                if (remoteClose.failed()) {
                    log.info("peer closed command receiver link [exception: {}]", remoteClose.cause().getClass().getName());
                    result.tryFail(remoteClose.cause());
                }
            });
            receiver.handler(msgHandler);
            receiver.openHandler(remoteAttach -> {
                if (remoteAttach.failed()) {
                    log.info("peer rejects opening of sender link [exception: {}]", remoteAttach.cause().getClass().getName());
                    result.tryFail(remoteAttach.cause());
                } else {
                    result.complete(receiver);
                }
            });
            receiver.open();
        });
        return result;
    }

}
