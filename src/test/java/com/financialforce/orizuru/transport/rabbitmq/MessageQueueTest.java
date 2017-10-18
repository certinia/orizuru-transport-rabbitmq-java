/**
 * Copyright (c) 2017, FinancialForce.com, inc
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 *   are permitted provided that the following conditions are met:
 *
 * - Redistributions of source code must retain the above copyright notice,
 *      this list of conditions and the following disclaimer.
 * - Redistributions in binary form must reproduce the above copyright notice,
 *      this list of conditions and the following disclaimer in the documentation
 *      and/or other materials provided with the distribution.
 * - Neither the name of the FinancialForce.com, inc nor the names of its contributors
 *      may be used to endorse or promote products derived from this software without
 *      specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 *  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 *  OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 *  THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 *  EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 *  OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY
 *  OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 **/

package com.financialforce.orizuru.transport.rabbitmq;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.rules.ExpectedException;

import com.financialforce.orizuru.transport.rabbitmq.exception.MessagingException;

public class MessageQueueTest {

	@Rule
	public final ExpectedException exception = ExpectedException.none();

	@Rule
	public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

	@Test
	public void createChannel_shouldReturnTheChannel() throws Exception {

		// given
		String expectedCloudAmqpUrl = "localhost";

		environmentVariables.set("CLOUDAMQP_URL", expectedCloudAmqpUrl);

		ConnectionFactory factory = mock(ConnectionFactory.class);
		Connection connection = mock(Connection.class);
		Channel expectedChannel = mock(Channel.class);
		when(factory.newConnection()).thenReturn(connection);
		when(connection.createChannel()).thenReturn(expectedChannel);

		MessageQueue messageQueue = new MessageQueue(factory);

		// when
		Channel channel = messageQueue.createChannel();

		// then
		assertEquals(expectedChannel, channel);
		verify(factory, times(1)).setUri(eq(expectedCloudAmqpUrl));
		verify(factory, times(1)).newConnection();
		verify(connection, times(1)).createChannel();

	}

	@Test
	public void createChannel_ThrowMessagingExceptionForCreateChannelException() throws Exception {

		// given
		String expectedCloudAmqpUrl = "localhost";

		environmentVariables.set("CLOUDAMQP_URL", expectedCloudAmqpUrl);

		ConnectionFactory factory = mock(ConnectionFactory.class);
		Connection connection = mock(Connection.class);
		when(factory.newConnection()).thenReturn(connection);
		when(connection.createChannel()).thenThrow(MessagingException.class);

		MessageQueue messageQueue = new MessageQueue(factory);

		// expect
		exception.expect(MessagingException.class);

		// when
		messageQueue.createChannel();

	}

	@Test
	public void consume_shouldCallBasicConsume() throws Exception {

		// given
		String expectedQueueName = "testQueue";

		ConnectionFactory factory = mock(ConnectionFactory.class);
		Channel channel = mock(Channel.class);
		DefaultConsumer consumer = mock(DefaultConsumer.class);
		when(consumer.getQueueName()).thenReturn(expectedQueueName);

		MessageQueue messageQueue = new MessageQueue(factory);

		// when
		messageQueue.consume("consumer", channel, consumer);

		// then
		verify(channel, times(1)).basicConsume(anyString(), anyBoolean(), anyString(), any());

	}

	@Test
	public void consume_shouldThrowMessagingExceptionForBasicConsumeException() throws Exception {

		// given
		ConnectionFactory factory = mock(ConnectionFactory.class);
		Channel channel = mock(Channel.class);
		DefaultConsumer consumer = mock(DefaultConsumer.class);
		when(consumer.getQueueName()).thenThrow(NullPointerException.class);

		MessageQueue messageQueue = new MessageQueue(factory);

		// expect
		exception.expect(MessagingException.class);

		// when
		messageQueue.consume("consumer", channel, consumer);

	}

}
