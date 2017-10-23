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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;

import com.rabbitmq.client.Channel;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.financialforce.orizuru.exception.publisher.OrizuruPublisherException;
import com.financialforce.orizuru.exception.publisher.encode.EncodeTransportException;
import com.financialforce.orizuru.message.Context;

public class DefaultPublisherTest {

	@Rule
	public final ExpectedException exception = ExpectedException.none();

	@Test
	public void publish_shouldCallTheChannelBasicPublishMethod() throws Exception {

		// given
		Schema schema = new Schema.Parser().parse("{\"name\":\"test\",\"type\":\"record\",\"fields\":[]}");
		
		Context context = mock(Context.class);
		when(context.getSchema()).thenReturn(schema);
		when(context.getDataBuffer()).thenReturn(ByteBuffer.wrap("test".getBytes()));

		GenericContainer message = mock(GenericContainer.class);
		when(message.getSchema()).thenReturn(schema);

		Channel channel = mock(Channel.class);
		DefaultPublisher<GenericContainer> publisher = new DefaultPublisher<GenericContainer>(channel, "output");

		// when
		publisher.publish(context, message);

		// then
		verify(channel, times(1)).basicPublish(any(), any(), any(), any());

	}
	
	@Test
	public void publish_shouldThrowAnOrizuruPublisherExceptionIfTheMessagePublishingFails() throws Exception {

		// given
		Schema schema = new Schema.Parser().parse("{\"name\":\"test\",\"type\":\"record\",\"fields\":[]}");
		
		Context context = mock(Context.class);
		when(context.getSchema()).thenReturn(schema);

		GenericContainer message = mock(GenericContainer.class);
		when(message.getSchema()).thenReturn(schema);

		Channel channel = mock(Channel.class);
		DefaultPublisher<GenericContainer> publisher = new DefaultPublisher<GenericContainer>(channel, "output");

		// expect
		exception.expect(OrizuruPublisherException.class);
		exception.expectMessage("Failed to publish message");
		exception.expectCause(IsInstanceOf.<Throwable>instanceOf(EncodeTransportException.class));
		
		// when
		publisher.publish(context, message);

	}

}
