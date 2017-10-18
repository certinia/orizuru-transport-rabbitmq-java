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

import java.io.IOException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

import org.apache.avro.generic.GenericContainer;

import com.financialforce.orizuru.AbstractConsumer;
import com.financialforce.orizuru.exception.OrizuruException;
import com.financialforce.orizuru.interfaces.IPublisher;

/**
 * DefaultConsumer
 * <p>
 * RabbitMQ implementation of the Orizuru {@link AbstractConsumer}.
 */
public abstract class DefaultConsumer<I extends GenericContainer, O extends GenericContainer>
		extends AbstractConsumer<I, O> implements Consumer {

	private volatile String consumerTag;
	
	private Channel channel;

	public DefaultConsumer(Channel channel, String incomingQueueName, String outgoingQueueName) {
		
		super(incomingQueueName);
		
		if (outgoingQueueName != null) {
			this.publisher = new DefaultPublisher<O>(channel, outgoingQueueName);
		}
		
		this.channel = channel;
		
	}

	@Override
	public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
			byte[] incomingMessage) throws IOException {

		try {
			consume(incomingMessage);
		} catch (OrizuruException ex) {
			throw new IOException("Failed to consume message", ex);
		}

	}

	@Override
	public void handleConsumeOk(String consumerTag) {
		this.consumerTag = consumerTag;
	}

	@Override
	public void handleCancelOk(String consumerTag) {
	}

	@Override
	public void handleCancel(String consumerTag) throws IOException {
	}

	@Override
	public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
	}

	@Override
	public void handleRecoverOk(String consumerTag) {
	}

	public void setPublisher(IPublisher<O> publisher) {
		this.publisher = publisher;
	}

	public Channel getChannel() {
		return channel;
	}

	public String getConsumerTag() {
		return consumerTag;
	}

}
