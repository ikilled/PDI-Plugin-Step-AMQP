package com.instaclick.pentaho.plugin.amqp.processor;

import com.instaclick.pentaho.plugin.amqp.initializer.Initializer;
import com.instaclick.pentaho.plugin.amqp.AMQPPlugin;
import com.instaclick.pentaho.plugin.amqp.AMQPPluginData;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class BasicConsumerProcessor extends BaseConsumerProcessor
{
    public BasicConsumerProcessor(final Channel channel, final AMQPPlugin plugin, final AMQPPluginData data, final List<Initializer> initializers)
    {
        super(channel, plugin, data, initializers);
    }

    @Override
    protected boolean consume() throws IOException
    {
        System.out.println("ENTERED: consume()");
        final GetResponse response = channel.basicGet(data.target, false);

        if (response == null) {
            return false;
        }

        final byte[] body                       = response.getBody();
        final Envelope envelope                 = response.getEnvelope();
        final long tag                          = envelope.getDeliveryTag();
        final Map<String,Object> headers;
        // System.out.println("BasicConsumerProcessor consume(): response.getProps(): " + response.getProps() );
        if ( response.getProps() != null )
            headers = response.getProps().getHeaders();
        else
            headers = new HashMap<String,Object>();
            // headers = null;

        data.routing = envelope.getRoutingKey();
        data.body    = new String(body);
        data.amqpTag = tag;
        data.headers = headers;

        System.out.println("CONSUME(): data.headers = "+data.headers.entrySet() );

        System.out.println("consume() EXITING...");

        return true;
    }
}
