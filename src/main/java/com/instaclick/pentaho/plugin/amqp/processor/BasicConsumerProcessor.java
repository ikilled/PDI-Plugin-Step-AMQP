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
        final GetResponse response = channel.basicGet(data.target, false);

        if (response == null) {
            return false;
        }

        final byte[] body                       = response.getBody();
        final Envelope envelope                 = response.getEnvelope();
        final long tag                          = envelope.getDeliveryTag();
        final Map<String,Object> headers;
        plugin.logDebug("response: "+response.toString());
        if ( response.getProps() != null )
            headers = response.getProps().getHeaders();
        else
            headers = new HashMap();
            // headers = null;
        // final HashMap<String,Object> headers    = (HashMap)response.getProps().getHeaders();
        // final HashMap<String,String> headers;
        // final Map<String,Object> rawHeaders = response.getProps().getHeaders(); // getProps().getHeaders() got from: https://www.rabbitmq.com/releases/rabbitmq-java-client/v1.7.0/rabbitmq-java-client-javadoc-1.7.0/com/rabbitmq/client/GetResponse.html#getProps() OR Is it response.getProperties().getHeaders() as at https://github.com/dvoraka/AV-checker/blob/master/src/main/java/cz/nkp/edeposit/avchecker/AVReceiver.java#L70
        // for (Map.Entry e : rawHeaders.keySet() ) {
        //     headers.put(e.getKey(), (String)e.getValue() );
        // }

        data.routing = envelope.getRoutingKey();
        data.body    = new String(body);
        data.amqpTag = tag;
        data.headers = headers;



        return true;
    }
}
