package com.instaclick.pentaho.plugin.amqp.processor;

import com.instaclick.pentaho.plugin.amqp.initializer.Initializer;
import com.instaclick.pentaho.plugin.amqp.AMQPPlugin;
import com.instaclick.pentaho.plugin.amqp.AMQPPluginData;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.AMQP.BasicProperties;
import java.io.IOException;
import java.util.List;
import org.pentaho.di.core.exception.KettleStepException;

public class ProducerProcessor extends BaseProcessor
{
    public ProducerProcessor(final Channel channel, final AMQPPlugin plugin, final AMQPPluginData data, final List<Initializer> initializers)
    {
        super(channel, plugin, data, initializers);
    }

    @Override
    public boolean process(Object[] r) throws KettleStepException, IOException
    {
        System.out.println("PRODUCER process()....");
        if (r == null) {
            plugin.setOutputDone();

            return false;
        }

        if (data.isTransactional && ! data.isTxOpen) {
            channel.txSelect();

            data.isTxOpen = true;
        }

        data.body    = getAmqpBody(r);
        data.routing = (data.routingIndex != null)
            ? getAmqpRoutingKey(r)
            : "";
        data.headers    = getAmqpHeaders(r); // WIP
        System.out.println("process data.headers.toString()"+data.headers.toString());
        System.out.println("process JUST AFTER getAmqpHeaders call");

        // publish the current message
        channel.basicPublish(data.target, data.routing, new BasicProperties.Builder().headers(data.headers).build(), data.body.getBytes());

        // put the row to the output row stream
        plugin.putRow(data.outputRowMeta, r);

        // set metadata about publishing event
        plugin.incrementLinesOutput();

        return true;
    }

    @Override
    public void onSuccess() throws IOException
    {
        if ( ! data.isTxOpen) {
            return;
        }

        plugin.logMinimal("Commit channel transaction");
        channel.txCommit();

        data.isTxOpen = false;
    }

    @Override
    public void onFailure() throws IOException
    {
        if ( ! data.isTxOpen) {
            return;
        }

        channel.txRollback();

        data.isTxOpen = false;
    }
}
