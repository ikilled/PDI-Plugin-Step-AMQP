package com.instaclick.pentaho.plugin.amqp.processor;

import com.instaclick.pentaho.plugin.amqp.initializer.Initializer;
import com.instaclick.pentaho.plugin.amqp.AMQPPlugin;
import com.instaclick.pentaho.plugin.amqp.AMQPPluginData;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import org.pentaho.di.core.exception.KettleStepException;

abstract class BaseProcessor implements Processor
{
    final List<Initializer> initializers;
    final AMQPPluginData data;
    final AMQPPlugin plugin;
    final Channel channel;

    public BaseProcessor(final Channel channel, final AMQPPlugin plugin, final AMQPPluginData data, final List<Initializer> initializers)
    {
        this.initializers = initializers;
        this.channel      = channel;
        this.plugin       = plugin;
        this.data         = data;
    }

    protected String getAmqpRoutingKey(final Object[] r) throws KettleStepException
    {
        if (hasAmqpRoutingKey(r)) {
            return (r[data.routingIndex] == null) ? null : r[data.routingIndex].toString();
        }

        logUndefinedRow(r, "Invalid routing key", "ICAmqpPlugin001");

        return null;
    }

    protected String getAmqpBody(final Object[] r) throws KettleStepException
    {
        if (hasAmqpBody(r)) {
            return (r[data.bodyFieldIndex] == null) ? "" : r[data.bodyFieldIndex].toString();
        }

        logUndefinedRow(r, "Invalid body", "ICAmqpPlugin002");

        return null;
    }

    // // WIP WIP WIP WIP WIP WIP WIP WIP
    protected Map<String,Object> getAmqpHeaders(final Object[] r) throws KettleStepException
    {
        System.out.println("ENTERED BaseProcessor getAmqpHeaders");
        System.out.println("data.routingIndex:"+data.routingIndex);
        System.out.println("headersNamesFieldsIndexes:"+data.headersNamesFieldsIndexes.keySet());
        System.out.println("headersNames2FieldsNames:"+data.headersNames2FieldsNames.keySet());

        HashMap<String,Object> headers = new HashMap<String,Object>(); //WIP
        Integer rowFieldIndex;
        System.out.println("constructing headers to be attached to produced amqp msg...");
        for ( String headerName : data.headersNames2FieldsNames.keySet() ) {
            rowFieldIndex = data.headersNamesFieldsIndexes.get(headerName);
            if( rowFieldIndex != null ) {
                System.out.println("headerName:"+headerName+",rowFieldIndex:"+rowFieldIndex+",content:"+r[rowFieldIndex]);
                headers.put( headerName, r[rowFieldIndex] );
            }
        }

        // logUndefinedRow(r, "Invalid headers", "ICAmqpPlugin003"); // EMPTY headers - it's okay to have no headers

        return headers;

    }
    // // WIP WIP WIP WIP WIP WIP WIP WIP

    protected boolean hasAmqpRoutingKey(final Object[] r)
    {
        return rowContains(r, data.routingIndex);
    }

    protected boolean hasAmqpBody(final Object[] r)
    {
        return rowContains(r, data.bodyFieldIndex);
    }

    // // WIP WIP WIP WIP WIP WIP WIP WIP
    // protected boolean hasAmqpHeaders(final Object[] r)
    // {   
    //     // foreach fieldnames2index do:
    //         return rowContains(r, data.bodyFieldIndex); // (do it only once, one header is enough
    // }
    // // WIP WIP WIP WIP WIP WIP WIP WIP

    protected boolean rowContains(final Object[] r, final Integer index)
    {
        if (index == null || index < 0) {
            return false;
        }

        return (r.length > index);
    }

    protected void logUndefinedRow(final Object[] r, final String log, final String code) throws KettleStepException
    {
        final String msg = plugin.getLinesRead() + " - " + log;

        if (plugin.isDebug()) {
            plugin.logDebug(msg);
        }

        plugin.putError(plugin.getInputRowMeta(), r, 1, msg, null, code);
    }

    @Override
    public void shutdown() throws IOException
    {
        if (channel.isOpen()) {
            channel.close();
        }
    }

    @Override
    public void start() throws KettleStepException, IOException
    {
        for (final Initializer initializer : initializers) {
            initializer.initialize(channel, plugin, data);
        }
    }

    @Override
    public void cancel() throws IOException
    {
    }


}
