package org.apache.cassandra.utils.erasurecode.net;

import java.io.IOException;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;

public class ECReadyVerbHandler implements IVerbHandler<ECReady>{

    @Override
    public void doVerb(Message<ECReady> message) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'doVerb'");
    }
    
}
