package org.apache.cassandra.utils.erasurecode.net;

import java.io.IOException;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;

public class ECParityNodeVerbHandler implements IVerbHandler<ECParityNode>{

    @Override
    public void doVerb(Message<ECParityNode> message) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'doVerb'");
    }
    
}
