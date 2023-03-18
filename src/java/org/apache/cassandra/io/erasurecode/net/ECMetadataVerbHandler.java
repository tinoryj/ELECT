package org.apache.cassandra.io.erasurecode.net;

import java.io.IOException;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;

public class ECMetadataVerbHandler implements IVerbHandler<ECMetadata>{
    public static final ECMetadataVerbHandler instance = new ECMetadataVerbHandler();
    private static final String ecMetadataDir = System.getProperty("user.dir")+"/data/ECMetadata/";


    @Override
    public void doVerb(Message<ECMetadata> message) throws IOException {
        // receive metadata and record it to files (append)
        
        
    }
}
