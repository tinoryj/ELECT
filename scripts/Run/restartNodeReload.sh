#!/bin/bash
source ../settings.sh
function reload {
    cd ${PathToELECTPrototype} || exit
    bin/nodetool coldStartup reload
}

reload 
