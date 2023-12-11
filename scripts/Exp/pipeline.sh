#!/bin/bash

while true; do
    if ! ps aux | grep "Exp6-breakdown.sh" | grep -v grep >/dev/null; then
        # if [ ! -f playbook-update.yaml ]; then
        #     cp ../playbook/playbook-update.yaml .
        # fi
        # ansible-playbook -v -i hosts.ini playbook-update.yaml
        echo "Start running Exp6 of experiments..."
        ./Exp7-recovery.sh
        exit 0
    fi
    echo "Waiting for the Exp6-breakdown of experiments to finish..."
    sleep 60
done
