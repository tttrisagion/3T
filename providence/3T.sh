#!/bin/bash
while true; do
	cd /opt/3T-protected
	source bin/activate
	./3T-PURGE.sh &
	bin/python 3T-FEED.py &
	bin/python 3T-VOLATILITY.py &
	bin/python 3T-PROVIDENCE.py
	pkill -f 3T-FEED
	pkill -f 3T-VOLATILITY
	pkill -f 3T-PURGE
	sleep 1
done
