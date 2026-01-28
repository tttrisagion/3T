#!/bin/bash
while true; do
	./3T-PURGE.sh &
	python 3T-FEED.py &
	python 3T-VOLATILITY.py &
	python 3T-PROVIDENCE-v2.py
	pkill -f 3T-FEED
	pkill -f 3T-VOLATILITY
	pkill -f 3T-PURGE
	sleep 1
done
