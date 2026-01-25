#!/bin/bash
echo "3T-PURGE Sleeping on initial startup..."
sleep 300
while true; do
	bin/python 3T-PURGE.py
	sleep 60
done
