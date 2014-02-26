#!/bin/bash

echo -n -e "subscribe control\nannounce control quit\n" | nc localhost 9898
