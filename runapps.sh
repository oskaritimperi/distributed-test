#!/bin/bash

for i in $(seq 10); do
    python testapp.py &
done
