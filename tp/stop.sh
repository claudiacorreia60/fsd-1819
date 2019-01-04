#!/bin/bash

ps aux | grep -P 'java -jar' | awk '{ print $2 }' | xargs kill
