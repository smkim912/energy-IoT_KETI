#!/bin/sh
cd /home/keti/runner
tmux new -s tunn -d './ssh_tunn.sh'
tmux new -s eiot -d 'python eiot_sub.py'
