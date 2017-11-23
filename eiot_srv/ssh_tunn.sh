#!/usr/bin/expect -f

set passwd mikms;

spawn ssh -R 2200:localhost:22 smkim@125.140.110.217
expect {
	"password:" {
		exp_send "$passwd\r"
		exp_continue
	}
	"connecting (yes/no)?" {
		exp_send "yes\r"
		exp_continue
	}
}
interact
