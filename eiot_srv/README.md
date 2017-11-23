(KETI) e-IoT analysis server program
-------------------------------------
### 2017.11.22 대전한솔제지 현장설치
- python code
- since 11.22 16:00
- Required capacity for DB is expected to be 19GB per 1-year
- vpn.hansol.co.kr (nexg03)


> e-iot_sub.py: Main server program based on mqtt subscriber

> e-iot_eval.py: e-iot_sub.py w/ performance evaluation code (old)

> ssh_tunn.sh: ssh tunneling script (deprecated)

> run_sub.conf: upstart for 'e-iot_sub.py'

> run_tunn.conf: upstart for 'ssh_tunn.sh' (deprecated)
