python3 worker1.py 4000 1 & python3 worker1.py 4001 2 & python3 worker1.py 4001 3 & python3 master1.py config.json RANDOM & python3 requests.py 5
