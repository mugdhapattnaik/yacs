python3 worker2.py 4000 1 &
python3 worker2.py 4001 2 &
python3 worker2.py 4002 3 &
python3 worker2.py 4003 4 &
python3 master2.py config.json 'RANDOM' &

