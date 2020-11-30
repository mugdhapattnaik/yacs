#!/usr/bin/python

import json
import socket
import time
import sys
import random



class Worker:

    def __init__(self):
        self.num_active_slots = 0
        self.slots = 0

    #incoming tasks must be queued and scheduled into slots(fcfs round robin cuz worker is dum)
    def schedule(self):
        pass

    def send_updates(self):
        pass

    def run_task(self, task):
        pass
    


if __name__ == '__main__':
    host = 'localhost'
    port = int(sys.argv[1])
    print("port=",port)
    worker_id = int(sys.argv[2])
    
    #have to get slots number from config file somehow - not sure if worker.py has access to config.json file(prolly doesn't)

    task_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    task_socket.bind((host, port))    
    task_socket.listen(5)

    worker = Worker()
    while True:
        conn, addr = task_socket.accept()
        t = conn.recv(2048).decode()
        task = json.loads(t)
        print(task)
        worker.run_task(task)
    conn.close()
