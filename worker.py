#!/usr/bin/python

import json
import socket
import time
import sys
import random
import threading


class Worker:

    def __init__(self, config):
        self.num_active_slots = 0
        self.num_slots = config["slots"]
        self.slots = list()

    #incoming tasks must be queued and scheduled into slots(fcfs round robin cuz worker is dum)
    def schedule(self,task):
        task["start_time"] = time.time()
        slot_lock.acquire()
        if(self.num_active_slots < self.num_slots):
            self.slots.append(task)
            self.num_active_slots +=1
        slot_lock.release()

    def send_updates(self, task):
        finished_task = {"worker_id": worker_id, "task_id": task["task_id"]}
        updates_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        updates_port = 5001
        updates_socket.connect((host, updates_port))
        message = json.dumps(finished_task).encode()
        updates_socket.send(message)
        updates_socket.close()

    def run_task(self):
        while True:
            for task in self.slots:
                if(time.time() - task["start_time"] >= task["duration"]):
                    slot_lock.acquire()
                    self.slots.remove(task)
                    self.num_active_slots -=1
                    self.send_updates(task)
                    slot_lock.release()
                    break
                else:
                    time.sleep(1)

slot_lock=threading.Lock()
if __name__ == '__main__':
    host = 'localhost'
    port = int(sys.argv[1])
    worker_id = int(sys.argv[2])

    #have to get slots number from config file somehow - not sure if worker.py has access to config.json file(prolly doesn't)
    #get slot values from master
        
    worker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    worker_socket.bind(('localhost', port))
    worker_socket.listen(1)
    config_socket, addr = worker_socket.accept()

    c = config_socket.recv(2048).decode()
    config = json.loads(c)
    worker = Worker(config)
    monitor_thread=threading.Thread(target=worker.run_task).start()
    while True:
        task_socket, addr = worker_socket.accept()
        t = task_socket.recv(2048).decode()
        task = json.loads(t)
        worker.schedule(task)
        task_socket.close()
'''
[3,1,2,4]
[2,0,1,3] =>[2,1,3]

'''
