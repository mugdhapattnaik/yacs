#!/usr/bin/python

import json
import socket
import time
import sys
import random
import threading


class Worker:

    def __init__(self):
        self.num_active_slots = 0
        self.num_slots = 0
        self.slots = list()

    #incoming tasks must be queued and scheduled into slots(fcfs round robin cuz worker is dum)
    def schedule(self,task):
        task["start_time"] = time.time()
        slot_lock.acquire()
        if(self.num_active_slots < self.num_slots):
            self.slots.append(task)
            self.num_active_slots +=1
        slot_lock.release()

    def send_updates(self):
        pass

    def run_task(self):
        while True:
            for task in self.slots:
                if(time.time() - task["start_time"] >= task["duration"]):
                    slot_lock.acquire()
                    self.slots.remove(task)
                    self.num_active_slots -=1
                    slot_lock.release()
                    break

slot_lock=threading.Lock()
if __name__ == '__main__':
    host = 'localhost'
    port = int(sys.argv[1])
    worker_id = int(sys.argv[2])

    #have to get slots number from config file somehow - not sure if worker.py has access to config.json file(prolly doesn't)

    task_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    task_socket.bind((host, port))    
    task_socket.listen(5)

    worker = Worker()
    monitor_thread=threading.Thread(target=worker.run_task).start()
    while True:
        conn, addr = task_socket.accept()
        t = conn.recv(2048).decode()
        task = json.loads(t)
        worker.schedule(task)
    conn.close()
