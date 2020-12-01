#!/usr/bin/python

import json
import socket
import time
import sys
import random
import threading

from queue import SimpleQueue

class Master:

    def __init__(self, config, sch_algo='RR'):
        self.request_queue = SimpleQueue()
        self.workers = config["workers"]
        #print(self.workers)
        self.available_slots = {}
        
        if sch_algo == 'RR':
            self.sch_algo = self.round_robin
        elif sch_algo == 'RANDOM':
            self.sch_algo = self.random
        elif sch_algo == 'LL':
            self.sch_algo = self.least_loaded

        for worker in self.workers:
            self.available_slots[worker["worker_id"]] = worker["slots"]
            host = worker["host"]
            port = worker["port"]
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as c:
                c.connect((host, port))
                message = json.dumps(worker).encode()
                c.send(message)
        worker_message = threading.Thread(target = self.listen).start()
        #have to define shared variable(can be queue of requests) - techinically master doesn't care about slots, 
        #only the scheduler does - can keep scheduler independent
    
    def listen(self):
        #print(worker_updates_port)
        worker_updates_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        worker_updates_port = 5001
        
        worker_updates_socket.bind((host, worker_updates_port))
        worker_updates_socket.listen(3)
        while True:
            conn, addr = worker_updates_socket.accept()
            m = conn.recv(2048).decode()
            message = json.loads(m)
            #print(message)
            available.acquire()
            self.available_slots[message["worker_id"]] +=1
            available.release()
            #print("-",self.available_slots)
            #decide the format of message sent by workers 
            if message["Dependency"] == True:   
                mapTaskcounts[message["job_id"]] -=1
                if mapTaskcounts[message["job_id"]] == 0:
                    for reduce_task in reduceTasks[message["job_id"]]:
                        reduce_task["job_id"] = message["job_id"]
                        reduce_task["Dependency"] = False
                        taskQueue.append(reduce_task)
            #update active slots
            conn.close()
    
    def random(self):
        free_slot_found = False
        worker_ids = []
        for w in self.workers:
            worker_ids.append(w["worker_id"]) 
        while not free_slot_found:
            worker = worker_ids[random.randrange(0, (len(self.workers)))]
            if self.available_slots[worker] > 0: 
                free_slot_found = True
                break
        return worker

    def round_robin(self):
        free_slot_found = False
        #they said it will be sorted, idk if they meant that we assume it is sorted or we have to sort
        #so I just sorted it 
        worker_ids = []
        for w in self.workers:
            worker_ids.append(w["worker_id"]) 
        worker_ids.sort()
        for worker in worker_ids:
            print(self.available_slots)
            if self.available_slots[worker] > 0:
                free_slot_found = True
                break

        return worker    

    def least_loaded(self):
        free_slot_found = False
        while not free_slot_found:
            max_slots = max(self.available_slots.values())
            if max_slots == 0:
                time.sleep(1)
            else:
                #to get key from value
                worker = list(self.available_slots.keys())[self.available_slots.values().index(max_slots)]
                free_slot_found = True
                break
        return worker

    def parse(self, request):
        if len(request["map_tasks"]) == 0:
            return False
        else:
            t = (request)
            self.request_queue.put(t)            
            return True
    '''
    def schedule(self,request):
        map_tasks = request["map_tasks"]
        for mapper in map_tasks:
            w = self.sch_algo() #returns a worker that is free for task
            available.acquire()
            self.available_slots[w] -= 1
            print(self.available_slots)
            	print(map_tasks)
            available.release()
            self.send_task(mapper, w)
        
        reduce_tasks = request["reduce_tasks"]
        for reducer in reduce_tasks:
            w = self.sch_algo() #returns a worker_id that is free for task
            available.acquire()
            self.available_slots[w] -= 1
            t = (reducer["task_id"], w)
            self.request_queue.put(t)
            available.release()
            self.send_task(reducer, w)
    '''
    def scheduler(self):	 
        #Needs to be finished
        while True:
            for tasks in taskQueue:
                w = self.sch_algo() #returns a worker_id that is free for task

                task = taskQueue[0]
                taskQueue.remove(task)
                self.send_task(task, w)
        
         
    def get_req(self, request):	 
        map_tasks = request["map_tasks"]
        reduce_tasks = request["reduce_tasks"]
        mapTaskcounts[request["job_id"]] = {}
        mapTaskcounts[request["job_id"]] = len(request["map_tasks"])
        reduceTasks[request["job_id"]] = []
        #taskQueue[request["job_id"]] = []
        #store reducer tasks in reduceTasks so that it can be queued once mapper tasks have finished running
        for reducer in reduce_tasks:
            reduceTasks[request["job_id"]].append(reducer)  
        #store mapper tasks in taskQueue     
        task={}
        for mapper in map_tasks:
            mapper["job_id"] = request["job_id"]
            mapper["Dependency"] = True
            taskQueue.append(mapper)  
                
    def send_task(self, task, worker_id):
        host = 'localhost' #TBD
        port = 1
        for w in self.workers:
            if w["worker_id"] == worker_id:
                port = int(w["port"])
                
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as c:
            c.connect((host, port))
            message = json.dumps(task).encode()
            c.send(message)
        available.acquire()
        #print(type(worker_id),worker_id)
        #print(self.available_slots[worker_id])
        self.available_slots[worker_id] -=1
        print("-",self.available_slots)
        available.release()

available=threading.Lock()
mapTaskcounts = {}
taskQueue = []
reduceTasks = {}
if __name__ == '__main__':     

    requests_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    host = 'localhost'
    requests_port = 5000
    requests_socket.bind((host, requests_port))
    requests_socket.listen(3)

    
    config_file = open(sys.argv[1], 'r')
    config = json.load(config_file)
    scheduling_algo = str(sys.argv[2])
    
    master = Master(config, scheduling_algo)
    scheduler_thread = threading.Thread(target = master.scheduler).start()
    while True:
        req_conn, addr = requests_socket.accept()
        r = req_conn.recv(2048).decode()
        request = json.loads(r)
        if master.parse(request):
            master.get_req(request) #currently schedule is being called on a per job basis, we need it to be called only once, after we receive all job requests
        req_conn.close()

        
        
'''
to listen:

#workers send updates to this port
worker_updates_socket.bind((host, worker_updates_port))
worker_updates_socket.listen(3)
while True:
    up_conn, addr = worker_updates_socket.accept()
    update_message = up_conn.recv.decode()
    #change number of active slots based on update message
        
'''
