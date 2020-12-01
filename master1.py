#!/usr/bin/python3

import json
import socket
import time
import sys
import random
import threading

from queue import Queue

class Master:

	class Worker:
		
		def __init__(self, config):
			self.id = config["worker_id"]
			self.total_slots = int(config["slots"])
			self.active_slots = 0
			self.port = config["port"]
		
		'''
		def jsonify(self):
			return json.dumps({"slots": self.total_slots})
		'''
		
		def available(self):
			return self.active_slots < self.total_slots
		
	class Job:
		
		def __init__(self, master, request):
			self.id = request["job_id"]
			self.map_tasks = Queue()
			self.reduce_tasks = Queue()
			
			for mt in request["map_tasks"]:
				map_tasks.put({"task_id": mt["task_id"], "duration": mt["duration"]})
				master.tasks[mt["task_id"]] = {"job_id": self.id, "type": "map"}
			
			for rt in request["reduce_tasks"]:
				reduce_tasks.put({"task_id": rt["task_id"], "duration": rt["duration"]})
				master.tasks[rt["task_id"]] = {"job_id": self.id, "type": "reduce"}

	def __init__(self, config, sch_algo='RR'):
		
		if sch_algo == 'RR':
			self.sch_algo = self.round_robin_algo
		elif sch_algo == 'RANDOM':
			self.sch_algo = self.random_algo
		elif sch_algo == 'LL':
			self.sch_algo = self.least_loaded_algo
		
		self.worker_ids = []
		self.workers = {}
		self.jobs = {}
		self.tasks = {}
		
		for worker_config in config["workers"]:
			self.worker_ids.append(worker_config["worker_id"])
			self.workers[worker_config["worker_id"]] = Master.Worker(worker_config)
		
		'''
		for worker_id in self.worker_ids:
			worker = self.workers[worker_id]
			port = worker.port
			with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as c:
				c.connect('localhost', port))
				message = worker.jsonify()
				c.send(message.encode())
		'''
		
	def listen_requests(self):
		requests_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	
		host = 'localhost'
		requests_port = 5000
		requests_socket.bind((host, requests_port))
		requests_socket.listen(1)

		while True:
			req_conn, addr = requests_socket.accept()
			if(addr[1] != '127.0.0.1'):
				continue
			r = req_conn.recv(2048).decode()
			req_conn.close()
			request = json.loads(r)
			job = Job(request)
			self.jobs[request["job_id"]] = job
			
			while not job.map_tasks.empty():
				
				worker = sch_algo()
				
				map_task = job.map_tasks.get()
				
				send_task(map_task, worker)
			
	def listen_updates(self):
		worker_updates_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		worker_updates_port = 5001
		
		worker_updates_socket.bind(('localhost', worker_updates_port))
		worker_updates_socket.listen(3)
		
		while True:
			conn, addr = worker_updates_socket.accept()
			if(addr[1] != '127.0.0.1'):
				continue
			m = conn.recv(2048).decode()
			conn.close()
			message = json.loads(m)
			
			worker_id = message["worker_id"]
			worker = self.workers[worker_id]
			task_id = message["task_id"]
			job_id = self.tasks[task_id]["job_id"]
			task_type = self.tasks[task_id]["type"]
			
			job = self.jobs[job_id]
					
			if(task_type == "reduce" or job.map_tasks.empty()):
				if(job.reduce_tasks.empty()):
					self.jobs.pop(job_id)
					worker.active_slots -= 1
					continue
				else:
					reduce_task = job.reduce_tasks.get()
					
					send_task(reduce_task, worker)					
			else:
				worker.active_slots -= 1
				continue

	def send_task(self, task, worker):
		host = 'localhost'
		port = int(worker.port)

		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as c:
			c.connect((host, port))
			message = json.dumps(task).encode()
			c.send(message)
	
	def random_algo(self):
		while True:
			worker_id = random.choice(self.worker_ids)
			worker = self.workers[worker_id]
			if(worker.available()):
				worker.active_slots += 1
				return worker

	def round_robin_algo(self):
		free_slot_found = False
		#they said it will be sorted, idk if they meant that we assume it is sorted or we have to sort
		#so I just sorted it 
		worker_ids = []
		for w in self.workers:
			worker_ids.append(w["worker_id"]) 
		worker_ids.sort()
		for worker in worker_ids:
			if self.available_slots[worker] > 0:
				free_slot_found = True
				break
		return worker	

	def least_loaded_algo(self):
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


if __name__ == '__main__':	 

	config_file = open(sys.argv[1], 'r')
	config = json.load(config_file)
	scheduling_algo = str(sys.argv[2])
	
	master = Master(config, scheduling_algo)
	
	listen_requests_thread = threading.Thread(target = master.listen_requests)
	listen_updates_thread = threading.Thread(target = master.listen_updates)
	
	listen_requests_thread.start()
	listen_updates_thread.start()
	
	listen_requests_thread.join()
	listen_updates_thread.join()
