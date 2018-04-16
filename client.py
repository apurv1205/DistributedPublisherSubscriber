from concurrent import futures
import time
import grpc
import pr_pb2
import pr_pb2_grpc
import thread
import sys
import json
import socket

_ONE_DAY_IN_SECONDS = 60 * 60 * 24
CENTRAL_SERVER_IP = ""
ACCESS_POINT = ""
SELF_IP=[l for l in ([ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1], [[(s.connect(('8.8.8.8', 53)), s.getsockname()[0], s.close()) for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]) if l][0][0]

if len(sys.argv) < 2 : 
	print "ERROR : Enter the port for access point server...exiting"
	exit()
port = sys.argv[1]
self_ip = str(SELF_IP)+":"+str(port)

class Client(pr_pb2_grpc.PublishTopicServicer):
	def forwardBackup(self, request_iterator, context):
		for request in request_iterator :
			print request
		return pr_pb2.acknowledge(ack="Backup received by the client...")

	def forwardData(self, request, context):
		print request
		return pr_pb2.acknowledge(ack="Data received by the client...")

def serve():
	server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
	pr_pb2_grpc.add_PublishTopicServicer_to_server(Client(), server)
	server.add_insecure_port(self_ip)
	server.start()
	try:
		while True:
			time.sleep(_ONE_DAY_IN_SECONDS)
	except KeyboardInterrupt:
		server.stop(0)

def subscribe_topic(topic,self_ip):
	lst = json.load(open("subscribedTopics"+port,"r"))
	if topic in lst :
		print "Already subscribed to the topic :",topic
	else :
		channel = grpc.insecure_channel(ACCESS_POINT)
		stub = pr_pb2_grpc.PublishTopicStub(channel)
		response = stub.subscribeRequest(pr_pb2.topicSubscribe(topic=topic,client_ip=self_ip))
		lst.append(topic)
		json.dump(lst,open("subscribedTopics"+port,"w"))

def push_topic(topic,data):
	channel = grpc.insecure_channel(ACCESS_POINT)
	stub = pr_pb2_grpc.PublishTopicStub(channel)
	response = stub.publishRequest(pr_pb2.topicData(topic=topic, data=data))
	print("Ack received: " + response.ack)

def get_front_ip():
	channel = grpc.insecure_channel(CENTRAL_SERVER_IP)
	stub = pr_pb2_grpc.PublishTopicStub(channel)
	response = stub.getFrontIp(pr_pb2.empty())
	print("Ip alloted: " + response.ip)
	print response.ip
	return response.ip

def generateTopics(lst,client_ip):
	for topic in lst :
		yield pr_pb2.topicSubscribe(topic=topic,client_ip=client_ip)		

if __name__ == '__main__':
	thread.start_new_thread(serve,())

	json.dump([],open("subscribedTopics"+port,"w"))

	a = json.load(open("options","r"))
	CENTRAL_SERVER_IP = a["Central_server"]
	ACCESS_POINT = get_front_ip()

	while (True) :

		print "Type 1 for publish\nType 2 for subscribe\nType 3 for exit"
		response = raw_input()

		if response == "1" :
			print "Enter topic"
			topic = raw_input()

			print "Enter data"
			data = raw_input()

			push_topic(topic,data)

		elif response == "2" :
			channel = grpc.insecure_channel(CENTRAL_SERVER_IP)
			stub = pr_pb2_grpc.PublishTopicStub(channel)
			responses = stub.querryTopics(pr_pb2.empty())
			topicList = []
			for i,response in enumerate(responses) :
				print i,": "+response.topic
				topicList.append(response.topic)

			if len(topicList) > 0 :	
				print "Select topic from following choices :"
				selectedNumber = raw_input()
				try :
					if int(selectedNumber) < len(topicList) :
						subscribe_topic(topicList[int(selectedNumber)],self_ip)
					else :
						print "Invalid option selected ..."

				except :
					print "Invalid option selected ..."

			else :
				print "No topics found ..."

		elif response == "3" :
			lst = json.load(open("subscribedTopics"+port,"r"))
			channel = grpc.insecure_channel(ACCESS_POINT)
			stub = pr_pb2_grpc.PublishTopicStub(channel)
			response = stub.unsubscribeRequest(generateTopics(lst,str(SELF_IP)+":"+port))
			json.dump([],open("subscribedTopics"+port,"w"))
			print "exiting now..."
			exit()