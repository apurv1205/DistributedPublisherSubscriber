from concurrent import futures
import time
import grpc
import pr_pb2
import pr_pb2_grpc
import thread
import sys
import csv
from multiprocessing.dummy import Pool as ThreadPool 
import json

if len(sys.argv) < 2 : 
    print "ERROR : Enter the port for access point server...exiting"
    exit()
port = sys.argv[1]

_ONE_DAY_IN_SECONDS = 60 * 60 * 24

CENTRAL_SERVER_IP = ""

def register_ip():
    channel = grpc.insecure_channel(CENTRAL_SERVER_IP)
    stub = pr_pb2_grpc.PublishTopicStub(channel)
    response = stub.registerIp(pr_pb2.ips(ip = "localhost:"+str(port)))
    print(response.ack)

def publishData(lst):
    request = lst[0]
    accesspoint = lst[1]
    channel = grpc.insecure_channel(accesspoint)
    stub = pr_pb2_grpc.PublishTopicStub(channel)
    response = stub.publish(pr_pb2.topicData(topic=request.topic, data=request.data))
    return response.ack

def find_ips(topic):
    channel = grpc.insecure_channel(CENTRAL_SERVER_IP)
    stub = pr_pb2_grpc.PublishTopicStub(channel)
    responses = stub.giveIps(pr_pb2.topic(topic=topic))
    lst =[]
    for response in responses :
        print("IP received: " + response.ip)
        lst.append(response.ip)
    return lst

class AccessPoint(pr_pb2_grpc.PublishTopicServicer):

    def publishRequest(self, request, context):
        print "hit"
        returned_ips = find_ips(request.topic)
        lst = []
        pool = ThreadPool(len(returned_ips)) 
        for returned_ip in returned_ips :
            lst.append([request,returned_ip])
        results = pool.map(publishData, lst)
        print results
        return pr_pb2.Acknowledge(ack="Published in "+str(len(results))+" topic servers")

    def subscribeRequest(self, request, context):
        print "Subscribe request from client",request.client_ip," for topic",request.topic
        return pr_pb2.Acknowledge(ack="temporary acknowledge")

    def publish(self, request, context):
        print "Data received...",request.topic, request.data
        with open("topicDb"+port+".json","a") as outfile :
            json.dump([request.topic,request.data],outfile)
        return pr_pb2.Acknowledge(ack="Data write in "+str(port)+" topic server complete...")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pr_pb2_grpc.add_PublishTopicServicer_to_server(AccessPoint(), server)
    server.add_insecure_port('[::]:'+port)
    server.start()
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    a = json.load(open("options","r"))
    CENTRAL_SERVER_IP = a["Central_server"]
    register_ip()

    print "here"
    serve()