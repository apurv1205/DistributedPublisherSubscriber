from concurrent import futures
import time
import json
import grpc
import pr_pb2
import pr_pb2_grpc
import thread
import random
import sys
import socket

_ONE_DAY_IN_SECONDS = 60 * 60 * 24
SELF_IP=[l for l in ([ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1], [[(s.connect(('8.8.8.8', 53)), s.getsockname()[0], s.close()) for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]) if l][0][0]

class CentralServer(pr_pb2_grpc.PublishTopicServicer):

    def unsubscribeRequestCentral(self, request, context):
        dct = json.load(open("topic_servers_dict","r"))
        dctIp = dct[request.topic]
        for key, value in dctIp.items():
            if request.client_ip in value :
                dct[request.topic][key].remove(request.client_ip)
        json.dump(dct,open("topic_servers_dict","w"))
        return pr_pb2.acknowledge(ack="temporary acknowledge from central server")

    def deReplicaRequest(self, request, context):
        dct = json.load(open("topic_servers_dict","r"))
        dctIp = dct[request.topic]
        extraSubscribers = []
        if len(dctIp.keys()) == 1 :
            return pr_pb2.acknowledge(ack="ERROR") 
        extraSubscribers = dct[request.topic][request.client_ip]
        del dct[request.topic][request.client_ip]
        dctIp = dct[request.topic]
        for subscriber in extraSubscribers :
            allotedServer = ""
            l = sys.maxsize
            tempIp = ""
            for ip in dctIp.keys() :
                if len(dctIp[ip]) < l :
                    l=len(dctIp[ip])
                    tempIp = ip
            allotedServer = tempIp
            dct[request.topic][allotedServer].append(subscriber)
        json.dump(dct,open("topic_servers_dict","w"))
        return pr_pb2.acknowledge(ack="DONE") 

    def querryTopics(self, request, context):
        dct = json.load(open("topic_servers_dict","r"))
        for topic in dct.keys() :
            yield pr_pb2.topic(topic=topic)

    def replicaRequest(self, request, context):
        dct = json.load(open("topic_servers_dict","r"))
        dctIp = dct[request.topic]
        for key, value in dctIp.items() :
            allotedServer = key
            if key == request.client_ip :
                return pr_pb2.acknowledge(ack="Requesting front end server already a replica for "+request.topic)
            if request.client_ip in value :
                print "done modifying the dct"
                value.remove(request.client_ip)
        dct[request.topic][request.client_ip] = []
        dct[request.topic][request.client_ip].append(request.client_ip)
        print dct[request.topic]
        json.dump(dct,open("topic_servers_dict","w"))
        channel = grpc.insecure_channel(allotedServer)
        stub = pr_pb2_grpc.PublishTopicStub(channel)
        response = stub.sendBackupRequestReplica(pr_pb2.topicSubscribe(topic=request.topic, client_ip=request.client_ip))
        print response.ack
        return pr_pb2.acknowledge(ack="Requesting front end server "+request.client_ip+" made a replica of topic(backup sent) "+request.topic)

    def subscribeRequestCentral(self, request, context):
        dct = json.load(open("topic_servers_dict","r"))
        print "Subscribe request from access point",request.client_ip," for topic",request.topic," of type :",request.type
        allotedServer = ""
        if request.type == "new" :
            ipDct = dct[request.topic]
            if ipDct.has_key(request.client_ip) :
                allotedServer = request.client_ip 
            else :
                l = sys.maxsize
                tempIp = ""
                for ip in ipDct.keys() :
                    if len(ipDct[ip]) < l :
                        l=len(ipDct[ip])
                        tempIp = ip
                allotedServer = tempIp
            dct[request.topic][allotedServer].append(request.client_ip)
            json.dump(dct,open("topic_servers_dict","w"))

        else :
            dctIp = dct[request.topic]
            for key, value in dctIp.items() :
                if request.client_ip in value :
                    allotedServer=key

        channel = grpc.insecure_channel(allotedServer)
        stub = pr_pb2_grpc.PublishTopicStub(channel)
        response = stub.sendBackupRequest(pr_pb2.topicSubscribe(topic=request.topic, client_ip=request.client_ip))
        print response.ack
        return pr_pb2.acknowledge(ack="temporary acknowledge from central server")

    def giveSubscriberIps(self, request, context):
        dct = json.load(open("topic_servers_dict","r"))
        ipList = dct[request.topic][request.client_ip]
        if len(ipList) == 0 : 
            yield pr_pb2.ips(ip="none")
        for ip in ipList:
            yield pr_pb2.ips(ip=ip)

    def giveIps(self, request, context):
        print request.topic
        dct = json.load(open("topic_servers_dict","r"))
        if dct.has_key(request.topic) :
            ipDct = dct[request.topic]
            for ip in ipDct.keys():
                yield pr_pb2.ips(ip=ip)

        else : 
            dct[request.topic] = {}
            dct_access_point = json.load(open("list_front_end","r"))
            ip = random.choice(dct_access_point["ip"])
            dct[request.topic][ip] = []
            json.dump(dct,(open("topic_servers_dict","w")))
            yield pr_pb2.ips(ip=ip)

    def getFrontIp(self, request, context) :
        a = json.load(open("list_front_end","r"))
        i = a["index"]
        m = a["ip"][i]
        if a["index"] == len(a["ip"])-1 :
            a["index"] = 0
        else :
            a["index"] += 1

        json.dump(a,open("list_front_end","w"))
        return pr_pb2.ips(ip=m)

    def registerIp(self, request, context) :
        a = json.load(open("list_front_end","r"))
        if (len(a)==0) : 
            a = {}
            a["index"] = 0
            a["ip"] = []

        i = a["index"]
        a["ip"].append(request.ip)

        json.dump(a,open("list_front_end","w"))
        return pr_pb2.acknowledge(ack="Ip added...")


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pr_pb2_grpc.add_PublishTopicServicer_to_server(CentralServer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    json.dump({},open("list_front_end","w"))
    json.dump({},open("topic_servers_dict","w"))
    serve()