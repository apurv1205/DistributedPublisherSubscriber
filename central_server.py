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
from pymongo import MongoClient

if len(sys.argv) < 2 : 
    print "ERROR : Enter the port for access point server...exiting"
    exit()
port = sys.argv[1]

_ONE_DAY_IN_SECONDS = 60 * 60 * 24
SELF_IP=[l for l in ([ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1], [[(s.connect(('8.8.8.8', 53)), s.getsockname()[0], s.close()) for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]) if l][0][0]

class CentralServer(pr_pb2_grpc.PublishTopicServicer):

    def unsubscribeRequestCentral(self, request, context):
        twoLevelDict.delete_one({"topic":request.topic,"subscriber":request.client_ip})
        return pr_pb2.acknowledge(ack="temporary acknowledge from central server")

    def deReplicaRequest(self, request, context):
        dct = {}
        cursor = twoLevelDict.find({"topic":request.topic})
        for document in cursor:
            if dct.has_key(document["publisher"]):
                pass
            else : dct[document["publisher"]] = []
            if document["subscriber"]!="NULL" :
                dct[document["publisher"]].append(document["subscriber"])
        if len(dct.keys()) == 1:
            return pr_pb2.acknowledge(ack="ERROR")
        extraSubscribers = dct[request.client_ip]
        twoLevelDict.delete_many({"topic":request.topic,"publisher":request.client_ip})
        del dct[request.client_ip]
        for subscriber in extraSubscribers :
            allotedServer = ""
            l = sys.maxsize
            tempIp = ""
            for ip in dct.keys():
                tempCursor = twoLevelDict.find({"topic":request.topic,"publisher":ip})
                if tempCursor.count() < l:
                    l = tempCursor.count()
                    tempIp = ip

            twoLevelDict.insert_one({"topic":request.topic,"publisher":tempIp,"subscriber":subscriber})
        return pr_pb2.acknowledge(ack="DONE") 

    def querryTopics(self, request, context):
        cursor = twoLevelDict.find({"subscriber":"NULL"})
        for document in cursor :
            yield pr_pb2.topic(topic=document["topic"])

    def replicaRequest(self, request, context):
        document = twoLevelDict.find_one({"topic":request.topic,"subscriber":"NULL"})
        allotedServer = document["publisher"]
        if twoLevelDict.find({"topic":request.topic,"publisher":request.client_ip}).count() > 0 :
            return pr_pb2.acknowledge(ack="Requesting front end server already a replica for "+request.topic)
        if twoLevelDict.find({"topic":request.topic,"subscriber":request.client_ip}).count() > 0 :
            twoLevelDict.delete_one({"topic":request.topic,"subscriber":request.client_ip})

        twoLevelDict.insert_one({"topic":request.topic,"publisher":request.client_ip,"subscriber":"NULL"})
        twoLevelDict.insert_one({"topic":request.topic,"publisher":request.client_ip,"subscriber":request.client_ip})
        channel = grpc.insecure_channel(allotedServer)
        stub = pr_pb2_grpc.PublishTopicStub(channel)
        response = stub.sendBackupRequestReplica(pr_pb2.topicSubscribe(topic=request.topic, client_ip=request.client_ip))
        print "done modifying the dct"
        return pr_pb2.acknowledge(ack="Requesting front end server "+request.client_ip+" made a replica of topic(backup sent) "+request.topic)

    def subscribeRequestCentral(self, request, context):
        print "Subscribe request from access point",request.client_ip," for topic",request.topic," of type :",request.type
        allotedServer = ""
        if request.type == "new" :
            if twoLevelDict.find({"topic":request.topic,"publisher":request.client_ip}).count() > 0:
                allotedServer = request.client_ip 
            else :
                l = sys.maxsize
                tempIp = ""
                cursor = twoLevelDict.find({"subscriber":"NULL"})
                for document in cursor :
                    ip = document["publisher"]
                    if twoLevelDict.find({"topic":request.topic,"publisher":ip}).count() < l :
                        l=twoLevelDict.find({"topic":request.topic,"publisher":ip}).count()
                        tempIp = ip
                allotedServer = tempIp
            twoLevelDict.insert_one({"topic":request.topic,"publisher":allotedServer,"subscriber":request.client_ip})

        else :
            document = twoLevelDict.find_one({"topic":request.topic,"subscriber":request.client_ip})
            allotedServer = document["publisher"]

        channel = grpc.insecure_channel(allotedServer)
        stub = pr_pb2_grpc.PublishTopicStub(channel)
        response = stub.sendBackupRequest(pr_pb2.topicSubscribe(topic=request.topic, client_ip=request.client_ip))
        print response.ack
        return pr_pb2.acknowledge(ack="temporary acknowledge from central server")

    def giveSubscriberIps(self, request, context):
        if twoLevelDict.find({"topic":request.topic,"publisher":request.client_ip}).count() == 1: 
            yield pr_pb2.ips(ip="none")
        else:
            cursor = twoLevelDict.find({"topic":request.topic,"publisher":request.client_ip})
            for document in cursor:
                if document["subscriber"]!="NULL" :
                    yield pr_pb2.ips(ip=document["subscriber"])

    def giveIps(self, request, context):
        cursor = twoLevelDict.find({"topic":request.topic,"subscriber":"NULL"})
        if cursor.count() > 0:
            for document in cursor :
                yield pr_pb2.ips(ip=document["publisher"])

        else : 
            cursor = frontends.find({"type":"ip"})
            lst = []
            for document in cursor :
                lst.append(document["ip"])
            ip = random.choice(lst)
            twoLevelDict.insert_one({"topic":request.topic,"publisher":ip,"subscriber":"NULL"})
            yield pr_pb2.ips(ip=ip)

    def getFrontIp(self, request, context) :
        cursor = frontends.find({"type":"index"})
        if cursor.count() == 0:
            return pr_pb2.ips(ip="NONE")
        index = 0
        for document in cursor :
            index = document["index"]
        ipList = []
        cursor = frontends.find({"type":"ip"})
        for document in cursor :
            ipList.append(document["ip"])
        m = ipList[index]
        if index == len(ipList) - 1 :
            frontends.replace_one({"type":"index","index":index},{"type":"index","index":0})
        else :
            frontends.replace_one({"type":"index","index":index},{"type":"index","index":index+1})
        return pr_pb2.ips(ip=m)

    def registerIp(self, request, context) :
        cursor = frontends.find({"type":"index"})
        if cursor.count() == 0:
            frontends.insert_one({"type":"index","index":0})
        frontends.insert_one({"type":"ip","ip":request.ip})
        return pr_pb2.acknowledge(ack="Ip added...")


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pr_pb2_grpc.add_PublishTopicServicer_to_server(CentralServer(), server)
    server.add_insecure_port(str(SELF_IP)+":"+port)
    server.start()
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    selfIpDct = {}
    selfIpDct["centralServer"] = str(SELF_IP)+":"+port
    json.dump(selfIpDct,open("options","w"))

    mongoClient = MongoClient("localhost", 27017)
    mongoClient.drop_database('CentralServer'+port)
    db = mongoClient['CentralServer'+port]
    frontends = db["frontends"]
    twoLevelDict = db["twoLevelDict"]
    oneLevelDict = db["oneLevelDict"]
    serve()