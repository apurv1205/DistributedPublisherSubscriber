from concurrent import futures
import time
import json
import grpc
import pr_pb2
import pr_pb2_grpc
import thread

_ONE_DAY_IN_SECONDS = 60 * 60 * 24

dct = {}
dct['a'] = {}
dct['a']["localhost:50053"] = []
dct['a']["localhost:50054"] = []

class CentralServer(pr_pb2_grpc.PublishTopicServicer):
    def subscribeRequestCentral(self, request, context):
        print "Subscribe request from access point",request.client_ip," for topic",request.topic," of type :",request.type
        allotedServer = "localhost:50053"
        dct[request.topic][allotedServer].append(request.client_ip)
        if request.type == "new" :
            channel = grpc.insecure_channel(allotedServer)
            stub = pr_pb2_grpc.PublishTopicStub(channel)
            response = stub.sendBackupRequest(pr_pb2.topicSubscribe(topic=request.topic, client_ip=request.client_ip))
            print response.ack
        return pr_pb2.acknowledge(ack="temporary acknowledge from central server")

    def giveSubscriberIps(self, request, context):
        ipList = dct[request.topic][request.client_ip]
        if len(ipList) == 0 : 
            yield pr_pb2.ips(ip="none")
        for ip in ipList:
            yield pr_pb2.ips(ip=ip)

    def giveIps(self, request, context):
        print request.topic
        ipDct = dct[request.topic]
        for ip in ipDct.keys():
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
    serve()