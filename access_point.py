from concurrent import futures
import time

import grpc

import pr_pb2
import pr_pb2_grpc

_ONE_DAY_IN_SECONDS = 60 * 60 * 24


class Greeter(pr_pb2_grpc.PublishTopicServicer):

    def publish(self, request, context):
        print request.topic, request.data
        return pr_pb2.Acknowledge(ack="Acknowledgeeddddd")


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pr_pb2_grpc.add_PublishTopicServicer_to_server(Greeter(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    serve()
