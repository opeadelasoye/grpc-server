import grpc
import boto3
from concurrent import futures
import requests
import re

import computeandstorage_pb2
import computeandstorage_pb2_grpc

class EC2Operations(computeandstorage_pb2_grpc.EC2OperationsServicer):
    def StoreData(self, request, context):
        data = request.data

        try:
            session = get_session()
            client = session.client('s3')
            client.put_object(Body=data, Bucket='bucket-name', Key='key-name')

            return computeandstorage_pb2.StoreReply(s3uri="public-s3uri")

        except Exception as e:
            print("Exception :\n", str(e)) 
        
            return 400


    def AppendData(self, request, context):
        data = request.data

        session = get_session()
        client = session.client('s3')
        file = client.get_object(Bucket='bucket-name', Key='key-name')
        fileContent = file['Body'].read().decode('utf-8')

        fileContent = fileContent + data

        client.put_object(Body=fileContent, Bucket='bucket-name', Key='key-name')

        return computeandstorage_pb2.AppendReply()
    
    def DeleteFile(self, request, context):
        s3uri = request.s3uri
        uricontent = re.findall(r'/(\w+)', s3uri)
        keycontent = re.findall(r"com/(.*)", s3uri)

        bucket = uricontent[0]
        key = keycontent[0]

        session = get_session()
        client = session.client('s3')

        client.delete_object(Bucket=bucket, Key=key)

        return computeandstorage_pb2.DeleteReply()

init_json = {
    "banner": "B00842075",
    "ip": "ip:port-num"
}

port = 'port-num'
server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
computeandstorage_pb2_grpc.add_EC2OperationsServicer_to_server(EC2Operations(), server)
server.add_insecure_port("0.0.0.0:port-num")

def get_session():
    return boto3.Session(
        aws_access_key_id="access-key-id",
        aws_secret_access_key="secret-access-key",
        aws_session_token="session-token"
    )

def serve():
    server.start()
    print("Server started, running on port " + port + "\n")

    response = requests.post("http://54.173.209.76:9000/start", json=init_json)
    print("Sent JSON data to http://54.173.209.76:9000/start\n")

    # Check the response
    if response.status_code == 200:
        print('POST request was successful.')
        print(response.text)
    else:
        print(f'POST request failed with status code: {response.status_code}')
        print(response.text)

    server.wait_for_termination()

if __name__ == '__main__':
    serve()