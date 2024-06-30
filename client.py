import concurrent.futures
import socket
import threading
import json
import uuid
import struct
import re
import time
from functions import get_ip_adress
from functions import get_broadcast_ip

# Constants
BUFFER_SIZE = 1024
MULTICAST_BUFFER_SIZE = 10240
IP_ADDRESS = get_ip_adress()
print(IP_ADDRESS)

BROADCAST_IP = get_broadcast_ip()
print(BROADCAST_IP)
BROADCAST_PORT_SERVER = 50000 # port for dynamic discovery server

TCP_SERVER_PORT = 50510
TCP_TIMEOUT = 5

MULTICAST_CLIENT_PORT = 50550  # port for incoming messages
MULTICAST_GROUP_ADDRESS = '224.1.2.1'

class Client:
    def __init__(self):
        self.client_uuid = None
        self.shutdown_event = threading.Event()
        self.threads = []
        self.last_response_from_server = ''
        self.select_client_uuid = ''

    # get client name and start threads
    def start_client(self):
        # get uuid 
        self.client_uuid = uuid.uuid4()
        print(f'Your client uuid is {self.client_uuid}')

        while (True):
            server_address = self.find_server()
            if server_address:
                break
            if not server_address:
                print(f'Unable to connect to server. Try again')

        # start threads
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            self.threads.append(executor.submit(self.cli))
            self.threads.append(executor.submit(self.handle_chat_messages))
            print('Client started')

            try:
                # Keep the main thread alive while the threads are running
                while not self.shutdown_event.is_set():
                    self.shutdown_event.wait(1)
            except KeyboardInterrupt:
                print("Client shutdown initiated")
                self.shutdown_event.set()
                for thread in self.threads:
                    thread.cancel()
                executor.shutdown(wait=True)

    def handle_chat_messages(self):
        print('Open socket for incoming messages')
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as multicast_socket:
            multicast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            multicast_socket.bind(('', MULTICAST_CLIENT_PORT))
            mreq = struct.pack('4sL', socket.inet_aton(MULTICAST_GROUP_ADDRESS), socket.INADDR_ANY)
            multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

            while not self.shutdown_event.is_set():
                try:
                    data, addr = multicast_socket.recvfrom(BUFFER_SIZE)
                    print(f'{data.decode("utf-8")}')
                except socket.timeout as e:
                    continue
                except socket.error as e:
                    print(f'Socket error: {e}')
                except Exception as e:
                    print(f'Unexpected error: {e}')

    #  send out broadcast message to detect currently leading server
    def find_server(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as broadcast_server_discovery_socket:
                broadcast_server_discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                json_message = json.dumps({"client_uuid": str(self.client_uuid), "client_ip": str(IP_ADDRESS)})
                broadcast_server_discovery_socket.sendto(json_message.encode('utf-8'), (BROADCAST_IP, BROADCAST_PORT_SERVER))
                print('Broadcast message for server discovery sent')

                broadcast_server_discovery_socket.settimeout(3)
                while True:
                    try:
                        response, server_ip = broadcast_server_discovery_socket.recvfrom(BUFFER_SIZE)
                        print(f'Received server answer from lead server {server_ip[0]}')
                        return server_ip[0]
                    except socket.timeout:
                        break
        except Exception as e:
            print(f'Unexpected error in find_server: {e}')

    # send tcp message to server
    def send_message_to_server(self, json_message):
        server_address = ''
        retry = 3
        while retry > 0 and not self.shutdown_event.is_set():
            server_address = self.find_server()
            if server_address:
                break
            retry -= 1
            if retry == 0 and not server_address:
                print(f'Unable to connect to server. Please try again')
                return

        print(f'Proceeding to send {json_message} to server {server_address}')
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
                client_socket.settimeout(TCP_TIMEOUT)
                try:
                    client_socket.connect((server_address, TCP_SERVER_PORT))
                    client_socket.sendall(json_message.encode('utf-8'))

                    response = client_socket.recv(BUFFER_SIZE)
                    self.last_response_from_server = response.decode('utf-8')
                    print(response.decode('utf-8'))
                except socket.error as e:
                    print(f'Socket error: {e}')
        except Exception as e:
            print(f'Error in send_message_to_server: {e}')

    def get_clients(self):
        json_message = json.dumps({"function": "get_clients", "client_uuid": str(self.client_uuid), "client_ip": str(IP_ADDRESS)})
        self.send_message_to_server(json_message)

    def select_client(self):
        if self.last_response_from_server != 'No other clients connected':
            client_id = input("Type in the client you want to chat with: ")

            if client_id != '':
                clients = self.last_response_from_server

                try:
                    result = re.search(f'{client_id} - (.*)\n', clients)
                    self.select_client_uuid = result.group(1)
                except:
                    self.select_client_uuid = ''

                json_message = json.dumps({"function": "select_client", "client_uuid": str(self.client_uuid), "client_ip": str(IP_ADDRESS), "select_client_uuid": str(self.select_client_uuid)})
                self.send_message_to_server(json_message)

    def send_message_to_client(self):
        message = input("Type your message: ")
        json_message = json.dumps({"function": "send_message", "client_uuid": str(self.client_uuid), "client_ip": str(IP_ADDRESS), "msg": str(message)})
        self.send_message_to_server(json_message)

    # user interface
    def cli(self):
        time.sleep(1)
        while not self.shutdown_event.is_set():
            print("Please select your next action:")
            print("1 - Select chat client")
            print("2 - Send message to chat client")

            user_input = input()
            if user_input == "1":
                self.get_clients()
                self.select_client()
            elif user_input == "2":
                self.send_message_to_client()