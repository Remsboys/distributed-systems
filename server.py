import concurrent.futures
import socket
import threading
import json
import time
import uuid
import struct
from functions import get_ip_adress
from functions import get_broadcast_ip

# constants
BUFFER_SIZE = 1024
MULTICAST_BUFFER_SIZE = 10240
IP_ADDRESS = get_ip_adress()
print(IP_ADDRESS)

BROADCAST_ADDRESS = get_broadcast_ip()
print(BROADCAST_ADDRESS)
BROADCAST_PORT_CLIENT = 50000  # port to open to receive server discovery requests from client
BROADCAST_PORT_SERVER = 60000  # port to open to receive and send server discovery requests

TCP_CLIENT_PORT = 50510  # port for incoming messages from clients

MULTICAST_PORT_CLIENT = 50550  # port for outgoing chat messages
MULTICAST_GROUP_ADDRESS = '224.1.2.2'
MULTICAST_TTL = 2

LCR_PORT = 60600
LEADER_DEATH_TIME = 20

HEARTBEAT_PORT_SERVER = 60570  # port for incoming / outgoing heartbeat (leader)

class Server:
    def __init__(self):
        self.shutdown_event = threading.Event()
        self.threads = []
        self.server_uuid = None
        self.list_of_known_servers = []
        self.client_list = []  # List with clients
        self.lcr_ongoing = False
        self.is_leader = False
        self.last_message_from_leader_ts = time.time()
        self.direct_neighbour = ''
        self.ip_direct_neighbour = ''
        self.leader = ''
        self.ip_leader = ''
        self.lock = threading.Lock()
        self.participant = False

    def start_server(self):
        # get uuid
        self.server_uuid = str(uuid.uuid4())

        # start threads
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            methods = [
                self.handle_broadcast_server_requests,
                self.lcr,
                self.handle_leader_heartbeat,
                self.detection_of_missing_or_dead_leader,
                self.handle_broadcast_client_requests,
                self.handle_send_message_request
            ]

            for method in methods:
                self.threads.append(executor.submit(self.run_with_exception_handling, method))

            print('Server started')

            try:
                # Keep the main thread alive while the threads are running
                while not self.shutdown_event.is_set():
                    self.shutdown_event.wait(1)
            except KeyboardInterrupt:
                print("Server shutdown initiated.")
                self.shutdown_event.set()
                for thread in self.threads:
                    thread.cancel()
            finally:
                executor.shutdown(wait=True)

    def run_with_exception_handling(self, target):
        try:
            target()
        except Exception as e:
            print(f"Error in thread {target.__name__}: {e}")

# Client communication
    def handle_broadcast_client_requests(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as listener_socket:
                listener_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                listener_socket.bind(('', BROADCAST_PORT_CLIENT))
                listener_socket.settimeout(1)

                while not self.shutdown_event.is_set():
                    if self.is_leader:
                        try:
                            data, client_ip = listener_socket.recvfrom(BUFFER_SIZE)
                            

                            json_message = json.loads(data)
                            print(f"Server discovery request by {json_message['client_uuid']}")

                            current_client = {}
                            current_client['client_uuid'] = json_message['client_uuid']
                            current_client['client_ip'] = json_message['client_ip']
                            current_client['select_client_uuid'] = ''

                            new_client = True
                            for client in self.client_list:
                                if client['client_uuid'] == current_client['client_uuid']:
                                    new_client = False

                            if new_client:
                                self.client_list.append(current_client)

                            response_message = f'hello from server {self.server_uuid}'.encode()
                            listener_socket.sendto(response_message, client_ip)
                            print("Sent server hello to client: " + current_client["client_uuid"])
                        except socket.timeout:
                            continue
                        except Exception as e:
                            print(f"Error handling broadcast client request: {e}")
        except Exception as e:
            print(f"Failed to open Socket for handling client Broadcast requests: {e}")

    def handle_send_message_request(self):
        while not self.shutdown_event.is_set():
            if self.is_leader:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    server_socket.bind((IP_ADDRESS, TCP_CLIENT_PORT))
                    server_socket.listen()

                    client_socket, addr = server_socket.accept()
                    with client_socket:
                        try:
                            data = client_socket.recv(BUFFER_SIZE)
                            client_response_msg = ''
                            if data:
                                json_data = json.loads(data.decode('UTF-8'))
                                print(f"Received message {json_data}")
                                if json_data['function'] == 'get_clients':
                                    client_response_msg = self.get_clients(json_data['client_uuid'])
                                elif json_data['function'] == 'select_client':
                                    if json_data['select_client_uuid'] != '':
                                        client_response_msg = self.select_client(json_data['client_uuid'], json_data['select_client_uuid'])
                                    else:
                                        client_response_msg = 'The selected client is not available'
                                elif json_data['function'] == 'send_message':
                                    if json_data['msg']:
                                        client_response_msg = self.send_message(json_data['client_uuid'], json_data['msg'])
                                    else:
                                        client_response_msg = "No message received to submit"
                                else:
                                    client_response_msg = "Received invalid data object"
                                client_socket.sendall(client_response_msg.encode('UTF-8', errors='replace'))
                        finally:
                            client_socket.close()

    def get_clients(self, client_uuid):
        clients = ''
        counter = 1
        for client in self.client_list:
            if client['client_uuid'] != client_uuid:
                client_str = f"{counter} - {client['client_uuid']}\n"
                clients = clients + client_str
                counter += 1
 
        if clients == '':
            return "No other clients connected"
        else:
            return clients
        
    def select_client(self, client_uuid, select_client_uuid):
        for client in self.client_list:
            if client['client_uuid'] == client_uuid:
                client['select_client_uuid'] = select_client_uuid
                break

        return "Client selected"

    def send_message(self, client_uuid, message):
        is_client_selected = False
        select_client_uuid = ''
        for client in self.client_list:
            if client['client_uuid'] == client_uuid:
                if client['select_client_uuid'] != '':
                    is_client_selected = True
                    select_client_uuid = client['select_client_uuid']
                    break

        if is_client_selected:
            self.forward_message_select_client(select_client_uuid, message, client_uuid)
            return f'Message to {select_client_uuid} sent'

        return "First select a client to chat with"

    def forward_message_select_client(self, receiver, msg, sender):
        client_multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        client_multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, MULTICAST_TTL)
        send_message = f'{sender}: {msg}'.encode('UTF-8')

        try:
            for client in self.client_list:
                if client['client_uuid'] == receiver:
                    client_multicast_socket.sendto(send_message, (client['client_ip'], MULTICAST_PORT_CLIENT))
        except Exception as e:
            print(f"Error sending message to chat participants: {e}")
        finally:
            client_multicast_socket.close()

 #  Server connection
    def send_broadcast_to_search_for_servers(self):
        print('Sending server discovery message via broadcast')
        with self.lock:
            server = {}
            server['server_uuid'] = self.server_uuid
            server['server_ip'] = IP_ADDRESS
            self.list_of_known_servers.clear()
            self.list_of_known_servers.append(server)

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as broadcast_server_discovery_socket:
                broadcast_server_discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                json_message = json.dumps({"server_uuid": str(self.server_uuid), "server_ip": str(IP_ADDRESS)})
                broadcast_server_discovery_socket.sendto(json_message.encode('utf-8'), (BROADCAST_ADDRESS, BROADCAST_PORT_SERVER))

                broadcast_server_discovery_socket.settimeout(3)
                while True:
                    try:
                        response, addr = broadcast_server_discovery_socket.recvfrom(BUFFER_SIZE)
                        response_message = json.loads(response)
                        print('Received server discovery answer from ' + response_message['server_uuid'])

                        check_server_not_in_list = True
                        for known_server in self.list_of_known_servers:
                            if response_message['server_uuid'] == known_server['server_uuid']:
                                check_server_not_in_list = False

                        if check_server_not_in_list:
                            server = {}
                            server['server_uuid'] = response_message['server_uuid']
                            server['server_ip'] = response_message['server_ip']
                            self.list_of_known_servers.append(server)

                    except socket.timeout:
                        print('No more responses, ending wait')
                        break
                    except Exception as e:
                        print(f'Error receiving response: {e}')
                        break
        except Exception as e:
            print(f'Failed to send broadcast message: {e}')

    def handle_broadcast_server_requests(self):
        print('Starting to listen for broadcast server requests')

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as listener_socket:
                listener_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                listener_socket.bind(('', BROADCAST_PORT_SERVER))
                listener_socket.settimeout(1)

                while not self.shutdown_event.is_set():
                    try:
                        msg, addr = listener_socket.recvfrom(BUFFER_SIZE)
                        json_message = json.loads(msg)
                        print('Received server discovery message via broadcast')

                        check_server_not_in_list = True
                        for known_server in self.list_of_known_servers:
                            if json_message['server_uuid'] == known_server['server_uuid']:
                                check_server_not_in_list = False

                        if check_server_not_in_list:
                            server = {}
                            server['server_uuid'] = json_message['server_uuid']
                            server['server_ip'] = json_message['server_ip']
                            self.list_of_known_servers.append(server)

                        response_message= json.dumps({"server_uuid": str(self.server_uuid), "server_ip": str(IP_ADDRESS)}).encode() 
                        listener_socket.sendto(response_message, addr)
                        print('Sent server hello to ' + json_message['server_uuid'])

                    except socket.timeout:
                        continue
                    except socket.error as e:
                        print(f'Socket error: {e}')
                    except Exception as e:
                        print(f'Unexpected error: {e}')

        except socket.error as e:
            print(f'Failed to set up listener socket: {e}')

# leader election
    def detection_of_missing_or_dead_leader(self):
        print('Starting detection of missing or dead leader')
        while not self.shutdown_event.is_set():
            time.sleep(3)
            if not self.is_leader and not self.lcr_ongoing:
                if (time.time() - self.last_message_from_leader_ts) >= LEADER_DEATH_TIME:
                    print('No active leader detected')
                    self.start_lcr()

    def form_ring(self):
        print('Forming ring with list of known servers')
        try:
            uuid_list = []
            for server in self.list_of_known_servers:
                uuid_list.append(uuid.UUID(server['server_uuid']))

            uuid_list.sort()

            uuid_ring = []
            for server in uuid_list:
                uuid_ring.append(str(server))
            
            print(f'Ring formed: {uuid_ring}')
            return uuid_ring
        except socket.error as e:
            print(f'Failed to form ring: {e}')
            return []
        
    def get_direct_neighbour(self):
        print('Preparing to get direct neighbour')
        self.direct_neighbour = ''
        try:
            ring = self.form_ring()

            if self.server_uuid in ring:
                index = ring.index(self.server_uuid)
                direct_neighbour = ring[(index + 1) % len(ring)]
                if direct_neighbour and direct_neighbour != self.server_uuid:
                    self.direct_neighbour = direct_neighbour

                    for server in self.list_of_known_servers:
                        if server['server_uuid'] == self.direct_neighbour:
                            self.ip_direct_neighbour = server['server_ip']

                    print(f'Direct neighbour: {self.direct_neighbour}')
            else:
                print(f'Ring is not complete!')
        except Exception as e:
            print(f'Failed to get direct neighbour: {e}')

    def start_lcr(self):
        print('Starting leader election')
        retry = 3
        while retry > 0:
            self.send_broadcast_to_search_for_servers()
            time.sleep(8)
            self.get_direct_neighbour()
            if not self.direct_neighbour == '':
                break
            retry -= 1

        if not self.direct_neighbour == '':
            lcr_start_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                election_message = {"mid": self.server_uuid, "isLeader": False}
                message = json.dumps(election_message).encode()
                lcr_start_socket.sendto(message, (self.ip_direct_neighbour, LCR_PORT))
                with self.lock:
                    self.lcr_ongoing = True
                    self.is_leader = False
                    self.participant = False
                print(f'LCR start message sent to {self.direct_neighbour}')
            except socket.error as e:
                print('Socket error occurred in start_lcr', e)
            finally:
                lcr_start_socket.close()
        else:
            print('Assuming to be the only active server - assigning as leader')
            with self.lock:
                self.is_leader = True
                self.participant = False
                self.lcr_ongoing = False

    def lcr(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as lcr_listener_socket:
            lcr_listener_socket.bind((IP_ADDRESS, LCR_PORT))
            while not self.shutdown_event.is_set():
                data, address = lcr_listener_socket.recvfrom(BUFFER_SIZE)
                with self.lock:
                    election_message = json.loads(data.decode())

                    if election_message['isLeader']:
                        leader_uuid = election_message['mid']
                        if leader_uuid != self.server_uuid:
                            print(f'{self.server_uuid}: Leader was elected! {election_message["mid"]}')
                            lcr_listener_socket.sendto(json.dumps(election_message).encode(),
                                                       (self.ip_direct_neighbour, LCR_PORT))
                            self.leader = leader_uuid

                            for server in self.list_of_known_servers:
                                if server['server_uuid'] == self.leader:
                                    self.ip_leader = server['server_ip']

                            self.is_leader = False
                        self.participant = False
                        self.lcr_ongoing = False

                    elif uuid.UUID(election_message['mid']) < uuid.UUID(self.server_uuid) and not self.participant:
                        new_election_message = {"mid": self.server_uuid, "isLeader": False}
                        self.participant = True
                        lcr_listener_socket.sendto(json.dumps(new_election_message).encode(),
                                                   (self.ip_direct_neighbour, LCR_PORT))

                    elif uuid.UUID(election_message['mid']) > uuid.UUID(self.server_uuid):
                        self.participant = False
                        lcr_listener_socket.sendto(json.dumps(election_message).encode(),
                                                   (self.ip_direct_neighbour, LCR_PORT))
                    elif election_message['mid'] == self.server_uuid:
                        new_election_message = {"mid": self.server_uuid, "isLeader": True}
                        lcr_listener_socket.sendto(json.dumps(new_election_message).encode(),
                                                   (self.ip_direct_neighbour, LCR_PORT))
                        self.leader = self.server_uuid
                        self.ip_leader = IP_ADDRESS
                        self.is_leader = True
                        self.participant = False
                        self.lcr_ongoing = False
                        print(f'Current node won leader election! {self.server_uuid} (sent message to {self.direct_neighbour} )')
                    else:
                        print(f'Unexpected event occurred in LCR {election_message}')

# Heartbeat and update client data
    def handle_leader_heartbeat(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as heartbeat_server_socket:
            heartbeat_server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            heartbeat_server_socket.bind(('', HEARTBEAT_PORT_SERVER))

            group = socket.inet_aton(MULTICAST_GROUP_ADDRESS)
            mreq = struct.pack('4sL', group, socket.INADDR_ANY)
            heartbeat_server_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            heartbeat_server_socket.settimeout(2)

            try:
                while not self.shutdown_event.is_set():
                    if self.is_leader:
                        self.send_leader_heartbeat()
                        continue
                    try:
                        data, addr = heartbeat_server_socket.recvfrom(MULTICAST_BUFFER_SIZE)
                        data = json.loads(data.decode())
                        self.client_list = data['client_data']
                        print('Received heartbeat from leader server and updated client data send by leader server')

                        if not self.ip_leader:
                            self.leader = data['server_uuid']
                            self.ip_leader = addr[0]
                        with self.lock:
                            self.last_message_from_leader_ts = time.time()
                    except socket.timeout:
                        continue
                    except socket.error as e:
                        print('Socket error occurred while receiving heartbeat: {e}')
                    except Exception as e:
                        print(f'Unexpected error occurred: {e}')
            finally:
                print('Shutting down heartbeat listener')

    def send_leader_heartbeat(self):
        heartbeat_client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        heartbeat_client_socket.settimeout(1)
        try:
            print('Sending heartbeat and client data')
            payload = {}
            payload['server_uuid'] = self.server_uuid
            payload['client_data'] = self.client_list
            print(payload)
            message = json.dumps(payload).encode()
            heartbeat_client_socket.sendto(message, (MULTICAST_GROUP_ADDRESS, HEARTBEAT_PORT_SERVER))

            time.sleep(2)
        except socket.error as e:
            print(f"Socket error: {e}")
        except Exception as e:
            print(f"Error: {e}")
        finally:
            heartbeat_client_socket.close()