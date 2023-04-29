import os
import json
import time
import socket
import threading
import math

class Chunk_Announcer:
    def __init__(self):
        self.file_path = input("Please enter the path of the file to host: ")
        self.chunk_size = math.ceil(math.ceil(os.path.getsize(self.file_path)) / 5)  # Specify the size of each chunk in bytes (change as needed)
        self.num_chunks = 5  # Specify the number of chunks per file (change as needed)
        self.chunks_dir = os.path.join(os.path.dirname(self.file_path), "chunks")
        self.chunk_names = self.divide_file_into_chunks()
        self.broadcast_address = "255.255.255.255"  # Specify the broadcast IP address to use (change as needed)
        self.broadcast_port = 5001  # Specify the port number to use for broadcasting (change as needed)
        threading.Thread(target=self.broadcast_chunks).start()
    
    def divide_file_into_chunks(self):
        with open(self.file_path, "rb") as f:
            file_data = f.read()
        
        chunk_names = []
        for i in range(self.num_chunks):
            start = i * self.chunk_size
            end = (i + 1) * self.chunk_size
            chunk_data = file_data[start:end]
            chunk_name = f"{os.path.basename(self.file_path)}_{i}"
            chunk_path = os.path.join(self.chunks_dir, chunk_name)

            if not os.path.exists(self.chunks_dir):
                os.makedirs(self.chunks_dir)

            with open(chunk_path, "wb") as f:
                f.write(chunk_data)
            chunk_names.append(chunk_name)
        
        print(f"File divided into {self.num_chunks} chunks.")
        return chunk_names
    
    def broadcast_chunks(self):
        while True:
            chunk_files = os.listdir(self.chunks_dir)
            chunk_names = [chunk_file for chunk_file in chunk_files if chunk_file.startswith(os.path.basename(self.file_path))]
            chunk_names_json = json.dumps({"chunks": chunk_names}).encode()
            
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                sock.sendto(chunk_names_json, (self.broadcast_address, self.broadcast_port))
                
            time.sleep(2)  # Wait for 60 seconds before broadcasting again


class Content_Discovery:
    def __init__(self):
        self.content_dict = {}
        threading.Thread(target=self.listen_broadcast).start()
        

    def listen_broadcast(self):
        # create a UDP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # bind the socket to a specific address and port
        sock.bind(("255.255.255.255", 5001))

        while True:
            # receive the broadcast message
            data, addr = sock.recvfrom(2048)

            # parse the message contents using JSON parser
            message = json.loads(data.decode())

            # get the sender's IP address
            sender_ip = addr[0]

            # store the list of files in the content dictionary
            for chunk in message['chunks']:
                if chunk not in self.content_dict:
                    self.content_dict[chunk] = []
                if sender_ip not in self.content_dict[chunk]:
                    self.content_dict[chunk].append(sender_ip)
                    # display the detected user and their hosted content on console
                    print(sender_ip + ": " + "has " + chunk)

            # write the content dictionary to a shared text file
            with open('content_dict.txt', 'w') as f:
                json.dump(self.content_dict, f)



class Chunk_Downloader:
    def __init__(self, content_dict_file):
        self.content_dict = {}
        self.content_dict_file = content_dict_file
        self.downloads = []
    
    def load_content_dict(self):
        with open(self.content_dict_file, 'r') as f:
            self.content_dict = json.load(f)

    def save_content_dict(self):
        with open(self.content_dict_file, 'w') as f:
            json.dump(self.content_dict, f)

    def download(self, content_name):
        self.load_content_dict()

        chunks = [f"{content_name} {i}" for i in range(1, 6)]

        # Initialize the download log
        with open(f"{content_name}-download.log", "a") as log_file:
            log_file.write(f"{'Timestamp':<20} {'Chunk':<15} {'Downloaded From':<20}\n")

        for chunk_name in chunks:
            ips = self.content_dict.get(chunk_name, [])
            downloaded = False

            for ip in ips:
                try:
                    # Open a TCP connection to the first IP address in the array
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect((ip, 5002))

                    # Request the chunk
                    request = {"requested content": chunk_name}
                    s.sendall(json.dumps(request).encode())

                    # Receive the chunk
                    data = b''
                    while True:
                        chunk = s.recv(1024)
                        if not chunk:
                            break
                        data += chunk

                    # Close the TCP connection
                    s.close()

                    # Write the downloaded chunk to disk
                    with open(f"{chunk_name}.chunk", "wb") as chunk_file:
                        chunk_file.write(data)

                    # Log the download
                    with open(f"{content_name}-download.log", "a") as log_file:
                        log_file.write(f"{time.time():<20} {chunk_name:<15} {ip:<20}\n")

                    downloaded = True
                    break
                except:
                    continue

            if not downloaded:
                print(f"CHUNK {chunk_name} CANNOT BE DOWNLOADED FROM ONLINE PEERS.")

        # Merge the downloaded chunks
        with open(f"{content_name}", "wb") as f:
            for chunk_name in chunks:
                with open(f"{chunk_name}.chunk", "rb") as chunk_file:
                    f.write(chunk_file.read())

        print(f"{content_name} has been successfully downloaded.")

        self.save_content_dict()


class Chunk_Uploader:
    def __init__(self, content_directory):
        self.content_directory = content_directory
        self.log_file = os.path.join(content_directory, "upload_log.txt")
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind(('localhost', 5000))
        self.socket.listen(1)

    def start(self):
        while True:
            conn, addr = self.socket.accept()
            threading.Thread(target=self.handle_connection, args=(conn, addr)).start()

    def handle_connection(self, conn, addr):
        try:
            data = conn.recv(1024)
            request = json.loads(data)
            chunk_name = request["requested content"]
            chunk_path = os.path.join(self.content_directory, chunk_name)
            if os.path.exists(chunk_path):
                with open(chunk_path, 'rb') as f:
                    chunk_data = f.read()
                    conn.sendall(chunk_data)
                    self.log_upload(chunk_name, addr)
            else:
                print(f"Chunk {chunk_name} not found on this server.")
        except json.JSONDecodeError:
            print("Invalid JSON format in message.")
        except Exception as e:
            print(f"Error: {e}")
        finally:
            conn.close()

    def log_upload(self, chunk_name, destination_address):
        with open(self.log_file, 'a') as f:
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"{timestamp}, {chunk_name}, {destination_address}\n")


class Peer:
    def __init__(self):
        self.content_dict = {}
        self.downloaded_chunks = set()
        self.uploaded_chunks = set()

        self.chunk_announcer = Chunk_Announcer()
        self.content_discovery = Content_Discovery()

        