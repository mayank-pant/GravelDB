import socket

def send_redis_commands(host='localhost', port=6371, num_keys=50000):
    with socket.create_connection((host, port)) as sock:
        for i in range(30000, num_keys + 1):
            command = f"SET {i} {i}\r\n"
            sock.sendall(command.encode())
            response = sock.recv(1024)
            print(f"SET {i} {i} -> {response.decode().strip()}")

if __name__ == "__main__":
    send_redis_commands()