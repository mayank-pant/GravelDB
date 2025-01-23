import socket

class RedisCLI:
    def __init__(self, host='localhost', port=6379):
        self.host = host
        self.port = port
        self.client = None

    def connect(self):
        """Establishes a connection to the Redis-like server."""
        try:
            self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.client.connect((self.host, self.port))
            print(f"Connected to server at {self.host}:{self.port}")
        except Exception as e:
            print(f"Error connecting to server: {e}")
            exit(1)

    def send_command(self, command):
        """Sends a command to the server and receives the response."""
        try:
            self.client.sendall((command + "\r\n").encode('utf-8'))
            response = self.client.recv(4096)
            return response.decode('utf-8')
        except Exception as e:
            return f"Error communicating with server: {e}"

    def run(self):
        """Starts the CLI interface."""
        self.connect()
        print("Type your Redis commands. Type 'exit' to quit.\n")

        while True:
            try:
                command = input("> ")
                if command.lower() in ('exit', 'quit'):
                    print("Closing connection.")
                    break
                if not command.strip():
                    continue
                response = self.send_command(command)
                print(response.strip())
            except KeyboardInterrupt:
                print("\nClosing connection.")
                break
            except Exception as e:
                print(f"Unexpected error: {e}")
                break

        self.client.close()

if __name__ == "__main__":
    cli = RedisCLI(host="localhost", port=6379)
    cli.run()