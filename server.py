import socket
import sys

class CreditShard():
    def execute(balance, value):
        return balance + value

class DebitShard():
    def execute(balance, value):
        return balance - value

class TransactionCoordinator():
    def __init__(self, address=("localhost", 8080), buffer_size=4096):
        self.buffer_size = buffer_size
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(address)

    def start(self):
        print(f"Transaction coordinator listening on {self.socket.getsockname()[0]}:{self.socket.getsockname()[1]}")
        ACCOUNT_INDEX = 0
        BALANCE_INDEX = 1
        VALUE_INDEX = 2
        KIND_INDEX = 3
        while True:
            data, address = self.recv_message()
            request = data.split(",")
            account_id = request[ACCOUNT_INDEX]
            balance = int(request[BALANCE_INDEX])
            operation_value = int(request[VALUE_INDEX])
            operation_kind = request[KIND_INDEX]
            if operation_kind == "C":
                result = CreditShard.execute(balance, operation_value)
                response = self.build_message("OK", account_id, result)
            elif operation_kind == "D":
                result = DebitShard.execute(balance, operation_value)
                response = self.build_message("OK", account_id, result)
            else:
                response = self.build_message("ERR", f"Operation {operation_kind} doesn't exist")
            self.socket.sendto(response.encode(), address)
    
    def build_message(self, *args):
        return ",".join([ str(arg) for arg in args ])

    def recv_message(self):
        data, address = self.socket.recvfrom(self.buffer_size)
        return (data.decode(), address)
    
if __name__ == "__main__":
    args_length = len(sys.argv)
    if args_length != 2:
        print(f"Usage: {sys.argv[0]} <host:port>")
        sys.exit(11)
    server_address_arg = sys.argv[1].split(":")
    server_address = (server_address_arg[0], int(server_address_arg[1]))
    transaction_coordinator = TransactionCoordinator(server_address)
    transaction_coordinator.start()
