import enum
import socket
import sys
import time
import threading
import random

class Client():
    class OperationType(enum.StrEnum):
        CREDIT = "C"
        DEBIT = "D"

    def __init__(self, message_queue_address, n):
        self.n = n
        self.message_queue = MessageQueue(message_queue_address, n)
    
    def credit(self, account, account_balance, operation_value):
        credit_operation = self.OperationType.CREDIT.value
        response = self.request_operation(account, account_balance, operation_value, credit_operation)
        message = self.build_user_response_message(response, credit_operation)
        print(f"\033[32m{message}")
    
    def debit(self, account, account_balance, operation_value):
        debit_operation = self.OperationType.DEBIT.value
        response = self.request_operation(account, account_balance, operation_value, debit_operation)
        message = self.build_user_response_message(response, debit_operation)
        print(f"\033[32m{message}")

    def request_operation(self, account, account_balance, operation_value, operation_type):
        date = int(time.time())
        message = self.message_queue.send(account, account_balance, operation_value, operation_type, date)
        return message.split(",")
    
    def build_user_response_message(self, response, operation_type):
        STATUS_INDEX = 0
        ACCOUNT_INDEX = 1
        BALANCE_INDEX = 2
        status = "SUCCESS" if response[STATUS_INDEX] == "OK" else "FAIL"
        transaction_type = "Credit" if operation_type == self.OperationType.CREDIT.value else "Debit"
        text = f"{transaction_type} transaction commited" if response[STATUS_INDEX] == "OK" else f"{transaction_type} transaction failed"
        message = f"[Thread-{self.n}][OUT] {status} -> {text}."
        if response[STATUS_INDEX] == "OK":
            message += f" New balance for account {response[ACCOUNT_INDEX]}: ${int(response[BALANCE_INDEX]) / 100:.2f}"
        return message

class MessageQueue():
    def __init__(self, address, n, timeout=3, retries=3, buffer_size=4096):
        self.n = n
        self.address = address
        self.buffer_size = buffer_size
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.settimeout(timeout)
        self.retries = retries

    def build_message(self, *args):
        return ",".join([ str(arg) for arg in args ])
    
    def send_message(self, message):
        self.socket.sendto(message.encode(), self.address)
    
    def recv_response(self):
        data, address = self.socket.recvfrom(self.buffer_size)
        return (data.decode(), address)

    def send(self, *args):
        message = self.build_message(*args)
        HOST_INDEX = 0
        PORT_INDEX = 1
        print(f"[LOG] Sending to {self.address[HOST_INDEX]}:{self.address[PORT_INDEX]} the message \"{message}\" ")
        self.send_message(message)
        tries = 0
        while tries <= self.retries:
            try:
                if tries > 0:
                    print(f"\033[33m[Thread-{self.n}][LOG] Retrying... ({tries})")
                message, (host, port) = self.recv_response()
                print(f"\033[32m[Thread-{self.n}][LOG] {host}:{port} sent: {message}")
                return message
            except TimeoutError:
                tries += 1
                print(f"\033[33m[Thread-{self.n}][WRN] Didn't received the \"OK\" confirmation message")
        print(f"\033[31m[Thread-{self.n}][ERR] Operation failed. Didn't received the \"OK\" confirmation message after ({tries}) tries")
        return "ERR"

def intInput(prompt):
    while True:
        try:
            return int(input(prompt))
        except:
            print("Please input an integer value")

def thread_fn(mq_server_host, mq_server_port, account, balance, value, n):
    client = Client((mq_server_host, int(mq_server_port)), n)
    client.debit(account, balance, value)

def main():
    args_length = len(sys.argv)
    if args_length != 2:
        print(f"Usage: {sys.argv[0]} <mq_server host:port>")
        sys.exit(11)
    mq_server_host, mq_server_port = sys.argv[1].split(":")
    
    threads = []
    for i in range(100):
        threads.append(
            threading.Thread(
                target=thread_fn,
                args=(mq_server_host,
                mq_server_port, f"Account{i}",
                random.randint(100000, 1000000),
                random.randint(2500,100000),
                i,
                )))
    
    print("Starting threads...")
    for t in threads:
        t.start()

    for t in threads:
        t.join()
    print("Threads finished!")

if __name__ == "__main__":
    main()
    # args_length = len(sys.argv)
    # if args_length != 2:
        # print(f"Usage: {sys.argv[0]} <mq_server host:port>")
        # sys.exit(11)
    # mq_server_host, mq_server_port = sys.argv[1].split(":")
    # client = Client((mq_server_host, int(mq_server_port)))
    # command = "R" # R = Running
    # command_list = (("C", "Credit"), ("D", "Debit"), ("Q", "Quit"))
    # while command != "Q": # Q = Quit
        # print("Choose one of the following commands:")
        # for (command_char, command_name) in command_list:
            # print(f"\t{command_char} - {command_name}")
        # command = input(">>> ").upper().strip()
# 
        # if command == "C" or command == "D":
            # print("Fill the following fields:")
            # account = input("Account ID: ")
            # account_balance = intInput("Account balance (cents): ")
            # operation_name = "Credit" if command == "C" else "Debit"
            # operation_value = intInput(f"{operation_name} value (cents): ")
            # 
            # if command == "C":
                # client.credit(account, account_balance, operation_value)
            # elif command == "D":
                # client.debit(account, account_balance, operation_value)
