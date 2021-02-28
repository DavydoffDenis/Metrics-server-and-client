'''
Created on Dec 8, 2020

@author: denis
'''
import asyncio

class AsyncioServer(asyncio.Protocol):
    
    metrics_storage = dict()  # Хранилище метрик в виде словаря
    
    def connection_made(self, transport):
        self.transport = transport

    def process_client_request(self, data_to_process):
        pass
    
    def data_received(self, data):  # метод вызывающийся по принятию от клиента сообщения, data - есть само сообщение
        
        command, request_data = data.decode().split(maxsplit = 1)
        
        if command not in ["put", "get"]:
            self.transport.write("error\nwrong command\n\n".encode())
            return
        elif command == "put":
            key, value, timestamp = request_data.split()
            AsyncioServer.metrics_storage[key] = (float(value), int(timestamp))
            
        resp = self.process_client_request(data.decode())
        self.transport.write(resp.encode())

def run_server(host = "localhost", port = 8888):
    loop = asyncio.get_event_loop()
    coro = loop.create_server(AsyncioServer, host, port)
    server = loop.run_until_complete(coro)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()

if __name__ == '__main__':
    run_server()
    