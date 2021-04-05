'''
Created on Dec 8, 2020

@author: denis
'''
import asyncio

class AsyncioServer(asyncio.Protocol):
    
    metrics_storage = list()  # Хранилище метрик в виде словаря

    def connection_made(self, transport):
        self.transport = transport

    def process_client_request(self, data_to_process):
        pass
    
    def data_received(self, data):  # метод вызывающийся по принятию от клиента сообщения, data - есть само сообщение
        try:
            command, request_data = data.decode().split(maxsplit = 1)
        except ValueError:
            self.transport.write("error\nwrong command\n\n".encode())
            return
        if command not in ["put", "get"]:
            self.transport.write("error\nwrong command\n\n".encode())
            return
        elif command == "put":
            try:
                key, value, timestamp = request_data.split()
                # Проверка на запись в ячейку с уже существующей timestamp
                if bool(AsyncioServer.metrics_storage):
                    written = False  # Индикатор того, перезаписывалась ли ячейка
                    for idx, item in enumerate(AsyncioServer.metrics_storage):
                        if item[2] == int(timestamp) and item[0] == key:
                            AsyncioServer.metrics_storage[idx] = (key, float(value), int(timestamp))  # Перезаписываем эту ячейку
                            written = True
                            break
                        elif item[2] > int(timestamp) and item[0] == key:
                            AsyncioServer.metrics_storage.insert(idx, (key, float(value), int(timestamp)))
                            written = True
                            break
                    if written == False:
                        AsyncioServer.metrics_storage.append((key, float(value), int(timestamp)))
                else:
                    AsyncioServer.metrics_storage.append((key, float(value), int(timestamp)))
                self.transport.write("ok\n\n".encode())
            except ValueError:
                self.transport.write("error\nwrong command\n\n".encode())
                return
        elif command == "get":
            key = request_data.strip()  # Запрашиваемый ключ
            if len(request_data.split()) > 1:  # Посылаем ошибку, если нарушен формат ключа
                self.transport.write("error\nwrong command\n\n".encode())
                return
            else:
                response = "ok\n"  # Формируем ответ на запрос в строке response
                for item in AsyncioServer.metrics_storage:
                    if item[0] == key or key == "*":
                        response += " ".join([str(i) for i in item]) + "\n"
                    
                response += "\n"
                self.transport.write(response.encode())

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
    