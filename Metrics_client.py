'''
Created on Nov 7, 2020

@author: denis
'''
import time
import socket
import operator

class ClientError(Exception):
    "Пользовательское исключение ClientError"
    pass

class Client:
    "Создание клиента для отправки метрик об использовании компьютерного железа на сервер"
    
    def __init__(self, host, port, timeout = None):
        try:
            self.conn = socket.create_connection((host, port), timeout)
        except (SystemExit, KeyboardInterrupt):  # Закрываем соединение при завершении программы
            self.conn.close()
    
    def put(self, m_name, m_val, timestamp = None):
        "Метод для отправки метрик на сервер"
        try:
            if timestamp == None: timestamp = int(time.time())    
            m_put_str = f"put {m_name} {m_val} {timestamp}\n"  # Строка текста отправляемая на сервер, содержит в себе информацию о собираемых метриках
            self.conn.sendall(m_put_str.encode())  # Отправляем данные на сервер
            m_serv_ans = self.conn.recv(1024)  # Принимаем ответ сервера
            if m_serv_ans.decode().split()[0] == "error": # Невалидные данные
                raise ClientError  
        except ClientError as err:
            print("На сервер переданы невалидные данные!")
            raise
    
    def get(self, m_name):  
        "Метод для приема метрик с сервера"
        try:
            m_get_str = f"get {m_name}\n"  # Строка текста запроса на прием метрик от сервера
            self.conn.sendall(m_get_str.encode())  # Отправляем запрос на сервер
            m_serv_ans = self.conn.recv(1024)  # Принимаем ответ сервера
            status = m_serv_ans.decode().split()[0]
            if m_serv_ans.decode().split()[0] != "ok":  # Проверка поля статуса сооб. ответа сервера
                raise ClientError("Статус сообщения ответа сервера содержит 'error' либо невалидное значение")
            elif len(m_serv_ans.decode().split()) == 1 \
            and m_serv_ans.decode().split()[0] == "ok":  # Данных по запрашиваему ключу нет
                return dict()
            elif len(m_serv_ans.decode().split()) < 4:  # Проверка длинны сообщения с данными
                raise ClientError
            
            #  Парсим ответ сервера
            ret_m_dict = dict()  # Итоговый возвращаемый словарь
            
            lines = m_serv_ans.decode().splitlines()[1:-1]
            for line in lines:
                words = line.split()
                if words[0] != m_name and m_name != "*":  # Проверка на валидность названия метрики
                    raise ClientError("Название метрики не соответствует запрошенному")
                if not words[0] in ret_m_dict:  # Проверяем имя метрики на вхождение в словарь
                    ret_m_dict[words[0]] = list()
                try:  # Обрабатываем сценарий, если у нас имеются невалидные значения метрик и(или) timestamp
                    ret_m_dict[words[0]].append((int(words[2]), float(words[1])))
                except ValueError:
                    raise ClientError("невалидные значения метрик и(или) timestamp")
            for m_list in ret_m_dict.values():  # цикл для сортировки кортежей метрик по возрастанию timestamp
                m_list.sort(key = operator.itemgetter(0))
            return ret_m_dict
                            
        except ClientError as err:
            print("Произошла ошибка считывания данных с сервера!")
            raise
            
    
if __name__ == "__main__":
    client = Client("localhost", 8888)
    print(client.put("metric_name", 56.5))
    
    