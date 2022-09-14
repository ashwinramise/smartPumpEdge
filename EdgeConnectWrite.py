from pymodbus.pdu import ModbusRequest
from pymodbus.client.sync import ModbusSerialClient as ModbusClient
from pymodbus.transaction import ModbusRtuFramer
import time
from datetime import datetime
import pandas as pd
import paho.mqtt.client as mqtt
import json
import mqtt_config as config

mqttClient = mqtt.Client(config.mqtt_client)
mqtt_topic = config.listen_topic
mqttBroker = config.mqtt_broker

client = ModbusClient(method='rtu', port='/dev/ttyUSB0', parity='N', baudrate=9600, stopbits=2, auto_open=True) #pi
# client = ModbusClient(method='rtu', port='com5', parity='N', baudrate=9600, stopbits=2, auto_open=True)  # windows


def writeReg(register, bit):
    try:
        conn = client.connect()
        if conn:
            print('Connected')
            try:
                client.write_register(address=register, value=bit, unit=27)
                print("Write Success")
            except Exception as e:
                print(e)
    except Exception as k:
        print(k)


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker!")
    else:
        print(f"Failed to connect, return code {rc}", "Error\t")


def on_disconnect(client, userdata, rc):
    print("Unexpected disconnection.")


def on_message(client, userdata, msg):
    x = msg.payload
    command = json.loads(x)
    writeReg(command['register'], command['bit'])


mqttClient.connect(mqttBroker)
mqttClient.on_connect = on_connect
mqttClient.on_message = on_message
mqttClient.on_disconnect = on_disconnect
mqttClient.subscribe(mqtt_topic)
mqttClient.loop_forever()