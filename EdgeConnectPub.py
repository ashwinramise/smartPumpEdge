from pymodbus.pdu import ModbusRequest
from pymodbus.client.sync import ModbusSerialClient as ModbusClient
from pymodbus.transaction import ModbusRtuFramer
import time
from datetime import datetime
import pandas as pd
import paho.mqtt.client as mqtt
import json
import mqtt_config as config

mqtt_client = mqtt.Client(config.mqtt_client)
topic = config.publish_topic
broker = config.mqtt_broker

regs = pd.read_csv('RegisterData.csv')
holding = regs['Address'].tolist()
# Connect To Client and Get Data
client = ModbusClient(method='rtu', port='/dev/ttyUSB0', parity='N', baudrate=9600, stopbits=2, auto_open=True) #pi
# client = ModbusClient(method='rtu', port='com5', parity='N', baudrate=9600, stopbits=2, auto_open=True)  # windows
try:
    conn = client.connect()
    mqtt_client.connect(broker)
    if conn:
        print('Connected')
        while True:
            # read holding registers from device number 27 formulate data dictionary define data in SparkPlugB structure
            metrics = []
            for reg in holding:
                read = client.read_holding_registers(address=reg, count=1,
                                                     unit=27)
                metrics.append({str(reg): str(read.registers[0])})
            print(metrics)
            pub_data = {
                'site': 'digitalHUB',
                'pump': 'dda1',
                'timestamp': str(datetime.now()),
                'metrics': metrics
            }
            message = json.dumps(pub_data)
            try:
                mqtt_client.publish(topic, message, qos=0)  # publish to MQTT Broker every 5s
                print(f'{datetime.now()}: publishing {message} to {topic}')
            except:
                print('There was an issue sending data')
            time.sleep(5)
    else:
        print("Error Connecting to Pump")
except Exception as e:
    print(e)
