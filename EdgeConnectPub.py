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
mqtt_topic = config.listen_topic

regs = pd.read_csv('RegisterData.csv')
holding = regs['Address'].tolist()


def writeReg(register, bit):
    try:
        conn = client.connect()
        if conn:
            print('Connected')
            try:
                client.write_register(address=register, value=bit, unit=1)
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
    print(command, command['register'][1], command['bit'][1])
    writeReg(command['register'][0], command['bit'][0])
    writeReg(command['register'][1], command['bit'][1])


# Connect To Client and Get Data
client = ModbusClient(method='rtu', port='/dev/ttyUSB0', parity='N', baudrate=9600, stopbits=2, auto_open=True) #pi
# client = ModbusClient(method='rtu', port='com3', parity='N', baudrate=9600, stopbits=2, auto_open=True)  # windows
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
                                                     unit=1)
                metrics.append({str(reg): str(read.registers[0])})
            pub_data = {
                'site': 'digitalHUB',
                'pump': 'dda1',
                'timestamp': str(datetime.now()),
                'metrics': metrics
            }
            message = json.dumps(pub_data)
            try:
                mqtt_client.publish(topic, message, qos=0)  # publish to MQTT Broker every 5s
                # print(f'{datetime.now()}: publishing {message} to {topic}')
                mqtt_client.loop_start()
                mqtt_client.on_connect = on_connect
                mqtt_client.on_message = on_message
                mqtt_client.on_disconnect = on_disconnect
                mqtt_client.subscribe(mqtt_topic)
                mqtt_client.loop_stop()
            except:
                print('There was an issue sending data')
            time.sleep(5)
    else:
        print("Error Connecting to Pump")
except Exception as e:
    print(e)
