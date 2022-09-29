from pymodbus.pdu import ModbusRequest
from pymodbus.client.sync import ModbusSerialClient as ModbusClient
from pymodbus.transaction import ModbusRtuFramer
import time
from datetime import datetime
import pandas as pd
import paho.mqtt.client as mqtt
import json
import mqtt_config as config

mqtt_client = mqtt.Client(config.pumpName)
topic = config.domain + 'rawdata/' + config.Location + '/' + config.pumpName
broker = config.mqtt_broker
mqtt_topic = config.domain + 'edits/' + config.Location + '/' + config.pumpName
readout_topic = config.domain + 'readoutRequest/' + config.Location + '/' + config.pumpName
readout_publish = config.domain + 'readout/' + config.Location + '/' + config.pumpName


regs = pd.read_csv('RegisterData.csv')
holding = regs['Address'].tolist()

last_message = None


def writeReg(register, bit):
    try:
        conn = client.connect()
        if conn:
            print('Connected to pump')
            try:
                client.write_register(address=register, value=bit, unit=1)
                print("Write Success")
            except Exception as e:
                print(e)
    except Exception as k:
        print(k)


def getRegisterData(register_list):
    data = []
    for register in register_list:
        readout = client.read_holding_registers(address=register, count=1,
                                             unit=1)
        metrics.append({str(register): str(readout.registers[0])})
    reqestedData = {
        'site': config.Location,
        'pump': config.pumpName,
        'timestamp': str(datetime.now()),
        'metrics': data
    }
    mqtt_client.publish(readout_publish, reqestedData, qos=0)
    print('Readout Published')




def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker!")
        print(f"Listening on topic: {mqtt_topic}")
    else:
        print(f"Failed to connect, return code {rc}", "Error\t")


def on_disconnect(client, userdata, rc):
    print(f"Unexpected disconnection due to {rc}")
    while True:
        conn = mqtt_client.connect(broker, keepalive=60)
        print("Reconnecting...")
        if conn:
            break
        else:
            continue
        time.sleep(5)


def on_message(client, userdata, msg):
    x = msg.payload
    command = json.loads(x)
    print(f"Recieved write command {command}")
    if 'bit' in command.keys():
        writeReg(command['register'][0], command['bit'][0])
        writeReg(command['register'][1], command['bit'][1])
    elif 'command' in command.keys():
        getRegisterData(command['command'])



# Connect To Client and Get Data
client = ModbusClient(method='rtu', port='/dev/ttyUSB0', parity='N', baudrate=9600, stopbits=2, auto_open=True,
                      timeout=3)  # pi
# client = ModbusClient(method='rtu', port='com3', parity='N', baudrate=9600, stopbits=2, auto_open=True)  # windows
try:
    conn = client.connect()
    mqtt_client.connect(broker, keepalive=60)
    if conn:
        print('Connected to Pump!')
        while True:
            try:
                mqtt_client.loop_start()
                mqtt_client.on_connect = on_connect
                mqtt_client.on_message = on_message
                mqtt_client.on_disconnect = on_disconnect
                mqtt_client.subscribe([mqtt_topic, readout_topic])
                mqtt_client.loop_stop()
            except Exception as r:
                print(f'There was an issue sending data because {r}.. Reconnecting')
                connection = mqtt_client.connect(broker)
            # read holding registers from device number 27 formulate data dictionary define data in SparkPlugB structure
            metrics = []
            current = {}
            for reg in holding:
                read = client.read_holding_registers(address=reg, count=1,
                                                     unit=1)
                metrics.append({str(reg): str(read.registers[0])})
                current.update({str(reg): str(read.registers[0])})
            pub_data = {
                'site': config.Location,
                'pump': config.pumpName,
                'timestamp': str(datetime.now()),
                'metrics': metrics
            }
            if last_message is None or current != last_message:
                message = json.dumps(pub_data)
                last_message = current
                try:
                    mqtt_client.publish(topic, message, qos=0)
                    print(f'{datetime.now()}: published {message} to {topic}')
                except Exception as r:
                    print(f'There was an issue sending data because {r}.. Reconnecting')
                    connection = mqtt_client.connect(broker)
            elif current == last_message:
                continue
            time.sleep(2)  # repeat`
    else:
        print("Error Connecting to Pump")
except Exception as e:
    print(e)
