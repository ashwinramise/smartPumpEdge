from pymodbus.pdu import ModbusRequest
from pymodbus.client.sync import ModbusSerialClient as ModbusClient
from pymodbus.transaction import ModbusRtuFramer
import time
from datetime import datetime
import pandas as pd
import paho.mqtt.client as paho
from paho import mqtt
import json
import mqtt_config as config
import socket
import ssl

mqtt_client = paho.Client(config.pumpName, clean_session=False)
topic = config.domain + 'rawdata/' + config.Customer + '/' + config.Plant + '/' + config.pumpName
broker = config.mqtt_broker
mqtt_topic = config.domain + 'edits/' + config.Customer + '/' + config.Plant + '/' + config.pumpName

regs = pd.read_csv('/root/smartPumpEdge/RegisterData.csv')
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


def getRegData(holds, t=topic):
    mets = []
    for val in holds:
        out = client.read_holding_registers(address=val, count=1,
                                            unit=1)
        mets.append({str(val): str(out.registers[0])})
        pingD = {
            'site': config.Plant,
            'pump': config.pumpName,
            'timestamp': str(datetime.now()),
            'metrics': mets
        }
        pingR = json.dumps(pingD)
        try:
            mqtt_client.publish(t, pingR, qos=1)
        except Exception as exep:
            print(f'There was an issue sending data because {r}.. Reconnecting')
            mqtt_client.connect(broker)


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker!")
        print(f"Listening on topic: {mqtt_topic}")
    else:
        print(f"Failed to connect, return code {rc}", "Error\t")


def on_disconnect(client, userdata, rc):
    print(f"Unexpected disconnection due to {rc}")
    try:
        print("Reconnecting...")
        mqtt_client.reconnect()
    except socket.error:
        time.sleep(5)
        mqtt_client.reconnect()


def on_message(client, userdata, msg):
    x = msg.payload
    command = json.loads(x)
    if command['change'] == 'change':
        # print(f"Recieved write command {command}")
        registers, bits = command['register'], command['bit']
        for i in range(len(registers)):
            writeReg(registers[i], bits[i])
    elif command['change'] == 'ping':
        getRegData(holding)
    elif command['change'] == 'req':
        getRegData(command['register'], t=f'{config.pumpName}/requested')
    # writeReg(command['register'][0], command['bit'][0])
    # writeReg(command['register'][1], command['bit'][1])


# Connect To Client and Get Data
client = ModbusClient(method='rtu', port='/dev/ttymxc3', parity='N', baudrate=9600, stopbits=2, auto_open=True,
                      timeout=3)  # 7970
# client = ModbusClient(method='rtu', port='com3', parity='N', baudrate=9600, stopbits=2, auto_open=True)  # windows
try:
    conn = client.connect()
    # enable TLS
    mqtt_client.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS, cert_reqs=ssl.CERT_NONE)
    mqtt_client.tls_insecure_set(True)
    # set username and password
    mqtt_client.username_pw_set(config.mqtt_username, config.mqtt_pass)
    # connect to HiveMQ Cloud on port 8883
    mqtt_client.connect(broker, 8883, keepalive=60)
    if conn:
        print('Connected to Pump!')
        while True:
            try:
                mqtt_client.loop_start()
                mqtt_client.on_connect = on_connect
                mqtt_client.on_message = on_message
                mqtt_client.on_disconnect = on_disconnect
                mqtt_client.subscribe(mqtt_topic, qos=1)
                mqtt_client.loop_stop()
            except Exception as r:
                print(f'There was an issue sending data because {r}.. Reconnecting')
            # read holding registers from device number 27 formulate data dictionary define data in SparkPlugB structure
            metrics = []
            current = {}
            for reg in holding:
                read = client.read_holding_registers(address=reg, count=1,
                                                     unit=1)
                metrics.append({str(reg): str(read.registers[0])})
                current.update({str(reg): str(read.registers[0])})
            pub_data = {
                'site': config.Plant,
                'pump': config.pumpName,
                'timestamp': str(datetime.now()),
                'metrics': metrics
            }
            if last_message is None or current != last_message:
                message = json.dumps(pub_data)
                last_message = current
                try:
                    mqtt_client.publish(topic, message, qos=1)
                    print(f'{datetime.now()}: published {message} to {topic}')
                except Exception as r:
                    print(f'There was an issue sending data because {r}.. Reconnecting')
                    connection = mqtt_client.connect(broker)
            elif current == last_message:
                continue
            time.sleep(10)  # repeat
    else:
        print("Error Connecting to Pump")
except Exception as e:
    print(e)
