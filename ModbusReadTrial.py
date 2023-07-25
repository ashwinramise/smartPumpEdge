from pymodbus.pdu import ModbusRequest
from pymodbus.client.sync import ModbusSerialClient as ModbusClient
from pymodbus.transaction import ModbusRtuFramer
from datetime import datetime
import json
import csv

k = []
with open("RegisterData.csv", "r") as csvfile:
    reader_variable = csv.reader(csvfile, delimiter=",")
    for row in reader_variable:
        k.append(row)

holding = [int(i[0]) for i in k[1:]]


def getRegData(client, holds):
    mets = []
    for val in holds:
        try:
            out = client.read_holding_registers(address=val, count=10, unit=1)
            mets.append({str(val): str(out.registers[0])})
        except Exception as e:
            print(e)
    pingD = {
        'site': "test",
        'pump': "test",
        'timestamp': str(datetime.now()),
        'metrics': mets
    }
    return pingD


modbus_client = ModbusClient(method='rtu', port='/dev/ttymxc3', parity='N', baudrate=9600, stopbits=2, auto_open=True,
                             timeout=3)
print(holding)
Regdata = getRegData(modbus_client, holding)
print(Regdata)