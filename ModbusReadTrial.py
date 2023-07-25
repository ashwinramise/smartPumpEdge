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


def getRegData(client, val):
    try:
        out = client.read_holding_registers(address=val, count=1, unit=2)
        metric = {str(val): str(out.registers[0])}
        return metric
    except Exception as e:
        print(e)


modbus_client = ModbusClient(method='rtu', port='/dev/ttymxc2', parity='N', baudrate=9600, stopbits=2, auto_open=True,
                             timeout=3)
while True:
    value = int(input(f'Enter the register number: '))
    print(getRegData(modbus_client, value))

