from pymodbus.pdu import ModbusRequest
from pymodbus.client.sync import ModbusSerialClient as ModbusClient
from pymodbus.transaction import ModbusRtuFramer
from datetime import datetime
import json


def getRegData(client, holds):
    mets = []
    try:
        out = client.read_holding_registers(address=holds, count=10, unit=1)
        mets.append({str(holds): str(out.registers[0])})
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
Regdata  = getRegData(modbus_client, 201)
print(Regdata)