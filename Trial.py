import time
from datetime import datetime


def writefile(k):
    with open(f'/root/smartPumpEdge/logfiles/l{k}.txt', 'w+') as file:
        file.writelines(f'This is logfile{k} written at {str(datetime.now())}')
        file.close()

print('Writing files')

for i in range(10):
    writefile(i)
    time.sleep(5)

print('Files written')
