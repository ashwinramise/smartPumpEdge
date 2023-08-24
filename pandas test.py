import pandas as pd

# regs = pd.read_csv('/root/smartPumpEdge/RegisterData.csv')  # 7970
regs = pd.read_csv('RegisterData.csv') # windows
holding = regs['Address'].tolist()
print(holding)