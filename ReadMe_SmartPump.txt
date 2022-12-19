Configuration Instructions for Smart Pump 
On a fresh configured 7970

Install MobaXterm on PC
Connect using Serial
username: root
password: Bul@bAckumenMC21

Copy Mobile modem config into it

Commands: 

unzip BB_Controller_3.7.0.zip

cd BB_Controller_3.7.0

bash quick_install.sh arm aquaman prod

## Installation will take some time - be patient. On completion:

Disconnect Ethernet

Command: sudo reboot

## Reboot will take around 5 minutes
## Make sure SIM card is activated and data plan is on 250 MB. Get IP address from IL

On Windows: 
Win+R
cmd

Commands
ssh root@IPaddress

Enter password

Commands: 

## Install Python 3
sudo apt-get install python3

## Install Pip 
sudo apt-get install pip3

## Install git
sudo apt-get install git

## Clone Repository
git clone https://github.com/ashwinramise/smartPumpEdge

## Move into directory
cd smartPumpEdge

## Install all requirements
pip install -r requirements.txt

## Edit plant name and customer name
vi mqtt_config.py

## Press "i" to edit 
## Edit the customer name and plant name - Should be as is in the database. 
## press Esc key --> :w and hit enter key --> :q and hit enter key

## Check if the pump can be controlled remotely

Commands
sudo python3 smartPumpEdge/EdgeConnect.py

## use the dashboard to select and change pump parameters. 




