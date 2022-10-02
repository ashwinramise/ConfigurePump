from pymodbus.pdu import ModbusRequest
from pymodbus.client.sync import ModbusSerialClient as ModbusClient
from pymodbus.transaction import ModbusRtuFramer
import time
import paho.mqtt.client as mqtt
import json
import mqtt_config as config

mqtt_client = mqtt.Client(config.pumpName)
topic = config.domain + 'rawdata/' + config.Location + '/' + config.pumpName
broker = config.mqtt_broker
mqtt_topic = config.domain + 'edits/' + config.Location + '/' + config.pumpName


def writeReg(register, bit):
    try:
        client.write_register(address=register, value=bit, unit=1)
        print("Write Success")
    except Exception as e:
        print(e)


def readReg(register):
    try:
        readout = client.read_holding_registers(address=register, count=1, unit=1)
        return readout.registers[0]
    except Exception as e:
        print(e)



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
        writeReg(command['register'][2], command['bit'][2])
    else:
        requested = {}
        for reg in command['register']:
            readout = readReg(reg)
            requested.update({reg: readout})
        package = json.dumps(requested)
        mqtt_client.publish(topic, package, qos=1)


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
                mqtt_client.subscribe(mqtt_topic)
                mqtt_client.loop_stop()
            except Exception as r:
                print(f'There was an issue sending data because {r}.. Reconnecting')
                connection = mqtt_client.connect(broker)
            time.sleep(2)  # repeat`
    else:
        print("Error Connecting to Pump")
except Exception as e:
    print(e)
