import random
import pymongo
import datetime

from paho.mqtt import client as mqtt_client


broker = '138.197.194.35'
port = 1883
topic = "Temp"
# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 100)}'
username = 'rj'
password = 'Hapkido1!'
mycol = ""
waterTempCount = 0


def on_message_Temp(mosq, obj, msg):
    # This callback will only be called for messages with topics that match
    global waterTempCount
    print(msg.payload.decode())
    temp = msg.payload.decode()
    timeDate = datetime.datetime.now().strftime("%Y-%m-%d")
    timeTime = datetime.datetime.now().strftime("%H:%M:%S")

    if waterTempCount == 5:
        waterData = {"date":timeDate, "time":timeTime, "temp": temp}
        x = mycol.insert_one(waterData)
        waterTempCount = 0
    else:
        waterTempCount +=1

def on_message_ambientTemp(mosq, obj, msg):
    # This callback will only be called for messages with topics that match
    
    print(msg.payload.decode())

def mongoConnect ():
    global mycol
    client = pymongo.MongoClient("mongodb+srv://rj:Hapkido1!@cluster0.iiuhn.mongodb.net/HydroData?retryWrites=true&w=majority")
    dblist = client.list_database_names()
    if "HydroData" in dblist:
        print("Found HydroData")
    mydb = client["HydroData"]
    mycol = mydb["waterTemp"]
    waterData = { "temp": "66"}
    x = mycol.insert_one(waterData)
    


def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)

    return client


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        print(msg.payload.decode())

    client.subscribe(topic)
    client.subscribe("ambientTemp")
    client.on_message = on_message


def run():
    client = connect_mqtt()
    client.message_callback_add("Temp", on_message_Temp)
    client.message_callback_add("ambientTemp", on_message_ambientTemp)
    mongoConnect()
    subscribe(client)
    client.loop_forever()


if __name__ == '__main__':
    run()