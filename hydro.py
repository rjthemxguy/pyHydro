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

# collections for database
colWaterTemp = ""
colAmbientTemp = ""
colPH = ""
colHumidity = ""

# counters for DB send loops
waterTempCount = 0
ambientTempCount = 0
phCount = 0
humidityCount = 0



def on_message_Temp(mosq, obj, msg):
    # This callback will only be called for messages with topics that match
    global waterTempCount
    temp = msg.payload.decode()
    timeDate = datetime.datetime.now().strftime("%Y-%m-%d")
    timeTime = datetime.datetime.now().strftime("%H:%M:%S")

    if waterTempCount == 25:
        waterData = {"date":timeDate, "time":timeTime, "temp": temp}
        x = colWaterTemp.insert_one(waterData)
        waterTempCount = 0
        print("Water Temp Sent")
    else:
        waterTempCount +=1

def on_message_ambientTemp(mosq, obj, msg):
    global ambientTempCount
    temp = msg.payload.decode()
    timeDate = datetime.datetime.now().strftime("%Y-%m-%d")
    timeTime = datetime.datetime.now().strftime("%H:%M:%S")

    if ambientTempCount == 300:
        ambientData = {"date":timeDate, "time":timeTime, "temp": temp}
        x = colAmbientTemp.insert_one(ambientData)
        ambientTempCount = 0
        print("Ambient Temp sent")
    else:
        ambientTempCount +=1

def on_message_humidity(mosq, obj, msg):
    global humidityCount
    humidity = msg.payload.decode()
    timeDate = datetime.datetime.now().strftime("%Y-%m-%d")
    timeTime = datetime.datetime.now().strftime("%H:%M:%S")

    if humidityCount == 300:
        data = {"date":timeDate, "time":timeTime, "humidity": humidity}
        x = colHumidity.insert_one(data)
        humidityCount = 0
        print("Humidity sent")
    else:
        humidityCount +=1

def on_message_ph(mosq, obj, msg):
    global phCount
    ph = msg.payload.decode()
    print(ph)
    timeDate = datetime.datetime.now().strftime("%Y-%m-%d")
    timeTime = datetime.datetime.now().strftime("%H:%M:%S")

    if phCount == 25:
        data = {"date":timeDate, "time":timeTime, "ph": ph}
        x = colPH.insert_one(data)
        phCount = 0
        print("PH sent")
    else:
        phCount +=1

def mongoConnect ():
    global colWaterTemp
    global colAmbientTemp
    global colPH
    global colHumidity

    client = pymongo.MongoClient("mongodb+srv://rj:Hapkido1!@cluster0.iiuhn.mongodb.net/HydroData?retryWrites=true&w=majority")
    dblist = client.list_database_names()
    if "HydroData" in dblist:
        print("Found HydroData")
    mydb = client["HydroData"]

    # columns in database
    colWaterTemp = mydb["waterTemp"]
    colAmbientTemp = mydb["ambientTemp"]
    colPH = mydb["waterPH"]
    colHumidity = mydb["garageHumidity"]
   
    


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

    # subscribe to topics
    client.subscribe(topic)
    client.subscribe("ambientTemp")
    client.subscribe("PH")
    client.subscribe("humidity")
    client.on_message = on_message


def run():
    client = connect_mqtt()

    # callbacks for topics
    client.message_callback_add("Temp", on_message_Temp)
    client.message_callback_add("ambientTemp", on_message_ambientTemp)
    client.message_callback_add("PH", on_message_ph)
    client.message_callback_add("humidity", on_message_humidity)

    mongoConnect()
    subscribe(client)
    client.loop_forever()


if __name__ == '__main__':
    run()