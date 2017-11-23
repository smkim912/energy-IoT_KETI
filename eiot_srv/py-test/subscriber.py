import paho.mqtt.client as mqtt
import time

def on_disconnect(client, userdata, rc):
	print "Disconnected from MQTT server with code: %s" % rc + "\n"
	while rc != 0:
		time.sleep(1)
		print "Reconnecting..."
		rc=mqtt.reconnect()

def on_connect(client, userdata, rc):
  print ("Connected with result coe " + str(rc))
  client.subscribe("#")

def on_message(client, userdata, msg):
  print "Topic: ", msg.topic + '\nMessage: ' + str(msg.payload)

client = mqtt.Client()
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message

client.connect("tinyos.asuscomm.com", 20062, 60)

client.loop_forever()
