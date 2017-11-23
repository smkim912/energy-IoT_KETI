import paho.mqtt.client as mqtt

mqttc = mqtt.Client("ketii_pub")  
#mqttc.connect("test.mosquitto.org", 1883) 
mqttc.connect("tinyos.asuscomm.com", 20062)
mqttc.publish("/oneM2M/req/material/Mobius/json", "Hello World!")
mqttc.loop(2)      
