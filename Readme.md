MicroPython MQTT
===
upaho-mqtt is a MicroPython version of the [Eclipse python paho-mqtt](https://github.com/eclipse/paho.mqtt.python).

It supports:
- MQTT v3.1 and v3.1.1
- QOS 0, 1, 2

Includes an updated [threading.py](threading.py) which implements the Lock() and stack_size() functions.

---
> NOTE: Tested on hardware: Pycom GPy (ESP32 and Sequans LTE modem), with firmware: Pybytes release v1.20.2.r3
