# !/usr/bin/python
# -*- coding: utf-8 -*-
#
# HOMEBASE RX program for recieving packets via the NRF24L01+chip using the lib_nrf24 lib
# Target device is RPi2/3

# TODO : clean up protocol doc

# General protocol:
# Radio has 2 states, open (0) or waiting for device (1-255)
# Radio exposes states in ack packages using the first byte
# Radio begins with a state of open (0)
# once a channel is requested by a device the state changes to the id of that device (1-255)
# any inbound messages not matching the id are ignored
# if no message is recieved from the target device the radio will release the hold and return to open (0)

from lib_nrf24 import NRF24
import RPi.GPIO as GPIO
import time
import spidev
import config
import json
from kafka import KafkaProducer
import datetime
import logging
import logging.handlers as handlers
from kafka.errors import KafkaError

producer = KafkaProducer(bootstrap_servers=config.CONFIG['kafka_producer'])
logger = logging.getLogger('HomeBaseRx')
error_file = config.CONFIG['error_file']
logger.setLevel(logging.INFO)

# Here we define our formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logHandler = handlers.TimedRotatingFileHandler('logs/HomeBaseRx.log', when='D', interval=1, backupCount=10)
logHandler.setLevel(logging.INFO)
# Here we set our logHandler's formatter
logHandler.setFormatter(formatter)

logger.addHandler(logHandler)
# set pin mode
GPIO.setmode(GPIO.BCM)

# set 2 default pipes 0xe7e7e7e7e7 0xc2c2c2c2c2
pipes = config.CONFIG['nrf24-pipes']

# CE (GPIO22) - CSN (SPICSO)
csn_pin = config.CONFIG['csn_pin']
ce_pin = config.CONFIG['ce_pin']

# device timeout (seconds)
# this is the number fo seconds the recieving station will hold the channel open for a particular device
# without any communication
rx_hold_timeout = config.CONFIG['rx_hold_timeout']

# define length of message part in bytes
# WW XX YY ZZ.....ZZ
id_length = 1   # device id (0-255)
# device type dictates the type of transmitter (i.e. temperature/humidity, soil moisture level, etc..)
device_type_length = 1
# some devices might have multiple possible data packets (i.e. temperature, humidity)
msg_type_length = 1
# indication of whether or not the current packet is the last one in the transfer
tail_length = 1
msg_body_length = 20  # message body size

radio = NRF24(GPIO, spidev.SpiDev())
radio.begin(csn_pin, ce_pin)  # set pins CE - GPIO22, CSN - SPICSO/GPIO8
radio.setPayloadSize(id_length + device_type_length +
                     msg_type_length + msg_body_length)  # default payload size
radio.setChannel(config.CONFIG['nrf24-channel'])  # set default channel
# set transfer rate and power usage (must match transmitter)
radio.setDataRate(NRF24.BR_250KBPS)
radio.setPALevel(NRF24.PA_LOW)
# enable automatic acknowlegement
radio.setAutoAck(True)
radio.enableDynamicPayloads()
radio.enableAckPayload()
# radio.openWritingPipe(pipes[0]) #-# code is not required for recieving data
# set the reading pipe (must match transmitter)
radio.openReadingPipe(0, pipes[1])
radio.printDetails()
radio.startListening()
now = datetime.datetime.now()
# packet object with general attributes
s = "NRF24 channel : " + str(config.CONFIG['nrf24-channel'])
logger.info(s);
s= "Pipes : " + ' '.join(str(x) for x in config.CONFIG['nrf24-pipes'])
logger.info(s);
s = "RX holdout timeout: " + str(config.CONFIG['rx_hold_timeout'])
logger.info(s);
s = "CS pin : " + str(config.CONFIG['csn_pin']) + " | CE pin : " + str(config.CONFIG['ce_pin'])
logger.info(s);
s = "Kafka server : " + config.CONFIG['kafka_producer']
logger.info(s);

class Packet:
    def __init__(self, device, device_type, type, msg, tail):
        self.device = device
        self.type = type
        self.device_type = device_type
        self.msg = msg
        self.tail = tail

# read message content into the packet object based on the defined component lenths


def read_packet(packet):
    device = int(packet[:id_length], 16)
    device_type = packet[id_length:device_type_length+id_length]
    type = packet[device_type_length +
                  id_length:msg_type_length + device_type_length + id_length]
    tail = packet[msg_type_length + device_type_length +
                  id_length: msg_type_length + device_type_length + id_length + tail_length]
    msg = packet[id_length + msg_type_length +
                 tail_length + device_type_length:]
    s = ("Device: " + ' '.join(str(x) for x in device) +
          " | Msg type: " + ' '.join(str(x) for x in type) +
          " | Device type: " + ' '.join(str(x) for x in device_type) + 
          " | Message body: " + ' '.join(chr(int(x)) for x in msg) + 
          " | Message #: " + ' '.join(str(x) for x in tail))
    logger.info(s)
    return Packet(device, device_type, type, msg, tail)

# build an ack for the current packet


def build_ack(packet):
    ack_buffer = [0] * (id_length + msg_type_length +
                        device_type_length + msg_body_length)
    ack_buffer[0] = packet.device[0]
    return ack_buffer

# TODO: update with KAFKA logic
# process packet


def process_packet(packet):
    s = "Doing something with packet, for device " + ' '.join(str(x) for x in packet.device)
    logger.info(s)
    kafka_data = build_kafka_packet(packet.device, packet.type, packet.device_type, packet.msg, packet.tail, time.time())
    #{'device': packet.device, 'type': packet.type,
    #              'device_type': packet.device_type, 'msg': packet.msg, 'tail': packet.tail, 'timestamp': time.time()}
    try:
        producer.send(config.CONFIG['kafka_channel'], json.dumps(kafka_data))
    except KafkaError as error:
        save_failed_payload(kafka_data)
        logger.error("Faile to send payload to Kafka endpoint, caused by %s", error.message)
    
    if(packet.tail == 1):
        return False
    else:
        return True

# build empty ack (i.e. radio free)
def clear_ack():
    ack_buffer = [0] * (id_length + device_type_length +
                        msg_type_length + msg_body_length)
    return ack_buffer

def build_kafka_packet(device, type, device_type, msg, tail, time):
    return {'device': device, 'type': type, 'device_type': device_type, 'msg': msg, 'tail': tail, 'timestamp': time}

# save offline payload
def save_failed_payload(payload):
    with open('Failed.py', 'w') as file:
        file.write(payload)

# start listening / reading from the pipe
def start(rx_hold_timeout):
    logger.info("Starting HomeBaseRX application.");
    ack_target = [0]  # set open
    ak_buffer = clear_ack()  # load the open ack
    rx_listening_start = time.time()  # init the timestamp (used for dropping holds)
    radio.writeAckPayload(1, ak_buffer, len(ak_buffer))  # setup the ack (0)
    while True:
        pipe = [0]
        # keep checking for incoming data
        while not radio.available(pipe):
            time.sleep(1/100.0)  # sleep for 1/100 of a second
        now = datetime.datetime.now()
        # define an array to store incoming data
        recv_buffer = []
        # read the data into the array
        radio.read(recv_buffer, radio.getDynamicPayloadSize())
        packet = read_packet(recv_buffer)

        # if no message is recieved for X seconds (dictated by holdout time) release the channel
        if(time.time() - rx_listening_start >= rx_hold_timeout and ack_target != [0]):     
            s = ("Releasing channel... device " + ' '.join(str(x) for x in ack_target) + " took too long to respond")
            logger.info(s);
            ack_target = [0]
            ak_buffer = clear_ack()
        # if the channel is open, accept the device
        if(ack_target == [0]):            
            if(packet.type == [0]):
                ack_target = packet.device
                ak_buffer = build_ack(packet)
                rx_listening_start = time.time()  # begin hold counter
            else:
                #ak_buffer[0] = 255
                logger.info("Message ignored... ack re-loaded");
        # if incoming packet matches the target device process message
        elif(ack_target == packet.device and packet.type != [0]):
            if (process_packet(packet)):  # release if tail
                ack_target = [0]
                ak_buffer = clear_ack()
            rx_listening_start = time.time()  # extend hold counter
        elif(ack_target == packet.device):
            ak_buffer = clear_ack()
        else:            
            logger.info("Message ignored... ack re-loaded");

        # sent ack payload and print to terminal
        radio.writeAckPayload(1, ak_buffer, len(ak_buffer))
        s = "Loaded payload reply : " + ' '.join(str(x) for x in ak_buffer)
        logger.info(s);


# run listener
start(rx_hold_timeout)


def minute_passed(oldepoch):
    return time.time() - oldepoch >= 60
