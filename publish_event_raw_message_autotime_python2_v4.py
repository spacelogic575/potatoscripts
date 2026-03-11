# -*- coding: utf-8 -*-
from __future__ import print_function  # Enables Python 3 style print()
import json
import sys
import time
import pika
import sqlite3
from datetime import datetime, timedelta

# Utility module imports (Assuming these modules exist in your environment)
from utility_module import RMQMessageSender, Utility

# For Python 2, we must define a UTC class as it's not in the datetime module
class UTC(datetime.tzinfo):
    """UTC implementation for Python 2.7"""
    def utcoffset(self, dt):
        return timedelta(0)
    def tzname(self, dt):
        return "UTC"
    def dst(self, dt):
        return timedelta(0)

utc_tz = UTC()

totalfoundrows = 0
deviceinformation = ""
fulldeviceinformation = ""

class Send_EventRaw(RMQMessageSender):
    """Send data via backhaul rewrite module."""
    def __init__(self):
        self.chip_serial = Utility.get_serial()
        self.parameters = pika.ConnectionParameters(
            host='localhost',
            credentials=pika.PlainCredentials('guest', 'guest')
        )
        self.message_sender = RMQMessageSender()
        self.message_sender.init(self.parameters)

    def send_persistent_data(self, data, endtimestamp):
        """Send single record to RMQ."""
        # Convert integer or string timestamp to datetime
        if isinstance(endtimestamp, (int, float, long)):
            local_dt = datetime.fromtimestamp(float(endtimestamp))
            utc_dt = datetime.fromtimestamp(float(endtimestamp)).replace(tzinfo=utc_tz)
        else:
            try:
                # String to datetime
                local_dt = datetime.strptime(str(endtimestamp), "%Y-%m-%d %H:%M:%S")
                utc_dt = local_dt.replace(tzinfo=utc_tz)
            except ValueError:
                # If stored as UNIX timestamp string
                ts = float(endtimestamp)
                local_dt = datetime.fromtimestamp(ts)
                utc_dt = datetime.fromtimestamp(ts).replace(tzinfo=utc_tz)

        data.update({
            'CameraSerial': self.chip_serial,
            'UploadedLocalDateTime': local_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'UploadedUTCDateTime': utc_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'EventStartLocalTime': local_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'EventStartUTCTime': utc_dt.strftime('%Y-%m-%d %H:%M:%S'),
        })

        body = {
            "type": "json",
            "data": json.dumps(data),
            "topic": 'ffc-eventrawdata'
        }
        
        # Determine routing key based on device serial prefix
        # Use string slicing carefully for Python 2 strings
        prefix = str(deviceinformation)[:3]
        
        if prefix == '15F':
            self.message_sender.send_message(
                body=json.dumps(body),
                prop=pika.BasicProperties(
                    content_type="application/json"
                ),
                exchange_name='data',
                routing_key='backhaul.data'
            )
            print("[INFO-15F] Sent data for RoiId={} MetricId={}".format(data.get('RoiId'), data.get('MetricId')))
            
        elif prefix in ('25F', '24J', '25X'):
            self.message_sender.send_message(
                body=json.dumps(body),
                prop=pika.BasicProperties(
                    content_type="application/json"
                ),
                exchange_name='data',
                routing_key='persistent.backhaul.data'
            )
            print("[INFO-25F] Sent data for RoiId={} MetricId={}".format(data.get('RoiId'), data.get('MetricId')))

        time.sleep(0.01)

    def close_connection(self):
        """Close RMQ connection."""
        self.message_sender.stop()
        print("[INFO] RabbitMQ connection closed.")

    def generate_data(self, start, end):
        """Select and send records between given start and end timestamps."""
        self.db_raw_path = "/home/pi/Raspicam/eventRaw.db"
        conn = sqlite3.connect(self.db_raw_path)
        cursor = conn.cursor()

        print("[INFO] Fetching records between {} and {}...".format(start, end))

        cursor.execute('''
            SELECT RegionID, MetricID, PeopleTypeID, PeopleID, EventStartTimeStamp, EventEndTimeStamp, CombineObjectTypeID
            FROM EventRaw
            WHERE EventStartTimeStamp >= ? AND EventStartTimeStamp <= ?
        ''', (start, end))

        rows = cursor.fetchall()

        global totalfoundrows
        totalfoundrows = len(rows)
        
        print("[INFO] Found {} records in range.".format(len(rows)))

        for row in rows:
            data = {
                'RoiId': row[0],
                'MetricId': row[1],
                'PeopleTypeId': row[2],
                'PeopleId': row[3],
                'EventStartTime': row[4],
                'EventEndTime': row[5],
                'CombineObjectTypeId': row[6]
            }
            # print("[DEBUG] Sending data: {}".format(data))
            self.send_persistent_data(data, row[5])  # row[5] = EventEndTimeStamp
            time.sleep(0.01)

        conn.close()
        print("[INFO] Database connection closed.")

    def get_device_data(self):
        self.db_raw_path = "/home/pi/Raspicam/raspicam"
        conn = sqlite3.connect(self.db_raw_path)
        cursor = conn.cursor()

        global deviceinformation, fulldeviceinformation
        
        cursor.execute('SELECT * FROM camera LIMIT 1;')
        row = cursor.fetchone()
        if row:
            fulldeviceinformation = row
            
        cursor.execute('SELECT companyserial FROM camera LIMIT 1;')
        row = cursor.fetchone()
        if row:
            deviceinformation = row[0]

        conn.close()
        print("[INFO] Database connection closed.")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python send_eventraw.py <start_time> <end_time>")
        print('Example: python send_eventraw.py 1761868800 1762127999')
        sys.exit(1)

    start_time = sys.argv[1]
    end_time = sys.argv[2]

    sender = Send_EventRaw()
    sender.get_device_data()
    
    print('[INFO] Device Info: {}'.format(deviceinformation))
    
    prefix = str(deviceinformation)[:3]
    if prefix == '15F':
        print('[INFO] 15F Device Detected')
    elif prefix in ('25F', '24J', '25X'):
        print('[INFO] 25F Device Detected')
    
    try:
        sender.generate_data(start_time, end_time)
    finally:
        sender.close_connection()
        
    dev_serial = "Unknown"
    try:
        with open('/proc/device-tree/serial-number', 'r') as f:
            dev_serial = f.read().strip('\x00').strip()
    except Exception as e:
        dev_serial = "Error reading serial: {}".format(e)

    print('[INFO] Total rows of records found: {}'.format(totalfoundrows))
    print('[INFO] Device Info: {}'.format(fulldeviceinformation))
    print('[INFO] Device Company Serial: {}'.format(deviceinformation))
    print('[INFO] Device Serial: {}'.format(dev_serial))