# -*- coding: utf-8 -*-
from __future__ import print_function
import subprocess
import sys

# --- AUTO-INSTALLER BLOCK ---
try:
    import pika
except ImportError:
    print("[INFO] pika not found. Attempting to install pika==1.1.0 for Python 2...")
    try:
        # Calls the current python executable to install the library
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pika==1.1.0"])
        import pika
        print("[INFO] pika installed successfully.")
    except Exception as e:
        print("[ERROR] Failed to auto-install pika. Please run: sudo pip install pika==1.1.0")
        print("Error details: {}".format(e))
        sys.exit(1)
# ----------------------------

import json
import time
import sqlite3
from datetime import datetime, timedelta
from utility_module import RMQMessageSender, Utility

class UTC(datetime.tzinfo):
    def utcoffset(self, dt): return timedelta(0)
    def tzname(self, dt): return "UTC"
    def dst(self, dt): return timedelta(0)

utc_tz = UTC()
totalfoundrows = 0
deviceinformation = ""
fulldeviceinformation = ""

class Send_EventRaw(RMQMessageSender):
    def __init__(self):
        self.chip_serial = Utility.get_serial()
        self.parameters = pika.ConnectionParameters(
            host='localhost',
            credentials=pika.PlainCredentials('guest', 'guest')
        )
        self.message_sender = RMQMessageSender()
        self.message_sender.init(self.parameters)

    def send_persistent_data(self, data, endtimestamp):
        if isinstance(endtimestamp, (int, float, long)):
            local_dt = datetime.fromtimestamp(float(endtimestamp))
            utc_dt = datetime.fromtimestamp(float(endtimestamp)).replace(tzinfo=utc_tz)
        else:
            try:
                local_dt = datetime.strptime(str(endtimestamp), "%Y-%m-%d %H:%M:%S")
                utc_dt = local_dt.replace(tzinfo=utc_tz)
            except ValueError:
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

        body = {"type": "json", "data": json.dumps(data), "topic": 'ffc-eventrawdata'}
        prefix = str(deviceinformation)[:3]
        
        routing_key = 'backhaul.data' if prefix == '15F' else 'persistent.backhaul.data'
        
        self.message_sender.send_message(
            body=json.dumps(body),
            prop=pika.BasicProperties(content_type="application/json"),
            exchange_name='data',
            routing_key=routing_key
        )
        print("[INFO-{}] Sent data for RoiId={} MetricId={}".format(prefix, data.get('RoiId'), data.get('MetricId')))
        time.sleep(0.01)

    def close_connection(self):
        self.message_sender.stop()
        print("[INFO] RabbitMQ connection closed.")

    def generate_data(self, start, end):
        conn = sqlite3.connect("/home/pi/Raspicam/eventRaw.db")
        cursor = conn.cursor()
        cursor.execute('''
            SELECT RegionID, MetricID, PeopleTypeID, PeopleID, EventStartTimeStamp, EventEndTimeStamp, CombineObjectTypeID
            FROM EventRaw WHERE EventStartTimeStamp >= ? AND EventStartTimeStamp <= ?
        ''', (start, end))
        rows = cursor.fetchall()
        global totalfoundrows
        totalfoundrows = len(rows)
        for row in rows:
            data = {'RoiId': row[0], 'MetricId': row[1], 'PeopleTypeId': row[2], 'PeopleId': row[3],
                    'EventStartTime': row[4], 'EventEndTime': row[5], 'CombineObjectTypeId': row[6]}
            self.send_persistent_data(data, row[5])
        conn.close()

    def get_device_data(self):
        conn = sqlite3.connect("/home/pi/Raspicam/raspicam")
        cursor = conn.cursor()
        global deviceinformation, fulldeviceinformation
        cursor.execute('SELECT * FROM camera LIMIT 1;')
        fulldeviceinformation = cursor.fetchone()
        cursor.execute('SELECT companyserial FROM camera LIMIT 1;')
        row = cursor.fetchone()
        if row: deviceinformation = row[0]
        conn.close()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python test.py <start_time> <end_time>")
        sys.exit(1)

    sender = Send_EventRaw()
    sender.get_device_data()
    try:
        sender.generate_data(sys.argv[1], sys.argv[2])
    finally:
        sender.close_connection()
    
    print('[INFO] Total rows: {}'.format(totalfoundrows))
    print('[INFO] Device Serial: {}'.format(deviceinformation))
