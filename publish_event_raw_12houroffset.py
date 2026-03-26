import json
import sys
import time
from secrets import token_hex
from utility_module import RMQMessageSender, Utility
import pika
import sqlite3
from datetime import datetime, timezone, timedelta

totalfoundrows = 0
deviceinformation = ""
fulldeviceinformation = ""

# --- THE FIX ---
# The 12-hour shift destroys the curve. Set to 0 to push absolute pure DB time.
HOURS_OFFSET = 0 
# ---------------

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

    def send_persistent_data(self, data, original_epoch):
        """Send single record to RMQ with absolute timezone bypass."""
        
        # 1. Convert the original epoch to an absolute UTC datetime object
        utc_dt = datetime.fromtimestamp(float(original_epoch), tz=timezone.utc)
        
        # 2. Apply the shift (Now 0 hours, passing pure data)
        shifted_dt = utc_dt + timedelta(hours=HOURS_OFFSET)
        
        # 3. Create a perfect string representation
        time_str = shifted_dt.strftime('%Y-%m-%d %H:%M:%S')

        # 4. Force all strings in the payload to match perfectly
        data.update({
            'CameraSerial': self.chip_serial,
            'UploadedLocalDateTime': time_str,
            'UploadedUTCDateTime': time_str,
            'EventStartLocalTime': time_str,
            'EventStartUTCTime': time_str,
        })

        body = {
            "type": "json",
            "data": json.dumps(data),
            "topic": 'ffc-eventrawdata'
        }
        
        if deviceinformation[:3] == '15F':
            self.message_sender.send_message(
                body=json.dumps(body),
                prop=pika.BasicProperties(content_type="application/json"),
                exchange_name='data',
                routing_key='backhaul.data'
            )
            print(f"[INFO-15F] Sent data for {time_str}")
            
        elif deviceinformation[:3] in ('25F', '24J', '25X'):
            self.message_sender.send_message(
                body=json.dumps(body),
                prop=pika.BasicProperties(content_type="application/json"),
                exchange_name='data',
                routing_key='persistent.backhaul.data'
            )
            print(f"[INFO-25F] Sent data for {time_str}")

        time.sleep(0.01)

    def close_connection(self):
        self.message_sender.stop()
        print("[INFO] RabbitMQ connection closed.")

    def generate_data(self, start, end):
        self.db_raw_path = "/home/pi/Raspicam/eventRaw.db"
        conn = sqlite3.connect(self.db_raw_path)
        cursor = conn.cursor()

        offset_seconds = HOURS_OFFSET * 3600
        search_start = str(int(start) - offset_seconds)
        search_end = str(int(end) - offset_seconds)

        print(f"[INFO] Seeking delayed DB records between {search_start} and {search_end}...")

        cursor.execute('''
            SELECT RegionID, MetricID, PeopleTypeID, PeopleID, EventStartTimeStamp, EventEndTimeStamp, CombineObjectTypeID
            FROM EventRaw
            WHERE EventStartTimeStamp >= ? AND EventStartTimeStamp <= ?
        ''', (search_start, search_end))

        rows = cursor.fetchall()
        global totalfoundrows
        totalfoundrows = len(rows)
        
        print(f"[INFO] Found {len(rows)} records. Applying explicit {HOURS_OFFSET}h shift to payload...")

        for row in rows:
            shifted_start_epoch = int(float(row[4])) + offset_seconds
            shifted_end_epoch = int(float(row[5])) + offset_seconds

            data = {
                'RoiId': row[0],
                'MetricId': row[1],
                'PeopleTypeId': row[2],
                'PeopleId': row[3],
                'EventStartTime': shifted_start_epoch,
                'EventEndTime': shifted_end_epoch,
                'CombineObjectTypeId': row[6]
            }
            self.send_persistent_data(data, row[5]) 

        conn.close()

    def get_device_data(self):
        self.db_raw_path = "/home/pi/Raspicam/raspicam"
        conn = sqlite3.connect(self.db_raw_path)
        cursor = conn.cursor()
        global deviceinformation, fulldeviceinformation
        
        cursor.execute('SELECT * FROM camera;')
        for row in cursor.fetchall():
            fulldeviceinformation = row
            
        cursor.execute('SELECT companyserial FROM camera;')
        for row in cursor.fetchall():
            deviceinformation = row[0]

        conn.close()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 send_eventraw.py <start_time> <end_time>")
        sys.exit(1)

    start_time = sys.argv[1]
    end_time = sys.argv[2]

    sender = Send_EventRaw()
    sender.get_device_data()
    
    try:
        sender.generate_data(start_time, end_time)
    finally:
        sender.close_connection()
        
    dev_serial = "Unknown"
    try:
        with open('/proc/device-tree/serial-number', 'r') as f:
            dev_serial = f.read().strip('\x00').strip()
    except Exception as e:
        pass

    print(f'[INFO] Total rows of records found: {totalfoundrows}')
    print(f'[INFO] Device Company Serial: {deviceinformation}')
