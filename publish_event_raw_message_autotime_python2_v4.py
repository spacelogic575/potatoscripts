import time
import calendar
import sqlite3
import subprocess
from copy import copy
import sys
import requests
import websocket, base64, json
sys.path.insert(0, '/var/www/cgi-bin')
import setupproxy
import logging_rpi
import serial
from datetime import datetime

class pulsardirect(object):
	def __init__(self):
		self.serveraddress = None
		self.pulsarAddress = None
		self.metaServer = False
		self.pulsarData = None
		self.offset = float((calendar.timegm(time.localtime()) - calendar.timegm(time.gmtime())) / 3600.0)
		self.timestampOffset = int(self.offset * 60 * 60)
		self._LASTROWID = 0  # replaced _LASTTIMESTAMP
		self._LASTTIMESTAMP = 0
		self.serial = serial.getserialN()
		self.LastPushRowId = 0  # replaced LastPushTimestamp
		self.topicPath = '/ws/v2/producer/persistent/public/default/'
		self.topic = 'ffc-eventrawdata'
		self.allowTimeChange = False
		logging_rpi.log.info('Pulsar Direct:::Initialize:::Function Started.')
		self.ws = None
		self.allowedAddress = None
		self.retry = 0

	def getPulsarDetails(self):
		try:
			conn = sqlite3.connect("/home/pi/Raspicam/raspicam")
			c = conn.cursor()
			c.execute('select Server from camera;')
			self.serveraddress = c.fetchone()[0] + "/"
			conn.close()
			print('my address:' + self.serveraddress)

			conn = sqlite3.connect("/home/pi/Raspicam/raspicam")
			c = conn.cursor()
			c.execute("select value from SystemSetting where param = 'PulsarServer';")
			pulsar_data = c.fetchone()[0]
			conn.close()
			data = json.loads(pulsar_data)
			self.pulsarAddress = "ws://" + data["Host"] + ":" + data["Port"]
			self.pulsarData = data
			print('my pulsar address:' + self.pulsarAddress)
			logging_rpi.log.info('Pulsar Direct:::get Pulsar Details:::Address retrieved. My Pulsar Address: ' + self.pulsarAddress)
			self.reconnectPulsar()
		except Exception as e:
			logging_rpi.log.error('Pulsar Direct:::get Pulsar Details:::' + str(e))
			print(e)
			exit(1)


	def reconnectPulsar(self):
		if self.retry > 3:
			print("Exhausted Retry. Sleep for 1 minute...")
			time.sleep(60)
			self.retry = 2
		try:
			if self.ws is not None:
				if self.ws.connected:
					self.ws.close()
		except Exception as e:
			logging_rpi.log.error("reconnectPulsar:::Closing connection failed:::"+str(e))

		try:
			self.ws = websocket.create_connection(self.pulsarAddress + self.topicPath + self.topic)
		except Exception as e:
			logging_rpi.log.error("reconnectPulsar:::Attempt failed:::"+str(e))
			self.fallbackAddress()
			self.retry += 1
			time.sleep(10)
			self.reconnectPulsar()
		print("Connection successful. Sending to: " + self.pulsarAddress + self.topicPath + self.topic)
		self.retry = 0


	def sendDatatoPulsar(self, footfallList):
		try:
			a = json.dumps(footfallList, ensure_ascii=False).encode('utf8').decode('utf8')
			jsd = json.dumps({
				'payload' : base64.b64encode(a.encode('utf-8')).decode('utf-8')
			})
			self.ws.send(jsd)
			res = self.ws.recv()
			if self.ws.connected:
				response = json.loads(res)
				if response['result'] == 'ok':
					print(footfallList)
					print('Data sent!')
					self.allowedAddress = self.pulsarAddress
					self.allowTimeChange = True
				else:
					self.allowTimeChange = False
		except Exception as e:
			print("Exception : " + str(e))
			self.allowTimeChange = False
			logging_rpi.log.error('Pulsar Direct:::send Data to Pulsar:::Exception hit: ' + str(e))
			self.fallbackAddress()
			time.sleep(10)
			self.reconnectPulsar()
			pass


	def getCountingData(self, startTimestamp, endTimestamp):
		connRaspi = sqlite3.connect('/home/pi/Raspicam/counting.db')
		cRaspi = connRaspi.cursor()

		# === MODIFIED QUERY: now use id instead of timestamp ===
		cRaspi.execute("""
			SELECT id, timestamp as EventStartTime, timestamp as EventEndTime,
			       (timestamp + ?) as Localtimestamp, invalue as InValue,
			       outvalue as OutValue, ? as Serial,
			       datetime(timestamp, 'unixepoch', 'localtime') as UploadedLocalDateTime,
			       datetime(timestamp, 'unixepoch') as UploadedUTCDateTime,
			       ? as CameraSerial
			FROM counting
			WHERE timestamp >= ? AND timestamp < ?
			ORDER BY id ASC;
		""", [self.timestampOffset, self.serial, self.serial, startTimestamp, endTimestamp])

		footfallData = [dict((cRaspi.description[i][0], value) \
						for i, value in enumerate(row)) for row in cRaspi.fetchall()]
		formattedDataList = []

		for footfall in footfallData:
			count_in = footfall["InValue"]
			count_out = footfall["OutValue"]
			max_count = count_in + count_out
			lastRowId = footfall["id"]  # replaced lastTimestamp
			localTimestamp = footfall["Localtimestamp"]
			lastTimestamp = footfall["EventStartTime"]
			footfall.pop("InValue")
			footfall.pop("OutValue")
			footfall.pop("Serial")
			footfall.pop("Localtimestamp")

			for count in range(max_count):
				formattedData = copy(footfall)
				formattedData["RoiId"] = 1
				formattedData["PeopleTypeId"] = 1
				formattedData["CombineObjectTypeId"] = 11

				formattedData["EventStartUTCTime"] = datetime.utcfromtimestamp(lastTimestamp).strftime('%Y-%m-%d %H:%M:%S')
				formattedData["EventEndUTCTime"] = datetime.utcfromtimestamp(lastTimestamp).strftime('%Y-%m-%d %H:%M:%S')
				formattedData["EventStartLocalTime"] = datetime.utcfromtimestamp(localTimestamp).strftime('%Y-%m-%d %H:%M:%S')
				formattedData["EventEndLocalTime"] = datetime.utcfromtimestamp(localTimestamp).strftime('%Y-%m-%d %H:%M:%S')

				if count_in >= 1:
					formattedData["MetricId"] = 1
					formattedData["PeopleId"] = (count + 1)
					count_in -= 1
				else:
					formattedData["MetricId"] = 2
					formattedData["PeopleId"] = (count + 1)

				formattedDataList.append(formattedData)

		if formattedDataList:
			self.LastPushRowId = lastRowId
			print(str(self.LastPushRowId))
			print('my retrieved data: ' + str(json.dumps(formattedDataList)))
			if self.LastPushRowId != 0:
				for data in formattedDataList:
					self.sendDatatoPulsar(data)
		else:
			print('No Data retrieved')
		connRaspi.close()


	def fallbackAddress(self):
		# first try wss with normal port
		# then try wss with 443 port
		# then ip number
		# then cycle back

		# If it already has a confirmed connection, then use it instead of cycling through it 
		# If this fails, then it's good as having no fallback -- ask client to change their stuff
		try:
			if self.allowedAddress is not None:
				self.pulsarAddress = self.allowedAddress
				return True

			if self.pulsarData is not None:
				print("Attempt with secure connection wss...")
				if "ws://" in self.pulsarAddress and self.pulsarData["Port"] in self.pulsarAddress:
					self.pulsarAddress = "wss://" + self.pulsarData["Host"] + ":" + self.pulsarData["SecurePort"]
				elif "wss://" in self.pulsarAddress and self.pulsarData["SecurePort"] in self.pulsarAddress:
					self.pulsarAddress = "ws://" + self.pulsarData["Host"] + ":" + self.pulsarData["Port"]
				print("Changed to: " + self.pulsarAddress)
		except Exception as e:
			logging_rpi.log.error("lo_pulsardirect.py:::falbackAddress:::"+str(e))


if __name__ == "__main__":
	startTimestamp = sys.argv[1]
	endTimestamp = sys.argv[2]
	pulsardirect = pulsardirect()
	pulsardirect.getPulsarDetails()
	pulsardirect.getCountingData(startTimestamp, endTimestamp)
