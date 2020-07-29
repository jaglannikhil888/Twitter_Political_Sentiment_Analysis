import urllib3
from twisted.internet.protocol import Factory, Protocol
import mysql.connector
from mysql.connector import Error
import tweepy
import json
from dateutil import parser
import time
import os
from http.client import IncompleteRead

import ssl






consumer_key = "Vt43tIrwitoHKF1U9QsqF8cEw"
consumer_secret = "YDMoR1JBS35EXBaHF4lueS64IZLwKQueuJ6sfwzL3c9YEd8wLU"
access_token = "1223126275524481025-hGLxtL9YIyO35J1YYgoiLYxniIqiAy"
access_token_secret = "ifdE6E3pVGXK9bdLtYmhMh0Dczn5rTeF0AWBTBLGPT9QO"
password = ""


def connect(username, created_at, tweet, location):
	
	"""
	connect to MySQL database and insert twitter data
	"""
	
	try:
		con = mysql.connector.connect(host = 'localhost',
		database='twitterdb', user='root', password = 'vaibhav09', charset = 'utf8')
		
		if con.is_connected():
			"""
			Insert twitter data
			"""
			cursor = con.cursor()
			
			
			query = "INSERT INTO tweets (username, created_at, tweet, location) VALUES (%s, %s, %s, %s)"
			cursor.execute(query, (username, created_at, tweet, location))
			con.commit()
			
			
	except Error as e:
		print(e)
	
	
	
		

	cursor.close()
	con.close()

	return


# Tweepy class to access Twitter API
class Streamlistener(tweepy.StreamListener):
	

	def on_connect(self):
		print("You are connected to the Twitter API")


	def on_error(self):
		if status_code != 200:  #200 code means normal execution
			print("error found")
		
			return False  #disconnect from stream

	"""
	This method reads in tweet data as Json
	and extracts the data we want.
	"""
	def on_data(self,data):
	
	
		
        
		try:
			raw_data = json.loads(data)
			

			if 'text' in raw_data :
				 
				
				username = raw_data['user']['screen_name']
				created_at = parser.parse(raw_data['created_at'])
				tweet = raw_data['text']
				
				

				location = raw_data['user']['location']

				#insert into MySQL database
				connect(username, created_at, tweet, location)
				print("Tweet colleted at: {} ".format(str(created_at)))
		except Error as e:
			print(e)
			
		
			


if __name__== '__main__':
	


	# authentication so we can access twitter
	auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_token_secret)
	api =tweepy.API(auth, wait_on_rate_limit=True)

	#instance of Streamlistener
	listener = Streamlistener(api = api)
	stream = tweepy.Stream(auth, listener = listener)
	
	track = ['Narendra Modi', 'BJP', 'Amit Shah', 'JP Nadda', 'PMO India', 'Yogi Adityanath', 'Nirmala Sitharaman', 'Arun Jaithley', 'Centre Government of India', 'Sambit Patra']   #For party in power at the centre 
	#track=['Rahul Gandhi', 'Sonia Gandhi', 'Congreaa', 'INC', 'Shashi Tharoor', 'Randeep Singh Sujrewala', 'Adhir Ranjan Chowdhury', 'P Chidambaram', 'Sachin Pilot', 'Alka Lamba']  #For opposition  
	
	while True:
		try:
			stream.filter(track = track,languages=['en'])
		except IncompleteRead:
			continue
			
		except urllib3.exceptions.ProtocolError:
			continue
			
	

	