import json
import os

if __name__ == "__main__":
	#parsed_json = json.loads(open("yelp_dataset/business.json").read())

	data = []
	with open("yelp_dataset/user.json") as f:
		for line in f:
			data.append(json.loads(line))
	
	#tmpStr = '{"type": "Feature","geometry": {"type": "Point","coordinates": [125.6, 10.1]},"properties": {"name": "Dinagat Islands"}}'
	with open('yelp_dataset/user-graph.json', 'w') as outfile:

		for o in data:
			
			for friend in o["friends"]:
				if friend != 'None':
					outfile.write(o["user_id"]+' '+friend+'\n')




		
	print("## End of program")