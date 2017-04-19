import json
import os

if __name__ == "__main__":
	#parsed_json = json.loads(open("yelp_dataset/business.json").read())

	data = []
	with open("../yelp_dataset/yelp_academic_dataset_business.json") as f:
		for line in f:
			data.append(json.loads(line))
	
	tmpStr = '{"type": "Feature","geometry": {"type": "Point","coordinates": [125.6, 10.1]},"properties": {"name": "Dinagat Islands"}}'
	with open('../yelp_dataset/business-geo.json', 'w') as outfile:
		outfile.write('{"type": "FeatureCollection", "features": [\n')

		firstLine = True
		for o in data:
			if not firstLine:
				outfile.write(',\n')
			else:
				firstLine = False
			tmp = json.loads(tmpStr)
			tmp["geometry"]["coordinates"][1] = o["latitude"]
			tmp["geometry"]["coordinates"][0] = o["longitude"]

			tmp["properties"]["name"] = o["name"]
			tmp["properties"]["stars"] = o["stars"]

			json.dump(tmp, outfile)
			#outfile.write('\n')

		outfile.write(']}')

	print("## End of program")