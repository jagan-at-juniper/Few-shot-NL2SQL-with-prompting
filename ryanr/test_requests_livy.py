import json, pprint, requests, textwrap
host = "http://ec2-3-93-213-4.compute-1.amazonaws.com:8998"
# data = {'kind': 'pyspark'}
headers = {'Content-Type': 'application/json'}
# r = requests.post(host + '/sessions', data=json.dumps(data), headers=headers)
# print(r.json())


# data = {'kind': 'pyspark'}
#
# r = requests.post(host + '/sessions', data=json.dumps(data), headers=headers)
# r.json()


data = {
  'code': textwrap.dedent("""

    """)
}

r = requests.post(host, data=json.dumps(data), headers=headers)
pprint.pprint(r.json())