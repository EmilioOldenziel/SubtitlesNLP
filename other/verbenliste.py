import json
from pprint import pprint

with open('verben.json') as f:
    data = json.load(f)

verben = []
for l in data:
    verben += l['r']
verben  = list(set(verben))

verben.sort()

print(len(verben))

fh = open("verben_list.txt","w") 
fh.writelines(map(lambda v: v + '\n', verben))
fh.close() 