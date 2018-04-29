import glob, json
from gzip import GzipFile
import xml.etree.ElementTree as etree
from pprint import pprint


file_names = glob.glob("subtitles/**/*.xml.gz", recursive=True)
words_dict = {}

for file_name in file_names:
    f = GzipFile(file_name)
    xml_iter = iter(etree.iterparse(f, events=('start', 'end')))

    for event, element in xml_iter:
        if event == 'start' and element.tag == 'w':
            if element.text in words_dict:
                words_dict[element.text] += 1
            else:
                words_dict[element.text] = 1

print(f"{len(words_dict)} words")

with open('fr.json', 'w') as outfile:
    json.dump(words_dict, outfile)

