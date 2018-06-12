import glob, json
from gzip import GzipFile
import xml.etree.ElementTree as etree
from pprint import pprint

# read all filenames from the subtitles directory (only Frisian for now)
file_names = glob.glob("subtitles/**/*.xml.gz", recursive=True)

# wordcount dictionary
words_dict = {}

for file_name in file_names:
    # unzip stream
    f = GzipFile(file_name)

    # iterative parse xml per element
    xml_iter = iter(etree.iterparse(f, events=('start', 'end')))

    # get and count word
    for event, element in xml_iter:
        if event == 'start' and element.tag == 'w':
            if element.text in words_dict:
                words_dict[element.text] += 1
            else:
                words_dict[element.text] = 1

print(f"{len(words_dict)} words")

# write wordcount dictionary to json file
with open('words.json', 'w') as outfile:
    json.dump(words_dict, outfile)

