import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    # key = record[0]
	value = record[1]
	mr.emit_intermediate(value, record)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    item = [[],[]]
    for v in list_of_values:
    	if(v[0]=="order"):
    		item[0].append(v)
    	elif(v[0]=="line_item"):
    		item[1].append(v)
    if(len(item[0])>0 and len(item[1])>0):
    	for a in item[0]:
    		for b in item[1]:
    			mr.emit(a+b)
    			
# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
