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
	name = record[0]
	row = record[1]
	col = record[2]
	val = record[3]
	if name == "a":
		for i in range(0,5):
			mr.emit_intermediate((row,i), (col,val))
	elif name == "b":
		for i in range(0,5):
			mr.emit_intermediate((i,col), (row,val))
		

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
	total = 0
	current = -1
	cal = 0
	for v in sorted(list_of_values):
		if v[0] == current:
			cal = cal * v[1]
			total += cal
		else:
			current = v[0]
			cal = v[1]	
	mr.emit((key[0], key[1], total))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
