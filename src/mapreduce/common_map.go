package mapreduce

import (
	//"bytes"
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"os"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	// TODO:
	// You will need to write this function.
	// You can find the filename for this map task's input to reduce task number
	// r using reduceName(jobName, mapTaskNumber, r). The ihash function (given
	// below doMap) should be used to decide which file a given key belongs into.
	//
	// The intermediate output of a map task is stored in the file
	// system as multiple files whose name indicates which map task produced
	// them, as well as which reduce task they are for. Coming up with a
	// scheme for how to store the key/value pairs on disk can be tricky,
	// especially when taking into account that both keys and values could
	// contain newlines, quotes, and any other character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
	var err error
	contents, err := ioutil.ReadFile(inFile)
	if err != nil {
		panic(err)
	}
	// calling the user defined map function. Returning array of key-value pairs.
	keyVals := mapF(inFile, string(contents))
	// Find the hash of the key,
	// used to partition the map output to nReduce Files.
	getReduceFile := func(key string) string {
		hashVal := int(ihash(key))
		// find out to which reducer the output should be partitioned to.
		r := hashVal % nReduce
		// get the name of the reducer file to which the output has to be written to.
		return reduceName(jobName, mapTaskNumber, r)
	}
	// map containing filename-filepointer pair.
	// Used to avoid system calls to open files for every key-value pair of map output.
	filePool := make(map[string]*os.File)
	// file pointer.
	var file *os.File
	var ok bool
	for _, kv := range keyVals {
		// partition the map output based on the key.
		// hash value is used to find to which reducer the key belongs to.
		reduceFile := getReduceFile(kv.Key)

		if file, ok = filePool[reduceFile]; !ok {
			// open file only it doesn't exist in file pool.
			file, err = os.OpenFile(reduceFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
			if err != nil {
				panic(err)
			}
			// insert it into file pool.
			filePool[reduceFile] = file

		}
		// encode and write it into the file.
		enc := json.NewEncoder(file)
		err = enc.Encode(&kv)
	}
	var poolCount int
	// iterate through the pool of file pointers and close them.
	for fileName := range filePool {
		poolCount++
		filePool[fileName].Close()
	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
