package mapreduce

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	decoders := make([]*json.Decoder, nMap)
	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTask)
		fd, err := os.OpenFile(fileName, os.O_RDONLY, 0600)
		if err != nil {
			// logFatal(jobName, reducePhase, reduceTaskNumber, fmt.Sprintf("Failed to open: %s", fileName))
			log.Panicf(jobName, reducePhase, reduceTask, fmt.Sprintf("Failed to open: %s", fileName))
			return
		}
		decoders[i] = json.NewDecoder(fd)
		defer fd.Close()
	}

	kvs := make(map[string][]string)

	for i := 0; i < nMap; i++ {
		var kv *KeyValue
		for {
			err := decoders[i].Decode(&kv)
			if err != nil {
				break
			}
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
		}
	}

	var keys []string

	// get keys from map
	for k := range kvs {
		keys = append(keys, k)
	}

	sort.Strings(keys)
	// Create output file
	fd, err := os.OpenFile(outFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		log.Panicf(jobName, reducePhase, reduceTask, fmt.Sprintf("Failed to open: %s", outFile))
		return
	}
	defer fd.Close()
	// Apply reduce f() and write results
	encoder := json.NewEncoder(fd)
	for _, key := range keys {
		encoder.Encode(KeyValue{key, reduceF(key, kvs[key])})
	}

}
