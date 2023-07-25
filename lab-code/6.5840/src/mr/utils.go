package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

func ReadFileAsString(fileName string) string {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("can not open file %v", fileName)
	}
	tmpContents, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("can not read file %s", fileName)
	}
	file.Close()
	return string(tmpContents)
}

func WriteKeyValueToFile(kvArray []KeyValue, outputFileName string) {
	outputFile, err := os.OpenFile(outputFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	defer outputFile.Close()
	if err != nil {
		log.Fatalf("write map tmp file to %s failed", outputFileName)
	}
	for _, kv := range kvArray {
		fmt.Fprintf(outputFile, "%v %v\n", kv.Key, kv.Value)
	}
}

func DeleteFile(fileName string) {
	err := os.Remove(fileName)
	if err != nil {
		log.Println("can not remove file " + fileName)
	}
}

func DeleteFiles(files []string) {
	for _, fileName := range files {
		DeleteFile(fileName)
	}
}

func RenameFiles(files []string) {
	for _, fileName := range files {
		nameArray := strings.Split(fileName, "-")
		length := len(nameArray)
		if length > 1 {
			nameArray = nameArray[0 : length-1]
			newFileName := strings.Join(nameArray, "-")
			os.Rename(fileName, newFileName)
		}
	}
}

func CheckAllStageTasksFinished(remainTasksNum int, tasksInMonitor int, errorMessage string) {
	if remainTasksNum != 0 {
		log.Fatalf("There are still %d tasks left to be processed. %s", remainTasksNum, errorMessage)
	}
	if tasksInMonitor != 0 {
		log.Fatalf("There are still %d tasks left in monitor. %s", tasksInMonitor, errorMessage)
	}
}

func (c *Coordinator) WaitAllTasksFinished(waitNum int) {
	c.waitGroup.Add(waitNum)
	c.waitGroup.Wait()
}

func (c *Coordinator) CleanIntermediateFiles() {
	// clean failed map or reduce tasks
	filesAndDir, _ := os.ReadDir("./")
	for _, file := range filesAndDir {
		if !file.IsDir() &&
			(strings.HasSuffix(file.Name(), "tmp") ||
				strings.HasPrefix(file.Name(), c.temporaryPath)) {
			DeleteFile(file.Name())
		}
	}
}
