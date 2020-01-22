package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

type parsedData struct {
RequestID, LoadURL, LoadDomain, Type, MimeType, RemoteIPAddr string
}

func main() {
	log.Info("Start")

	filenameChan := make(chan string)
	resultChan := make(chan parsedData)

	if len(os.Args) != 2 {
		log.Error("Usage: ./parser path/to/site")
		return
	}

	rootPath := os.Args[1]

	dirs, err := ioutil.ReadDir(rootPath)
	if err != nil {
		log.Error(err)
		return
	}

	WORKERS := 5

	var wg sync.WaitGroup
	var owg sync.WaitGroup

	owg.Add(1)
	go output(resultChan, "output.json", &owg)

	for i:=0; i<WORKERS; i++ {
		wg.Add(1)
		go worker(filenameChan, resultChan, &wg)
	}

	for _, dir := range dirs {
		filenameChan <- path.Join(rootPath, dir.Name(), "resource_metadata.json")
	}


	/*
	outfile, err := os.OpenFile("test.json", os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalf("failed opening file: %s", err)
	}
	defer outfile.Close()
	 */

	close(filenameChan)
	wg.Wait()

	close(resultChan)
	owg.Wait()

	log.Info("End")
}


func worker(filenameChan chan string, resultChan chan parsedData, wg *sync.WaitGroup) {

	for fName := range filenameChan {

		file, e := ioutil.ReadFile(fName)
		if e != nil {
			fmt.Printf("File error: %v\n", e)
			os.Exit(1)
		}

		myJSON := string(file)

		m, ok := gjson.Parse(myJSON).Value().(map[string]interface{})
		if !ok {
			fmt.Println("Error")
		}

		for requestID, v := range m {
			responses := v.(map[string]interface{})["responses"].([]interface{})
			for _, entry := range responses {
				loadURL := entry.(map[string]interface{})["response"].(map[string]interface{})["url"]
				loadurl := fmt.Sprintf("%v", loadURL)
				u, err := url.Parse(loadurl)
				if err != nil {
					panic(err)
				}
				loadDomain := u.Host
				Type := entry.(map[string]interface{})["type"]
				mimeType := entry.(map[string]interface{})["response"].(map[string]interface{})["mimeType"]
				remoteIPAddress := entry.(map[string]interface{})["response"].(map[string]interface{})["remoteIPAddress"]
				// siteURL := entry.(map[string]interface{})["response"].(map[string]interface{})["requestHeaders"]
				// siteurl := fmt.Sprintf("%v", siteURL)
				// u, err = url.Parse(siteurl)
				// if err != nil {
				// 	panic(err)
				// }
				// siteDomain := u.Host
				// fmt.Println("siteURL: ", siteURL)
				// fmt.Println("siteDomain: ", siteDomain)
				datatype := fmt.Sprintf("%v", Type)
				datamimeType := fmt.Sprintf("%v", mimeType)
				ip := fmt.Sprintf("%v", remoteIPAddress)

				pd := parsedData{
					RequestID:    requestID,
					LoadURL:      loadurl,
					LoadDomain:   loadDomain,
					Type:         datatype,
					MimeType:     datamimeType,
					RemoteIPAddr: ip,
				}

				resultChan <- pd

				/*
				b, _ := json.Marshal(pd)
				fmt.Println(string(b))
				len, err := outfile.WriteString(string(b) + "\r\n")
				if err != nil {
					log.Fatalf("failed writing to outfile: %s", err)
				}
				fmt.Printf("\nLength: %d bytes", len)
				fmt.Printf("\noutfile Name: %s", outfile.Name())
				*/
			}
		}

	}

	wg.Done()
}


func output (resultChan chan parsedData, ofName string, owg *sync.WaitGroup) {

	f, err := os.Create(ofName)
	if err != nil {
		log.Error(err)
		return
	}

	for result := range resultChan {
		b, _ := json.Marshal(result)
		_, err = f.WriteString(string(b) + "\r\n")
		if err != nil {
			log.Error(err)
		}
	}

	owg.Done()

}
