package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"sync"
	"time"

	runtime "github.com/banzaicloud/logrus-runtime-formatter"
	log "github.com/sirupsen/logrus"
	"github.com/teamnsrg/mida/types"
)

type parsedData struct {
	RequestID, LoadURL, LoadDomain, Type, MimeType, RemoteIPAddr string
}

func init() {
	formatter := runtime.Formatter{ChildFormatter: &log.TextFormatter{}}
	formatter.Line = true
	log.SetFormatter(&formatter)
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
}

func main() {

	if len(os.Args) != 2 {
		log.Error("Usage: ./parser path/to/folder_with_sites")
		return
	}

	log.Info("Start")

	filenameChan := make(chan string)
	resultChan := make(chan parsedData)

	rootPath := os.Args[1]

	WORKERS := 5

	var wg sync.WaitGroup
	var owg sync.WaitGroup

	now := time.Now().Format("01-02-2006")
	owg.Add(1)
	go output(resultChan, now+"_output.json", &owg)

	for i := 0; i < WORKERS; i++ {
		wg.Add(1)
		go worker(filenameChan, resultChan, &wg)
	}

	dirs, err := ioutil.ReadDir(rootPath)
	if err != nil {
		log.Error(err)
		return
	}

	for _, dir := range dirs {
		pathSub := path.Join(rootPath, dir.Name())
		subdirs, err := ioutil.ReadDir(pathSub)
		if err != nil {
			log.Error(err, pathSub)
			continue
		}

		for _, subdir := range subdirs {
			filenameChan <- path.Join(rootPath, dir.Name(), subdir.Name(), "resource_metadata.json")
		}
	}

	close(filenameChan)
	wg.Wait()

	close(resultChan)
	owg.Wait()

	log.Info("End")
}

func worker(filenameChan chan string, resultChan chan parsedData, wg *sync.WaitGroup) {

	for fName := range filenameChan {

		data, e := ioutil.ReadFile(fName)
		if e != nil {
			fmt.Printf("File error: %v\n", e)
			os.Exit(1)
		}

		var resources map[string]types.Resource

		err := json.Unmarshal(data, &resources)
		if err != nil {
			log.Error(err)
		}

		for k := range resources {

			Responses := resources[k].Responses

			for _, response := range Responses {

				RequestID := response.RequestID
				MimeType := response.Response.MimeType
				Type := response.Type
				RemoteIPAddress := response.Response.RemoteIPAddress

				LoadURL := response.Response.URL
				u, err := url.Parse(LoadURL)
				if err != nil {
					log.Error(err)
				}
				LoadDomain := u.Host

				pd := parsedData{
					RequestID:    RequestID.String(),
					LoadURL:      LoadURL,
					LoadDomain:   LoadDomain,
					Type:         Type.String(),
					MimeType:     MimeType,
					RemoteIPAddr: RemoteIPAddress,
				}

				resultChan <- pd
			}
		}
	}

	wg.Done()
}

func output(resultChan chan parsedData, ofName string, owg *sync.WaitGroup) {

	f, err := os.Create(ofName)
	if err != nil {
		log.Error(err)
		return
	}

	for result := range resultChan {
		b, _ := json.Marshal(result)
		_, err = fmt.Fprintln(f, string(b))
		if err != nil {
			log.Error(err)
		}
	}

	owg.Done()
}
