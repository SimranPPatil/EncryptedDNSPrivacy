package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	runtime "github.com/banzaicloud/logrus-runtime-formatter"
	"github.com/go-echarts/go-echarts/charts"
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

	if len(os.Args) != 3 {
		log.Error("Usage: ./parser path/to/folder_with_sites 10-01-2019")
		return
	}

	log.Info("Start")

	filenameChan := make(chan string)
	resultChan := make(chan parsedData)
	singlefreqChan := make(chan string)

	rootPath := os.Args[1]

	WORKERS := 32

	var wg sync.WaitGroup
	var owg sync.WaitGroup
	var fwg sync.WaitGroup

	now := time.Now().Format("01-02-2006")
	last := strings.Split(rootPath, "/")

	singlefreq := map[string]int{}
	allfreq := map[string]int{}

	owg.Add(1)
	go output(resultChan, now+"_"+last[len(last)-1]+"_output.json", &owg, &allfreq)

	fwg.Add(1)
	go frequency(singlefreqChan, &fwg, &singlefreq)

	for i := 0; i < WORKERS; i++ {
		wg.Add(1)
		go worker(filenameChan, resultChan, singlefreqChan, &wg)
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

	close(singlefreqChan)
	fwg.Wait()

	close(resultChan)
	owg.Wait()

	// get domains from allfreq and plot for both frequencies for these domain
	single := []int{}
	multiple := []int{}
	domains := []string{}

	bar := charts.NewBar()
	for domain := range allfreq {
		_, found := singlefreq[domain]
		if found {
			domains = append(domains, domain)
			single = append(single, singlefreq[domain])
			multiple = append(multiple, allfreq[domain])
		}
	}
	bar.SetGlobalOptions(charts.TitleOpts{Title: "Domain frequency over time comparison"})
	bar.AddXAxis(domains).AddYAxis("single", single).AddYAxis("over time", multiple)
	graph, err := os.Create("bar.html")
	if err != nil {
		log.Error("plotting error")
	}
	bar.Render(graph)

	log.Info("End")
}

func worker(
	filenameChan chan string,
	resultChan chan parsedData,
	singlefreqChan chan string,
	wg *sync.WaitGroup) {

	for fName := range filenameChan {
		data, e := ioutil.ReadFile(fName)
		if e != nil {
			fmt.Printf("File error: %v\n", e)
			continue
		}

		var resources map[string]types.Resource

		err := json.Unmarshal(data, &resources)
		if err != nil {
			log.Error(err)
			continue
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
					log.Info(strings.Join(strings.Split(LoadURL, "/")[:3], "/"))
					u, err = url.Parse(strings.Join(strings.Split(LoadURL, "/")[:3], "/"))
					if err != nil {
						log.Error(err)
						continue
					}
				}
				LoadDomain := u.Host

				FileInfo, _ := os.Stat(fName)
				if FileInfo.ModTime().Format("01-06-2006") == os.Args[2] {
					singlefreqChan <- LoadDomain
				}

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

func frequency(
	singlefreqChan chan string,
	fwg *sync.WaitGroup,
	singlefreq *map[string]int) {

	for domain := range singlefreqChan {
		_, found := (*singlefreq)[domain]
		if found == true {
			(*singlefreq)[domain]++
		} else {
			(*singlefreq)[domain] = 1
		}
	}

	fwg.Done()
}

func output(
	resultChan chan parsedData,
	ofName string,
	owg *sync.WaitGroup,
	allfreq *map[string]int) {

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
			continue
		}

		_, found := (*allfreq)[result.LoadDomain]
		if found == true {
			(*allfreq)[result.LoadDomain]++
		} else {
			(*allfreq)[result.LoadDomain] = 1
		}
	}

	owg.Done()
}
