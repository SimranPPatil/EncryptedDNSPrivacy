package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	runtime "github.com/banzaicloud/logrus-runtime-formatter"
	log "github.com/sirupsen/logrus"
	"github.com/teamnsrg/mida/types"
)

type parsedData struct {
	RequestID, LoadURL, LoadDomain, Type, MimeType, RemoteIPAddr, ModTime string
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

	WORKERS := 32

	domainSets := make(map[string]map[string]bool)

	var wg sync.WaitGroup
	var owg sync.WaitGroup

	now := time.Now().Format("01-02-2006")
	last := strings.Split(rootPath, "/")

	owg.Add(1)
	go output(resultChan, now+"_"+last[len(last)-1]+"_output.json", &domainSets, &owg)

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
		// sort.Slice(subdirs, func(i, j int) bool {
		// 	return subdirs[i].ModTime().Before(subdirs[j].ModTime())
		// })

		for _, subdir := range subdirs {
			FileInfo, _ := os.Stat(subdir.Name())
			statT := FileInfo.Sys().(*syscall.Stat_t)

			ModTime := FileInfo.ModTime().Format("01-06-2006")
			log.Info(
				subdir.Name(), "\n",
				FileInfo.ModTime().Format("01-06-2006"), "\n",
				timespecToTime(statT.Atim), "\n",
				timespecToTime(statT.Ctim), "\n",
				timespecToTime(statT.Mtim), "\n")
			log.Info(path.Join(rootPath, dir.Name(), subdir.Name(), "resource_metadata.json"), subdir.ModTime())
			filenameChan <- path.Join(rootPath, dir.Name(), subdir.Name(), "resource_metadata.json")
		}
	}

	close(filenameChan)
	wg.Wait()

	close(resultChan)
	owg.Wait()

	/*
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
	*/

	keys := make([]string, 0, len(domainSets))
	for k := range domainSets {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		fmt.Println(k, len(domainSets[k]))
		//temp := domainSets[k]
	}

	log.Info("End")
}

func timespecToTime(ts syscall.Timespec) string {
	return time.Unix(int64(ts.Sec), int64(ts.Nsec)).Format("01-06-2006")
}

func worker(
	filenameChan chan string,
	resultChan chan parsedData,
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

				FileDir := strings.Replace(fName, "/resource_metadata.json", "", -1)
				FileInfo, _ := os.Stat(FileDir)
				// statT := FileInfo.Sys().(*syscall.Stat_t)

				ModTime := FileInfo.ModTime().Format("01-06-2006")
				// log.Info(
				// 	fName, "\n",
				// 	FileDir, "\n",
				// 	FileInfo.ModTime().Format("01-06-2006"), "\n",
				// 	timespecToTime(statT.Atim), "\n",
				// 	timespecToTime(statT.Ctim), "\n",
				// 	timespecToTime(statT.Mtim), "\n")

				pd := parsedData{
					RequestID:    RequestID.String(),
					LoadURL:      LoadURL,
					LoadDomain:   LoadDomain,
					Type:         Type.String(),
					MimeType:     MimeType,
					RemoteIPAddr: RemoteIPAddress,
					ModTime:      ModTime,
				}

				resultChan <- pd
			}
		}
	}

	wg.Done()
}

func output(
	resultChan chan parsedData,
	ofName string,
	domainSets *map[string]map[string]bool,
	owg *sync.WaitGroup) {

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

		_, found := (*domainSets)[result.ModTime]
		if found == false {
			(*domainSets)[result.ModTime] = make(map[string]bool)
		}
		((*domainSets)[result.ModTime])[result.LoadDomain] = true
	}

	owg.Done()
}
