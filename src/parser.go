package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	runtime "github.com/banzaicloud/logrus-runtime-formatter"
	"github.com/go-echarts/go-echarts/charts"
	log "github.com/sirupsen/logrus"
	"github.com/teamnsrg/mida/types"
)

type parsedData struct {
	RequestID, Site, LoadURL, LoadDomain, Type, MimeType, RemoteIPAddr, ModTime string
}

type fileInformation struct {
	siteName, fileName, fileCTime string
}

func init() {
	formatter := runtime.Formatter{ChildFormatter: &log.TextFormatter{}}
	formatter.Line = true
	log.SetFormatter(&formatter)
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
}

type kv struct {
	Key   string
	Value int
}

func rankbyDomainCount(domainCounts map[string]int) ([]string, []float64) {

	var ss []kv
	var totalOccurrences = 0
	domains := make([]string, 0, len(domainCounts))
	counters := make([]float64, 0.0, len(domainCounts))
	for k, v := range domainCounts {
		totalOccurrences += v
		ss = append(ss, kv{k, v})
	}

	sort.Slice(ss, func(i, j int) bool {
		return ss[i].Value > ss[j].Value
	})

	for _, kv := range ss {
		domains = append(domains, kv.Key)
		counters = append(counters, float64(kv.Value)/float64(totalOccurrences))
	}

	return domains, counters
}

func getAlexaSites(csvfilename string) map[string]bool {
	csvfile, err := os.Open(csvfilename)
	alexaTop := make(map[string]bool)
	if err != nil {
		log.Error("Couldn't open the csv file", err)
	}

	reader := csv.NewReader(bufio.NewReader(csvfile))

	for {
		line, error := reader.Read()
		if error == io.EOF {
			break
		} else if error != nil {
			log.Fatal(error)
		}
		log.Info("line: ", line)
		if line[0] == "100" {
			break
		}
		alexaTop[line[1]] = true
	}

	return alexaTop
}

func main() {

	if len(os.Args) != 2 {
		log.Error("Usage: ./parser path/to/folder_with_sites")
		return
	}

	log.Info("Start")

	alexaTop := getAlexaSites("../input/alexa.csv")
	filenameChan := make(chan fileInformation)
	resultChan := make(chan parsedData)

	rootPath := os.Args[1]

	WORKERS := 32

	domainSets := make(map[string]map[string]map[string]bool)
	siteToDomains := make(map[string]map[string]int)

	var wg sync.WaitGroup
	var owg sync.WaitGroup

	now := time.Now().Format("01-02-2006")
	last := strings.Split(rootPath, "/")

	owg.Add(1)
	go output(resultChan, now+"_"+last[len(last)-1]+"_output.json", &domainSets, &siteToDomains, &owg)

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
			fileInfo := fileInformation{
				siteName:  dir.Name(),
				fileName:  path.Join(rootPath, dir.Name(), subdir.Name(), "resource_metadata.json"),
				fileCTime: strings.Split(subdir.ModTime().String(), " ")[0],
			}
			filenameChan <- fileInfo
		}
	}

	close(filenameChan)
	wg.Wait()

	close(resultChan)
	owg.Wait()

	// Graphs and intermediate output files -->
	GraphFolderPath := "../output/"
	if _, err := os.Stat(GraphFolderPath); os.IsNotExist(err) {
		log.Info("Creating: ", GraphFolderPath)
		os.Mkdir(GraphFolderPath, 0777)
	}

	siteToDomainscsv, err := os.Create("../output/siteToDomains_" + now + "_" + last[len(last)-1] + ".csv")
	if err != nil {
		log.Error("Cannot create file: ", err)
	}
	defer siteToDomainscsv.Close()
	writer := csv.NewWriter(siteToDomainscsv)
	defer writer.Flush()

	for site := range siteToDomains {
		_, found := alexaTop[site]

		if found {
			graphName := path.Join(GraphFolderPath, now+"_"+last[len(last)-1]+"_bar"+"_plf_fraction_"+site+"_.html")
			log.Info("Creating graph: ", graphName)
			domainCounts := siteToDomains[site]
			domains, counters := rankbyDomainCount(domainCounts)

			bar := charts.NewBar()
			bar.SetGlobalOptions(charts.TitleOpts{Title: site, Bottom: "0%"}, charts.ToolboxOpts{Show: false})
			bar.AddXAxis(domains).AddYAxis("Fraction of PLF per domain", counters)
			bar.XYReversal()
			graph, err := os.Create(graphName)
			if err != nil {
				log.Error("plotting error")
			}
			bar.Render(graph)
		}

		for domain, counter := range siteToDomains[site] {
			siteToDomainscsvEntry := []string{site, domain, strconv.Itoa(counter)}
			err := writer.Write(siteToDomainscsvEntry)
			if err != nil {
				log.Error("Cannot write to siteToDomain csv file", err)
			}
		}
	}

	log.Info("End")
}

func worker(
	filenameChan chan fileInformation,
	resultChan chan parsedData,
	wg *sync.WaitGroup) {

	for fName := range filenameChan {
		data, e := ioutil.ReadFile(fName.fileName)
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
				Site := fName.siteName
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

				pd := parsedData{
					RequestID:    RequestID.String(),
					Site:         Site,
					LoadURL:      LoadURL,
					LoadDomain:   LoadDomain,
					Type:         Type.String(),
					MimeType:     MimeType,
					RemoteIPAddr: RemoteIPAddress,
					ModTime:      fName.fileCTime,
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
	domainSets *map[string]map[string]map[string]bool,
	siteToDomains *map[string]map[string]int,
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

		// domainSets --> site: {date: {domains:bool}}
		_, found := (*domainSets)[result.Site]
		if found == false {
			(*domainSets)[result.Site] = make(map[string]map[string]bool)
		}
		_, found = (*domainSets)[result.Site][result.ModTime]
		if found == false {
			(*domainSets)[result.Site][result.ModTime] = make(map[string]bool)
		}
		((*domainSets)[result.Site])[result.ModTime][result.LoadDomain] = true

		// siteToDomains --> site: {domain:freq}
		_, found = (*siteToDomains)[result.Site]
		if found == false {
			(*siteToDomains)[result.Site] = make(map[string]int)
		}
		(*siteToDomains)[result.Site][result.LoadDomain]++
	}

	owg.Done()
}
