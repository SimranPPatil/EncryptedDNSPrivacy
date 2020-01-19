package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"

	"github.com/tidwall/gjson"
)

type parsedData struct {
	RequestID, LoadURL, LoadDomain, Type, MimeType, RemoteIPAddr string
}

func main() {

	file, e := ioutil.ReadFile("./resource_metadata.json")
	if e != nil {
		fmt.Printf("File error: %v\n", e)
		os.Exit(1)
	}

	outfile, err := os.OpenFile("test.json", os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalf("failed opening file: %s", err)
	}
	defer outfile.Close()

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
			fmt.Println("\nrequestID: ", requestID)
			fmt.Println("loadURL: ", loadurl)
			fmt.Println("loadDomain: ", loadDomain)
			fmt.Println("Type: ", Type)
			fmt.Println("mimeType: ", mimeType)
			fmt.Println("remoteIPAddress: ", remoteIPAddress)
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

			b, _ := json.Marshal(pd)
			fmt.Println(string(b))
			len, err := outfile.WriteString(string(b) + "\r\n")
			if err != nil {
				log.Fatalf("failed writing to outfile: %s", err)
			}
			fmt.Printf("\nLength: %d bytes", len)
			fmt.Printf("\noutfile Name: %s", outfile.Name())
		}
	}
}
