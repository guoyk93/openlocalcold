package main

import (
	"context"
	"errors"
	"flag"
	"github.com/olivere/elastic/v7"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

var (
	optDataDir string
	optURL     string
)

func exit(err *error) {
	if *err != nil {
		log.Println("exited with error:", (*err).Error())
		os.Exit(1)
	} else {
		os.Exit(0)
	}
}

func main() {
	var err error
	defer exit(&err)

	flag.StringVar(&optDataDir, "data-dir", "", "elasticsearch data directory")
	flag.StringVar(&optURL, "url", "http://127.0.0.1:9200", "elasticsearch url")
	flag.Parse()

	var dataDirs []string
	splits := strings.Split(optDataDir, ",")
	for _, split := range splits {
		split = strings.TrimSpace(split)
		if split == "" {
			continue
		}
		dataDirs = append(dataDirs, split)
	}

	var client *elastic.Client
	if client, err = elastic.NewClient(elastic.SetURL(optURL)); err != nil {
		return
	}

	var indices elastic.CatIndicesResponse
	if indices, err = client.CatIndices().Do(context.Background()); err != nil {
		return
	}

	uuids := map[string]bool{}

	for _, dataDir := range dataDirs {
		var idxDirs []os.FileInfo
		if idxDirs, err = ioutil.ReadDir(dataDir); err != nil {
			return
		}
		for _, idxDir := range idxDirs {
			if !idxDir.IsDir() {
				continue
			}
			idxDirPath := filepath.Join(dataDir, idxDir.Name())
			var idxSegDirs []os.FileInfo
			if idxSegDirs, err = ioutil.ReadDir(idxDirPath); err != nil {
				return
			}
			for _, idxSegDir := range idxSegDirs {
				if !idxSegDir.IsDir() {
					continue
				}
				if !isNum(idxSegDir.Name()) {
					continue
				}
				log.Printf("Found: %s/%s", idxDir.Name(), idxSegDir.Name())
				uuids[idxDir.Name()] = true
			}
		}
	}

	var idxNames []string

outerLoop:
	for uuid := range uuids {
		for _, idx := range indices {
			if idx.UUID != uuid {
				continue
			}
			idxNames = append(idxNames, idx.Index)
			continue outerLoop
		}
	}

	if len(idxNames) != len(uuids) {
		err = errors.New("indices count mismatch")
		return
	}

	for _, idxName := range idxNames {
		log.Println("Index:", idxName)
		if _, err = client.OpenIndex(idxName).Do(context.Background()); err != nil {
			return
		}
	}
}

func isNum(s string) bool {
	if i, err := strconv.Atoi(s); err != nil {
		return false
	} else {
		if strconv.Itoa(i) != s {
			return false
		}
		return true
	}
}
