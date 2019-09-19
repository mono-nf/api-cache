package http_utils

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

// Helper struct that aids in paged gets by keeping track of the next link.
type PagedGet struct {
	nextLink string
	authHdr  string
}

// Creates a new PagedGet struct.
func NewPagedGet(path string, apiToken string) *PagedGet {
	var authHdr string
	if apiToken != "" {
		authHdr = fmt.Sprintf("token %s", apiToken)
	}
	return &PagedGet{nextLink: fmt.Sprintf("https://api.github.com%s", path), authHdr:authHdr}
}

// Gets next page and whether there are more pages remaining.
func (g *PagedGet) GetPage() ([]byte, bool) {
	// We don't expect to be called if nextLink is empty.
	if g.nextLink == "" {
		log.Panicf("GetPage beyond page chain.")
	}
	req, err := http.NewRequest("GET", g.nextLink, nil)
	if err != nil {
		log.Panicf("Get request failed %v", err.Error())
	}
	req.Header.Add("Accept", "application/vnd.github.v3+json")
	// Add api token if needed.
	if g.authHdr != "" {
		req.Header.Add("Authorization", g.authHdr)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Panicf("Failed to issue http GET on url=%v, err=%v", g.nextLink, err.Error())
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	linksRelStr := resp.Header.Get("Link")
	// If link header is missing, then this url has only a single page.
	if linksRelStr == "" {
		return body, false
	}
	// Search for link to next page.
	linkRels := strings.Split(linksRelStr, ",")
	for _, lr := range linkRels {
		l:= strings.Split(lr, ";")
		// If next page link is found, return true to indicate to caller that GetPage
		// needs to be called again.
		if strings.TrimSpace(l[1]) == "rel=\"next\"" {
			g.nextLink = strings.TrimSpace(l[0])
			g.nextLink = strings.TrimPrefix(g.nextLink, "<")
			g.nextLink = strings.TrimSuffix(g.nextLink, ">")
			return body, true
		}
	}
	// This is the last page.
	return body, false
}

func Forward(w http.ResponseWriter, r *http.Request) {
	url := fmt.Sprintf("https://api.github.com%s", r.URL)
	log.Printf("Forwarding %v", url)
	req, err := http.NewRequest("GET", url, r.Body)
	if err != nil {
		log.Panicf("Get request failed %v", err.Error())
	}
	req.Header = r.Header
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Panicf("Failed to issue http GET on url=%v, err=%v", url, err.Error())
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	//w.Header() = resp.Header
	w.Write(body)
}
