package server

import (
	"api-cache/github_types"
	"api-cache/http_utils"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)


// This file contains the implementation of a server that provides a GitHub API read cache
// service. It performs the following :
// (1) Serve cached results for
//     /
//     /orgs/Netflix
//     /orgs/Netflix/members
//     /orgs/Netflix/repos
// (2) Provide views for
//     /view/top/N/forks
//     /view/top/N/last_updated
//     /view/top/N/open_issues
//     /view/top/N/stars
// (3) Proxies all other urls to github.


// Useful constants for paths we will be serving.
const (
	kRouteHealthCheck     = "/healthcheck"
	kGitHubRoot           = "/"
	kGitHubNetflix        = "/orgs/Netflix"
	kGitHubNetflixMembers = "/orgs/Netflix/members"
	kGitHubNetflixRepos   = "/orgs/Netflix/repos"
	kViews                = "/view/top/"
)

// viewElm caches netflix/repos fields that are required to satisfy the views API. We
// keep sorted pointers (sorted by the view's sort attribute) to these in per-view sorted
// lists.
type viewElm struct {
	name string
	forks int
	updated time.Time
	openIssues int
	stars int
}

// The server object.
type Server struct {
	// Port on which to listen on.
	port uint32
	// API token for getting around rate limiting. If this fields is non empty, then it's
	// sent in the "Authorization" header for all GET requests to github.
	apiToken string
	// Cache of cached paths to their bodies.
	caches map[string][]byte
	// Sorted slices of viewElm pointers for the various views.
	topForks []*viewElm
	lastUpdated []*viewElm
	topOpenIssues []*viewElm
	topStars []*viewElm

	// Whether the server is ready to serve requests.
	ready bool
	// Lock to synchronize access to above fields.
	lock sync.Mutex
}

// Construct a new server object.
func NewServer(port uint32, apiToken string) *Server {
	s := &Server{port:port, apiToken:apiToken, caches: make(map[string][]byte)}
	http.HandleFunc(kRouteHealthCheck, createWrappedHandlerFn(s, handleHealthCheck))
	http.HandleFunc(kGitHubRoot, createWrappedHandlerFn(s, handleRoot))
	http.HandleFunc(kGitHubNetflix, createWrappedHandlerFn(s, handleNetflix))
	http.HandleFunc(kGitHubNetflixMembers, createWrappedHandlerFn(s, handleNetflixMembers))
	http.HandleFunc(kGitHubNetflixRepos, createWrappedHandlerFn(s, handleNetflixRepos))
	http.HandleFunc(kViews, createWrappedHandlerFn(s, handleViews))
	return s
}

// Creates a callback function suitable for passing into golang's http.HandleFunc() method
// that also binds the server object along with it.
func createWrappedHandlerFn(s *Server, fn func(s *Server, w http.ResponseWriter,
	r *http.Request)) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		fn(s, w, r)
	}
}

// Run the server. This method doesn't return.
func (s *Server) Run() {
	// Start the server to handle HTTP requests in a gofunc.
	go func() {
		http.ListenAndServe(fmt.Sprintf(":%v", s.port), nil)
	}()

	// Loop forever, refreshing the caches every 5 minutes.
	for {
		s.refreshCaches()
		time.Sleep(time.Minute * 5)
	}
}

// Refresh the cached APIs.
func (s *Server) refreshCaches() {
	// Refresh all caches in parallel.
	var wg sync.WaitGroup
	wg.Add(4)
	go func() {
		defer wg.Done()
		s.refreshRoot()
	}()
	go func() {
		defer wg.Done()
		s.refreshNetflix()
	}()
	go func() {
		defer wg.Done()
		s.refreshNetflixRepos()
	}()
	go func() {
		defer wg.Done()
		s.refreshNetflixMembers()
	}()
	wg.Wait()
	// Mark ourselves ready after the first cache update. Even though s.ready is a single
	// bool, and updates to it should be inherently atomic, we perform the update under a
	// lock to ensure that the update invalidates cache lines on all cpus. This is because
	// the readycheck handler may be running on a different cpu.
	if !s.ready {
		// Update s.ready under a lock to flush it to main memory and invalidate it in
		// the cache lines, ensuring other goroutines running on other cpus see the change.
		s.lock.Lock()
		s.ready = true
		s.lock.Unlock()
		log.Printf("Ready to accept requests")
	}
}

// Helper functions to refresh the various caches.
func (s *Server) refreshRoot() {
	g := http_utils.NewPagedGet(kGitHubRoot, s.apiToken)
	// NOTE: we expect only a single page for this url.
	body, _ := g.GetPage()
	s.lock.Lock()
	defer s.lock.Unlock()
	s.caches[kGitHubRoot] = body
	log.Printf("Refreshed root cache")
}

func (s *Server) refreshNetflix() {
	g := http_utils.NewPagedGet(kGitHubNetflix, s.apiToken)
	// NOTE: we expect only a single page for this url.
	body, _ := g.GetPage()
	s.lock.Lock()
	defer s.lock.Unlock()
	s.caches[kGitHubNetflix] = body
	log.Printf("Refreshed orgs/netflix cache")
}

func (s *Server) refreshNetflixRepos() {
	g := http_utils.NewPagedGet(kGitHubNetflixRepos, s.apiToken)
	// NOTE: we expect multiple pages for this url. In order to flatten them into a single
	// page, we read, deserialize and append repos from each page into a single slice and
	// then serialize the slice into a single serialized json.
	next := true
	var elms []*viewElm
	var repos []*github_types.Repository
	for next {
		var body []byte
		// Get body for the next page.
		body, next = g.GetPage()
		// Deserialize into repos.
		var pageRepos []*github_types.Repository
		json.Unmarshal(body, &pageRepos)
		// Process each repo.
		for _, r := range pageRepos {
			// Append to single slice for flattening later.
			repos = append(repos, r)
			// Create view element.
			ve := &viewElm{name:*r.Name, forks:*r.ForksCount, updated:r.UpdatedAt.Time,
				openIssues:*r.OpenIssuesCount, stars:*r.StargazersCount}
			elms = append(elms, ve)
		}
		fmt.Printf("Number of netflix repos %v\n", len(repos))
	}

	// Once we have gathered all pages, we can lock to update the cache, and update the
	// sorted views.
	s.lock.Lock()
	defer s.lock.Unlock()
	// Serialize the flattened repos.
	s.caches[kGitHubNetflixRepos], _ = json.Marshal(repos)

	// Clear the per-view sorted slices before refreshing them.
	s.topForks = s.topForks[:0]
	s.lastUpdated = s.lastUpdated[:0]
	s.topOpenIssues = s.topOpenIssues[:0]
	s.topStars = s.topStars[:0]
	// Refresh the per-view sorted slices.
	for _, ve := range elms {
		s.topForks = append(s.topForks, ve)
		s.lastUpdated = append(s.lastUpdated, ve)
		s.topOpenIssues = append(s.topOpenIssues, ve)
		s.topStars = append(s.topStars, ve)
	}
	sort.Slice(s.topForks, func(i, j int) bool {
		return s.topForks[i].forks > s.topForks[j].forks
	})
	sort.Slice(s.lastUpdated, func(i, j int) bool {
		return s.lastUpdated[i].updated.After(s.lastUpdated[j].updated)
	})
	sort.Slice(s.topOpenIssues, func(i, j int) bool {
		return s.topOpenIssues[i].openIssues > s.topOpenIssues[j].openIssues
	})
	sort.Slice(s.topStars, func(i, j int) bool {
		return s.topStars[i].stars > s.topStars[j].stars
	})
	log.Printf("Refreshed orgs/netflix/repos cache")
}

func (s *Server) refreshNetflixMembers() {
	g := http_utils.NewPagedGet(kGitHubNetflixMembers, s.apiToken)
	body, _ := g.GetPage()
	s.lock.Lock()
	defer s.lock.Unlock()
	s.caches[kGitHubNetflixMembers] = body
	log.Printf("Refreshed orgs/netflix/members cache")
}

// HTTP handler functions.
func handleHealthCheck(s *Server, w http.ResponseWriter, r *http.Request) {
	s.lock.Lock()
	ready := s.ready
	s.lock.Unlock()
	if ready {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
}

func handleRoot(s *Server, w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" {
		s.lock.Lock()
		body := make([]byte, len(s.caches[kGitHubRoot]))
		copy(body, s.caches[kGitHubRoot])
		s.lock.Unlock()
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Write(body)
	} else {
		http_utils.Forward(w, r)
	}
}

func handleNetflix(s *Server, w http.ResponseWriter, r *http.Request) {
	s.lock.Lock()
	body := make([]byte, len(s.caches[kGitHubNetflix]))
	copy(body, s.caches[kGitHubNetflix])
	s.lock.Unlock()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write(body)
}

func handleNetflixRepos(s *Server, w http.ResponseWriter, r *http.Request) {
	s.lock.Lock()
	body := make([]byte, len(s.caches[kGitHubNetflixRepos]))
	copy(body, s.caches[kGitHubNetflixRepos])
	s.lock.Unlock()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write(body)
}

func handleNetflixMembers(s* Server, w http.ResponseWriter, r *http.Request) {
	s.lock.Lock()
	body := make([]byte, len(s.caches[kGitHubNetflixMembers]))
	copy(body, s.caches[kGitHubNetflixMembers])
	s.lock.Unlock()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write(body)
}

func handleViews(s* Server, w http.ResponseWriter, r *http.Request) {
	s.lock.Lock()
	tokens := strings.Split(strings.TrimSpace(r.URL.Path), "/")
	count, _ := strconv.Atoi(tokens[3])
	sortBy := tokens[4]
	body := "["
	for ii := int(0); ii < count; ii++ {
		var elm string
		if sortBy == "forks" {
			elm = fmt.Sprintf("[\"Netflix/%v\",%v]", s.topForks[ii].name, s.topForks[ii].forks)
		} else if sortBy == "last_updated" {
			elm = fmt.Sprintf("[\"Netflix/%v\",\"%vZ\"]", s.lastUpdated[ii].name, strings.TrimSuffix(s.lastUpdated[ii].updated.Local().String(), "-0700 PDT"))
		} else if sortBy == "open_issues" {
			elm = fmt.Sprintf("[\"Netflix/%v\",%v]", s.topOpenIssues[ii].name, s.topOpenIssues[ii].openIssues)
		} else if sortBy == "stars" {
			elm = fmt.Sprintf("[\"Netflix/%v\",%v]", s.topStars[ii].name, s.topStars[ii].stars)
		}
		body += elm
		if ii < count - 1 {
			body += ","
		}
	}
	body += "]"
	s.lock.Unlock()
	w.Write([]byte(body))
}
