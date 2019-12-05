package sofp

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cavaliercoder/grab"
)

type Downloader struct {
	RootDir string
	Host    string
}

func NewDownloader(rootDir, host string) (*Downloader, error) {
	_, err := url.Parse(host)
	return &Downloader{
		RootDir: rootDir,
		Host:    host,
	}, err

}

func (d *Downloader) Run(ctx context.Context) error {

	if err := DownloadSitesXml(ctx, d.Host, d.RootDir); err != nil {
		return err
	}

	if exists, err := SitesXmlExists(d.RootDir); !exists || err != nil {
		return fmt.Errorf("Sites.xml does not exist or can't be found")
	}

	sites, err := ListDomains(d.RootDir)
	if err != nil {
		return err
	}

	for _, domain := range sites {
		fmt.Println(domain)
		etag, err := GetRemoteCurrentVersion(ctx, d.Host, domain)
		if err == nil {
			SetCurrentArchiveVersion(d.RootDir, domain, etag)
		}

		if ok, err := CurrentVersionIsSet(d.RootDir, domain); !ok || err != nil {
			fmt.Println("current version is not set for:", domain, err)
			continue
		}

		completed, err := CurrentArchiveIsDownloaded(d.RootDir, domain)
		if completed {
			continue
		}
		if err != nil {
			fmt.Println("error checking download status:", err)
			continue
		}

		err = DownloadArchive(ctx, d.Host, d.RootDir, domain)
		if err != nil {
			fmt.Println("error downloading:", domain, err)
			continue
		}

		err = SetArchiveDownloaded(d.RootDir, domain)
		if err != nil {
			fmt.Println("error downloading:", domain, err)
			continue
		}

	}

	return nil

}

func DownloadSitesXml(ctx context.Context, host string, rootDir string) error {
	output := filepath.Join(rootDir, "Sites.xml.partial")

	// clean up output file if an old on is laying around
	os.RemoveAll(output)

	downloadUrl := host + "/download/stackexchange/Sites.xml"

	// TODO: Cleanly implement cancellation
	_, err := grab.Get(output, downloadUrl)
	if err != nil {
		return err
	}

	return os.Rename(output, rootDir+"/Sites.xml")
}

func SitesXmlExists(rootDir string) (bool, error) {
	filename := filepath.Join(rootDir, "Sites.xml")
	info, err := os.Stat(filename)
	// NotExist error is and expected error. Do not return error
	if err != nil && err == os.ErrNotExist {
		return false, nil
	}
	// all other errors should be returned
	if err != nil {
		return false, err
	}

	if info.IsDir() {
		return false, fmt.Errorf("Sites.xml is a directory")
	}

	return true, nil

}

func ListDomains(rootDir string) ([]string, error) {
	filename := filepath.Join(rootDir, "Sites.xml")
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sites, err := GetDomainsFromSitesXml(f)

	domains := make([]string, len(sites))
	for i := range sites {
		u, err := url.Parse(sites[i].Url)
		if err != nil {
			return nil, err
		}
		domains[i] = u.Host
	}

	return domains, err

}

var domainIsMultiArchive = map[string]bool{
	"stackoverflow.com": true,
}

func GetRemoteCurrentVersion(ctx context.Context, host string, domain string) (string, error) {
	filename := domain + ".7z"
	if domainIsMultiArchive[domain] {
		filename = domain + "-Posts.7z"
	}
	downloadUrl := host + "/download/stackexchange/" + filename
	resp, err := http.Head(downloadUrl)
	if err != nil {
		return "", err
	}

	etag := resp.Header.Get("ETag")
	if etag == "" {
		return "", fmt.Errorf("no etag set in headers")
	}

	etag = strings.Trim(etag, `"`)

	return etag, nil
}

func SetCurrentArchiveVersion(rootDir string, domain string, etag string) error {
	currentLink := filepath.Join(rootDir, domain, "current")
	etag = filepath.Base(etag) // just to remove any potential nastiness
	versionDir := filepath.Join(rootDir, domain, etag)
	if err := os.MkdirAll(versionDir, os.ModePerm); err != nil {
		return err
	}
	os.RemoveAll(currentLink)
	return os.Symlink(etag, currentLink)

}

func CurrentVersionIsSet(rootDir string, domain string) (bool, error) {
	filename := filepath.Join(rootDir, domain, "current")

	_, err := os.Stat(filename)
	// NotExist error is and expected error. Do not return error
	if err != nil && err == os.ErrNotExist {
		return false, nil
	}
	// all other errors should be returned
	if err != nil {
		return false, err
	}

	// TODO: check this is a symlink
	//if info.Mode() != os.ModeSymlink {
	//	return false, fmt.Errorf("current must be a symlink")
	//}

	return true, nil
}

func CurrentArchiveIsDownloaded(rootDir string, domain string) (bool, error) {
	return flagIsSet(rootDir, domain, "download-complete")
}
func SetArchiveDownloaded(rootDir string, domain string) error {
	return setFlag(rootDir, domain, "download-complete")
}

func DownloadArchive(ctx context.Context, host string, rootDir string, domain string) error { // save to etag in header
	downloadFiles := []string{domain + ".7z"}
	if domainIsMultiArchive[domain] {
		downloadFiles = []string{domain + "-Posts.7z", domain + "-Badges.7z", domain + "-Comments.7z", domain + "-PostHistory.7z", domain + "-PostLinks.7z", domain + "-Tags.7z", domain + "-Users.7z", domain + "-Votes.7z"}
	}
	downloadUrl := host + "/download/stackexchange/" + downloadFiles[0]
	baseLastModified, err := getLastModifiedHeader(ctx, downloadUrl)
	if err != nil {
		return err
	}

	for _, filename := range downloadFiles {
		downloadUrl := host + "/download/stackexchange/" + filename
		lastModified, err := getLastModifiedHeader(ctx, downloadUrl)
		if err != nil {
			return err
		}

		// If the difference is more than a week, then it's in the process of uploading a new version
		if math.Abs(baseLastModified.Sub(lastModified).Hours()) > 24*7 {
			return fmt.Errorf("new version is being uploaded")
		}

		output := filepath.Join(rootDir, domain, "current", filename)
		_, err = grab.Get(output, downloadUrl)
		if err != nil {
			return err
		}
	}
	return nil
}
func getLastModifiedHeader(ctx context.Context, url string) (time.Time, error) {
	resp, err := http.Head(url)
	if err != nil {
		return time.Time{}, err
	}

	lastModifiedString := resp.Header.Get("Last-Modified")
	return time.Parse(http.TimeFormat, lastModifiedString)
}

func flagIsSet(rootDir string, domain string, flag string) (bool, error) {
	flag = filepath.Base(flag) // just remove any potential nastiness
	filename := filepath.Join(rootDir, domain, "current", flag)

	_, err := os.Stat(filename)
	// NotExist error is and expected error. Do not return error
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return false, nil
		}
	}
	// all other errors should be returned
	if err != nil {
		return false, err
	}

	return true, nil
}

func setFlag(rootDir string, domain string, flag string) error {
	filename := filepath.Join(rootDir, domain, "current", flag)
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	return f.Close()

}
