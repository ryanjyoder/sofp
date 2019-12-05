package sofp

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func TestDownloadSitesXml(t *testing.T) {
	// Create temp directory
	dir, err := ioutil.TempDir("", "TestDownloadSitesXml")
	if err != nil {
		t.Error("couldnt create test directory:" + err.Error())
		t.FailNow()
	}
	defer os.RemoveAll(dir)

	// Call DownloadSitesXml
	err = DownloadSitesXml(context.TODO(), "https://archive.org", dir)
	if err != nil {
		t.Error("failed to download sites.xml: " + err.Error())
		t.FailNow()
	}

	info, err := os.Stat(dir + "/Sites.xml")
	if err != nil {
		t.Error("failed to stat Sites.xml: " + err.Error())
		t.FailNow()
	}

	if info.IsDir() {
		t.Error("Sites.xml is a directory")
		t.FailNow()
	}
}

func TestGetRemoteCurrentVersion(t *testing.T) {
	version, err := GetRemoteCurrentVersion(context.TODO(), "https://archive.org", "sitecore.stackexchange.com")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	fmt.Println("version is:", version)
}
