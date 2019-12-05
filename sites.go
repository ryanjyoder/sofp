package sofp

import (
	"encoding/xml"
	"io"
	"io/ioutil"
)

type SitesXml struct {
	XMLName xml.Name `xml:"sites"`
	Sites   []Site   `xml:"row"`
}

type Site struct {
	row            xml.Name `xml:"row"`
	Id             int      `xml:"Id,attr"`             // "344"
	TinyName       string   `xml:"TinyName,attr"`       // "stellarme"
	Name           string   `xml:"Name,attr"`           // "Stellar Meta"
	LongName       string   `xml:"LongName,attr"`       // "Stellar Meta"
	Url            string   `xml:"Url,attr"`            // "https://stellar.meta.stackexchange.com"
	ImageUrl       string   `xml:"ImageUrl,attr"`       // "https://cdn.sstatic.net/Sites/stellarmeta/img/logo.png"
	IconUrl        string   `xml:"IconUrl,attr"`        // "https://cdn.sstatic.net/Sites/stellarmeta/img/favicon.ico"
	DatabaseName   string   `xml:"DatabaseName,attr"`   // "StackExchange.Stellar.Meta"
	Tagline        string   `xml:"Tagline,attr"`        // "Q&amp;A for developers and users of Stellar and the Stellar Distributed Exchange"
	TagCss         string   `xml:"TagCss,attr"`         // ""
	TotalQuestions string   `xml:"TotalQuestions,attr"` // "21"
	TotalAnswers   string   `xml:"TotalAnswers,attr"`   // "29"
	TotalUsers     string   `xml:"TotalUsers,attr"`     // "208"
	TotalComments  string   `xml:"TotalComments,attr"`  // "29"
	TotalTags      string   `xml:"TotalTags,attr"`      // "72"
	LastPost       string   `xml:"LastPost,attr"`       // "2019-03-03T03:00:57.460"
	ParentId       string   `xml:"ParentId,attr"`       // "343"
	BadgeIconUrl   string   `xml:"BadgeIconUrl,attr"`   // "https://cdn.sstatic.net/Sites/stellarmeta/img/apple-touch-icon.png"
}

func GetDomainsFromSitesXml(xmlFile io.Reader) ([]Site, error) {

	byteValue, err := ioutil.ReadAll(xmlFile)
	if err != nil {
		return nil, err
	}

	var sites SitesXml
	err = xml.Unmarshal(byteValue, &sites)
	return sites.Sites, err
}
