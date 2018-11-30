package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/dmibor/pilosa-ingester/logstorage"
	"github.com/jaffee/commandeer"
	"github.com/pilosa/pdk/termstat"
	"github.com/pkg/errors"
)

// Command holds the config for the downloader.
type Command struct {
	AWSKey    string `help:"aws key"`
	AWSSecret string `help:"aws secret"`
}

// NewCommand news up a Command.
func NewCommand() *Command {
	return &Command{}
}

var logFields = map[string]int{
	"ts":          0,
	"idfield":     1,
	"cfield":      2,
	"ofield":      3,
	"sfield_list": 4,
}

// Run runs the command
func (c *Command) Run() error {
	ls := logstorage.NewLogStorage(&logstorage.Config{
		BucketName:   "sample-for-pilosa.s3.eyeota.net",
		BucketRegion: "us-east-1",
		AwsKey:       c.AWSKey,
		AwsSecret:    c.AWSSecret,
	})

	coll := termstat.NewCollector(os.Stdout)

	cfield := make(map[string]int)
	ofield := make(map[string]int)
	sfield := make(map[string]int)
	sfieldNum := make(map[int]int)

	for i := 0; i <= 100; i++ {
		files, err := ls.List("2018-07-01", i)
		if err != nil {
			return errors.Wrapf(err, "listing bucket %d", i)
		}
		for _, f := range files {
			reader, err := ls.Get(f.Name)
			if err != nil {
				return errors.Wrapf(err, "getting file: %s", f.Name)
			}
			defer reader.Close()
			fz, err := gzip.NewReader(reader)
			if err != nil {
				return errors.Wrap(err, "gzip reader")
			}
			r := bufio.NewReader(fz)
			for {
				line, err := r.ReadString('\n')
				if err != nil {
					if err != io.EOF {
						return errors.Wrap(err, "reading line")
					}
					break
				}
				fields := strings.Split(line, "\t")
				if len(fields) != 5 {
					log.Println("Wrong number of fields in line: '%s'", line)
					coll.Count("badLines", 1, 1)
					continue
				}
				coll.Count("lines", 1, 1)
				cfield[fields[logFields["cfield"]]]++
				ofield[fields[logFields["ofield"]]]++
				str := fields[logFields["sfield_list"]]
				if str == "" { //don't process empty list
					sfieldNum[0]++
					continue
				}
				sfields := strings.Split(str, ",")
				sfieldNum[len(sfields)]++
				for _, sf := range sfields {
					sfield[sf]++
				}
			}
			log.Println("Done file: %s", f.Name)
			log.Printf("cfield: %v\nofield: %v\n", cfield, ofield)
			log.Printf("cfield: %d, ofield: %d, sfield: %d\n", len(cfield), len(ofield), len(sfield))
		}
	}

	for i, fieldMap := range []map[string]int{cfield, ofield, sfield} {
		f, err := os.Create(fmt.Sprintf("fielddata%d", i))
		if err != nil {
			log.Println("couldn't create output file", err)
		}
		for s, n := range fieldMap {
			f.WriteString(fmt.Sprintf("%s\t%d\n", s, n))
		}
		f.Close()
	}

	return nil
}

func main() {
	err := commandeer.Run(NewCommand())
	if err != nil {
		log.Fatal(err)
	}
}
