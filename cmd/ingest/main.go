package main

import (
	"bufio"
	"compress/gzip"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/dmibor/pilosa-ingester/logstorage"
	"github.com/jaffee/commandeer/cobrafy"
	gopilosa "github.com/pilosa/go-pilosa"
	"github.com/pilosa/pdk"
	"github.com/pilosa/pdk/termstat"
	"github.com/pkg/errors"
)

// Command holds the config for the downloader.
type Command struct {
	AWSKey    string `help:"aws key"`
	AWSSecret string `help:"aws secret"`
	Pilosa    []string
	Index     string
	BatchSize uint

	coll    *termstat.Collector
	indexer pdk.Indexer

	colNext pdk.INexter

	cfTrans *mapTranslator
	ofTrans *mapTranslator
	sfTrans *mapTranslator
}

type mapTranslator struct {
	m map[string]uint64
	n pdk.INexter
}

func newMapTranslator() *mapTranslator {
	return &mapTranslator{
		m: make(map[string]uint64),
		n: pdk.NewNexter(),
	}
}

func (m *mapTranslator) Trans(s string) uint64 {
	if i, ok := m.m[s]; ok {
		return i
	}
	n := m.n.Next()
	m.m[s] = n
	return n
}

// NewCommand news up a Command.
func NewCommand() *Command {
	return &Command{
		Pilosa:    []string{"localhost:10101"},
		Index:     "eyeotatest",
		BatchSize: 1000000,
		coll:      termstat.NewCollector(os.Stdout),

		colNext: pdk.NewNexter(),

		cfTrans: newMapTranslator(),
		ofTrans: newMapTranslator(),
		sfTrans: newMapTranslator(),
	}
}

var logFields = map[string]int{
	"ts":          0,
	"idfield":     1,
	"cfield":      2,
	"ofield":      3,
	"sfield_list": 4,
}

// Run runs the command
func (c *Command) Run() (err error) {
	ls := logstorage.NewLogStorage(&logstorage.Config{
		BucketName:   "sample-for-pilosa.s3.eyeota.net",
		BucketRegion: "us-east-1",
		AwsKey:       c.AWSKey,
		AwsSecret:    c.AWSSecret,
	})

	schema := gopilosa.NewSchema()
	idx := schema.Index(c.Index)
	idx.Field("sfield", gopilosa.OptFieldTypeTime(gopilosa.TimeQuantumMonthDay))
	idx.Field("cfield", gopilosa.OptFieldTypeTime(gopilosa.TimeQuantumMonthDay))
	idx.Field("ofield", gopilosa.OptFieldTypeTime(gopilosa.TimeQuantumMonthDay))

	c.indexer, err = pdk.SetupPilosa(c.Pilosa, c.Index, schema, c.BatchSize)
	if err != nil {
		return errors.Wrap(err, "setting up pilosa")
	}

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
				err = c.processLine(line)
				if err != nil {
					return errors.Wrap(err, "")
				}
			}
		}
	}

	return nil
}

func (c *Command) processLine(line string) error {
	fields := strings.Split(line, "\t")
	if len(fields) != 5 {
		log.Println("Wrong number of fields in line: '%s'", line)
		c.coll.Count("badLines", 1, 1)
		return nil
	}
	ts, err := time.Parse("2006-01-02 15:04:05", fields[logFields["ts"]])
	if err != nil {
		log.Println(errors.Wrapf(err, "could not convert time for id %s and time %s", fields[logFields["idfield"]],
			fields[logFields["ts"]]))
		c.coll.Count("badTimes", 1, 1)
	}
	c.coll.Count("lines", 1, 1)
	col := c.colNext.Next()
	c.indexer.AddColumnTimestamp("cfield", col, c.cfTrans.Trans(fields[logFields["cfield"]]), ts)
	c.indexer.AddColumnTimestamp("ofield", col, c.ofTrans.Trans(fields[logFields["ofield"]]), ts)

	str := fields[logFields["sfield_list"]]
	if str == "" { // don't process empty list
		return nil
	}
	sfields := strings.Split(str, ",")
	for _, sf := range sfields {
		c.indexer.AddColumnTimestamp("ofield", col, c.sfTrans.Trans(sf), ts)
	}

	return nil
}

func main() {
	err := cobrafy.Execute(NewCommand())
	if err != nil {
		log.Fatal(err)
	}
}
