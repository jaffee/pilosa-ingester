package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/dmibor/pilosa-ingester/logstorage"
	"github.com/jaffee/commandeer/cobrafy"
	gopilosa "github.com/pilosa/go-pilosa"
	"github.com/pilosa/pdk"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// Command holds the config for the downloader.
type Command struct {
	AWSKey    string `help:"aws key"`
	AWSSecret string `help:"aws secret"`
	Pilosa    []string
	Index     string
	BatchSize uint

	indexer pdk.Indexer

	colNext pdk.INexter

	colTrans *mapTranslator
	cfTrans  *mapTranslator
	ofTrans  *mapTranslator
	sfTrans  *mapTranslator

	lines int
	last  time.Time
	tl    sync.Mutex

	of io.Writer
}

type mapTranslator struct {
	m map[string]uint64
	l sync.RWMutex
	n pdk.INexter
}

func newMapTranslator() *mapTranslator {
	return &mapTranslator{
		m: make(map[string]uint64),
		n: pdk.NewNexter(),
	}
}

func (m *mapTranslator) Trans(s string) uint64 {
	m.l.RLock()
	if i, ok := m.m[s]; ok {
		m.l.RUnlock()
		return i
	}
	m.l.RUnlock()
	m.l.Lock()
	defer m.l.Unlock()
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

		colNext: pdk.NewNexter(),

		colTrans: newMapTranslator(),
		cfTrans:  newMapTranslator(),
		ofTrans:  newMapTranslator(),
		sfTrans:  newMapTranslator(),

		last: time.Now(),
		tl:   sync.Mutex{},
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
	f, err := ioutil.TempFile("", "")
	if err != nil {
		return errors.Wrap(err, "creating temp file")
	}
	defer f.Close()
	c.of = bufio.NewWriter(f)
	c.of = ioutil.Discard
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

	fc := make(chan logstorage.AwsFile, 10000)
	eg := errgroup.Group{}

	// start file processing routines
	for i := 0; i < 8; i++ {
		i := i
		eg.Go(func() error {
			return c.processFiles(fc, ls, i)
		})
	}

	for i := 0; i <= 100; i++ {
		files, err := ls.List("2018-07-01", i)
		if err != nil {
			return errors.Wrapf(err, "listing bucket %d", i)
		}

		for _, f := range files {
			fc <- f
		}
	}
	close(fc)

	return eg.Wait()
}

func (c *Command) processFiles(fc chan logstorage.AwsFile, ls *logstorage.LogStorage, num int) error {
	lines := 0
	for f := range fc {
		reader, err := ls.Get(f.Name)
		if err != nil {
			return errors.Wrapf(err, "getting file: %s", f.Name)
		}
		fz, err := gzip.NewReader(reader)
		if err != nil {
			reader.Close()
			return errors.Wrap(err, "gzip reader")
		}
		r := bufio.NewReader(fz)
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				if err != io.EOF {
					reader.Close()
					return errors.Wrap(err, "reading line")
				}
				break
			}
			lines++
			if lines%lineChunk == 0 {
				c.track()
			}
			err = c.processLine(line)
			if err != nil {
				reader.Close()
				return errors.Wrap(err, "")
			}
		}
		reader.Close()
	}
	return nil
}

func (c *Command) addColTS(a string, col, r uint64, ts time.Time) {
	c.indexer.AddColumnTimestamp(a, col, r, ts)

	// _, err := c.of.Write([]byte(fmt.Sprintf("%s %d %d %v\n", a, col, r, ts)))
	// if err != nil {
	// 	panic(err)
	// }
}

func (c *Command) processLine(line string) error {
	fields := strings.Split(line, "\t")
	if len(fields) != 5 {
		log.Println("Wrong number of fields in line: '%s'", line)
		return nil
	}
	ts, err := time.Parse("2006-01-02 15:04:05", fields[logFields["ts"]])
	if err != nil {
		log.Println(errors.Wrapf(err, "could not convert time for id %s and time %s", fields[logFields["idfield"]],
			fields[logFields["ts"]]))
	}
	col := c.colTrans.Trans(fields[logFields["idfield"]])
	c.addColTS("cfield", col, c.cfTrans.Trans(fields[logFields["cfield"]]), ts)
	c.addColTS("ofield", col, c.ofTrans.Trans(fields[logFields["ofield"]]), ts)

	str := fields[logFields["sfield_list"]]
	if str == "" { // don't process empty list
		return nil
	}
	sfields := strings.Split(str, ",")
	for _, sf := range sfields {
		c.addColTS("ofield", col, c.sfTrans.Trans(sf), ts)
	}

	return nil
}

func main() {
	err := cobrafy.Execute(NewCommand())
	if err != nil {
		log.Fatal(err)
	}
}

var lineChunk int = 1000
var block float64 = 500000

func (c *Command) track() {
	c.tl.Lock()
	c.lines += lineChunk
	if c.lines%int(block) == 0 {
		new := time.Now()
		rate := block / new.Sub(c.last).Seconds()
		fmt.Printf("Lines: %d, Rate: %g/s\n", c.lines, rate)
		c.last = new
	}
	c.tl.Unlock()
}
