package main

import (
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Config struct {
	Chunksize  int
	IntervalMs time.Duration
	Sync       bool
	Outfile    string
}

type Statistics struct {
	WrittenBytes      int
	WrittenBytesTotal int
	LastUpdate        time.Time
	Start             time.Time
}

type App struct {
	outfile   *os.File
	csvfile   *os.File
	csvwriter *csv.Writer
	cfg       Config
	stats     Statistics
	data      []byte
}

func (a *App) write() (int, error) {
	written, err := a.outfile.Write(a.data)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error writing data:", err)
		return 0, err
	}

	if written != a.cfg.Chunksize {
		fmt.Fprintf(os.Stderr, "Could only write %d bytes\n", a.cfg.Chunksize-written)
	}

	if a.cfg.Sync {
		a.outfile.Sync()
	}

	a.stats.WrittenBytes += written
	a.stats.WrittenBytesTotal += written

	return written, nil
}

func (a *App) gatherStats() {
	for {
		_, err := a.write()
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error during write: ", err)
			os.Exit(1)
		}
	}
}

func (a *App) collectStats() {
	for {
		duration := time.Now().Sub(a.stats.LastUpdate)
		written := a.stats.WrittenBytes
		bytes := int64(written) * 1000 / int64(duration.Milliseconds())

		mbytes := float64(bytes) / 1024 / 1024

		fmt.Printf("%f MByte/s\n", mbytes)

		a.csvwriter.Write([]string{
			time.Now().Format("2006-01-02_15-04-05"),
			fmt.Sprintf("%f", time.Now().Sub(a.stats.Start).Seconds()),
			fmt.Sprintf("%f", mbytes),
		})
		a.csvwriter.Flush()

		a.stats.LastUpdate = time.Now()
		a.stats.WrittenBytes = 0

		time.Sleep(a.cfg.IntervalMs)
	}
}

func (a *App) getFinalStats() {
	duration := time.Now().Sub(a.stats.Start)
	written := a.stats.WrittenBytesTotal
	bytes := int64(written) * 1000 / int64(duration.Milliseconds())
	mbytes := float64(bytes) / 1024 / 1024

	fmt.Printf("Total: %f MByte/s\n", mbytes)

	a.csvwriter.Write([]string{
		time.Now().Format("2006-01-02_15-04-05"),
		fmt.Sprintf("%f", duration.Seconds()),
		fmt.Sprintf("%f", mbytes),
		"End",
	})
	a.csvwriter.Flush()
}

func (a *App) Run() {
	a.stats.Start = time.Now()

	go a.collectStats()
	go a.gatherStats()
}

func NewApp(cfg Config) *App {
	file, err := os.OpenFile(cfg.Outfile, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if errors.Is(err, os.ErrNotExist) {
		file, err = os.Create(cfg.Outfile)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error creating app:", err)
			return nil
		}
	} else if err != nil {
		fmt.Fprintln(os.Stderr, "Error creating app:", err)
		return nil
	}

	csvfile, err := os.Create(fmt.Sprintf("%s.csv", time.Now().Format("2006-01-02_15-04-05")))
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error creating app:", err)
		return nil
	}

	csvWriter := csv.NewWriter(csvfile)

	return &App{file, csvfile, csvWriter, cfg, Statistics{}, make([]byte, cfg.Chunksize, cfg.Chunksize)}
}

func main() {
	bs := flag.Int("chunksize", 65536, "The default chunksize to write")
	intv := flag.Int("interval", 250, "The default interval to gather statistics in ms")
	sync := flag.Bool("sync", true, "Sync after every write")

	flag.Parse()

	outfiles := flag.Args()

	if len(outfiles) != 1 {
		fmt.Fprintf(os.Stderr, "Exactly one output file required\n")
		os.Exit(1)
	}

	out := outfiles[0]
	cfg := Config{*bs, time.Duration(*intv * 1000 * 1000), *sync, out}
	app := NewApp(cfg)

	cancelChan := make(chan os.Signal, 1)
	signal.Notify(cancelChan, syscall.SIGTERM, syscall.SIGINT)

	if app != nil {
		app.Run()

		<-cancelChan

		app.getFinalStats()
	}
}
