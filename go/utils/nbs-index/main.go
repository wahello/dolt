package main

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/dolthub/dolt/go/store/nbs"
)

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		fmt.Println("Converts an index file with lengths to an index file with offsets. Usage ./nbs-index [inFilePath] [outFilePath]")
		os.Exit(0)
	}

	inFileName := args[0]
	outFileName := args[1]

	src, err := os.Open(inFileName)
	if err != nil {
		log.Fatalf("failed to open infile, err: %s", err.Error())
	}
	defer src.Close()

	chunkCount, _, err := nbs.ReadTableFooter(src)
	if err != nil {
		log.Fatal(err)
	}

	_, err = src.Seek(0, io.SeekStart)
	if err != nil {
		log.Fatal(err)
	}
	dst, err := os.Create(outFileName)
	if err != nil {
		log.Fatalf("failed to create outfile, err: %s", err.Error())
	}
	defer dst.Close()

	trans := nbs.NewIndexFileTransformer(src, chunkCount)
	_, err = io.Copy(dst, trans)
	if err != nil {
		log.Fatalf("failed to copy data, err: %s", err.Error())
	}

	err = dst.Sync()
	if err != nil {
		log.Fatalf("failed to write data, err: %s", err.Error())
	}

	fmt.Println("Done!")
}
