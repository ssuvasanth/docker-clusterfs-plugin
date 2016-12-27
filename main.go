package main

import(
	"flag"
	"fmt"
	"os"
//	"path/filepath"
//	"strings"

	"github.com/docker/go-plugins-helpers/volume"
)

var (
	clusterPath = flag.String("clustermount","","OCFS2 volume default mount point. This is mandatory.")
	
)

func main() {
	var Usage = func() {
		fmt.Fprintf(os.Stderr,"Usage: %s -clustermount <default_ocfs2_mount_point>", os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()
	
	if *clusterPath == "" {
		Usage()
		os.Exit(1)
	} else if _, err := os.Lstat(*clusterPath); os.IsNotExist(err) {                                // fi removed . should be added when needed to read file information
		fmt.Fprintf(os.Stderr, "Provided clustermount %s does not exist", *clusterPath)
		os.Exit(1)
	}

	d := newOcfs2Driver(clusterPath)				
	h := volume.NewHandler(d)
	fmt.Fprintf(os.Stdout, "%v\n", h.ServeUnix("root", "clusterfs"))	
}
