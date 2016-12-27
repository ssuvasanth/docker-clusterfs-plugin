package main

import (
	"bytes"
	"fmt"	
	"os"
	"os/exec"
	"sync"
	"path/filepath"
	"log"
	"encoding/json"
	"io/ioutil"
	"strings"

	"github.com/docker/go-plugins-helpers/volume"
)

const (
	pluginDataID = ".ocfs2plugindata"
	hostDataID = ".global_hosts.json"
	globalDataID = ".global.json"
	localDataID = ".local.json"
)

type defaultDir struct {
        fsLabel string
        fsLocalPath     string                  // path on the filesystem. i.e. absolute path is fsLabel/fsLocalPath
}


type ocfs2driver struct {
        defaultDir defaultDir
        pluginData string
	hostData string
        m *sync.Mutex
}

type host struct {
        hostname string
        status  string
}

type vol struct {
        Name string
        FsLabel string
        FsLocalPath string
        Status string
//      count int
}


func newOcfs2Driver(defaultPath *string) ocfs2driver {
        device ,mountpoint := deviceAndMountPointOf(*defaultPath)
        label := labelOf(device)
        if label == "" {
                fmt.Fprintf(os.Stderr, "FATAL: Filesystem on %s does not have a LABEL.", device)
                os.Exit(1)
        }
        pathOnFS := strings.SplitAfterN(*defaultPath, mountpoint, 2)
        if pathOnFS[1] == "" {                                                  // For handling mountpoint == pathOnFS condition i.e. pathOnFS is '/' on the filesystem.
                pathOnFS[1] = "/"
        }
        defaultDir := defaultDir {
                fsLabel:        label,
                fsLocalPath:    filepath.Join(pathOnFS[1], "/"),
                }

        d := ocfs2driver {
                defaultDir:     defaultDir,
                pluginData:     filepath.Join(mountpoint, "/", pluginDataID),
                hostData:       filepath.Join(mountpoint, "/", pluginDataID, hostDataID),
                m:              &sync.Mutex{},
        }

        _,err := os.Lstat(d.pluginData)
        if os.IsNotExist(err){
                if err := os.MkdirAll(d.pluginData, 0750); err != nil {
                        fmt.Fprintf(os.Stderr, "%s\n", err)
                        os.Exit(1)
                }
        }

        hosts := make(map[string]string)
        fileHosts := make(map[string]string)
	
	_, err = os.Lstat(d.hostData); 
	if os.IsExist(err){
		fileData, _ := ioutil.ReadFile(d.hostData)
                json.Unmarshal(fileData, &fileHosts)
                for hostname, status := range fileHosts {
                        hosts[hostname] = status
                }
	}
	
	current_hostname, err := os.Hostname()
        if err != nil {
                log.Fatal(err)
        }

	hosts[current_hostname] = "active"
	
	json_hosts, _ := json.MarshalIndent(hosts, "", "        ")
	_ = ioutil.WriteFile(d.hostData, json_hosts, 0750)

        return d
}

func deviceAndMountPointOf(path string) (device,mountpoint string) {
        df_cmd := exec.Command("df", path)
        var out bytes.Buffer
        df_cmd.Stdout = &out
        err := df_cmd.Run()
        if err != nil {
                log.Fatal(err)
        }
        outputLines := strings.Split(out.String(),"\n")
        columns := strings.Fields(outputLines[1])
        device, mountpoint = columns[0], columns[5]
        return device, mountpoint
}

func labelOf(device string) string {
        blkid_cmd := exec.Command("blkid", device)
        var out bytes.Buffer
        blkid_cmd.Stdout = &out
        err := blkid_cmd.Run()
        if err != nil {
                log.Fatal(err)
        }
        fields := strings.Fields(out.String())
        deviceAttribute := make(map[string]string)
        for _, attr := range fields {
                if !strings.Contains(attr, "=") {
                        continue
                }
                val := strings.Split(attr, "=")
                deviceAttribute[val[0]] = val[1]
        }
        return deviceAttribute["LABEL"]
}


func mountPointOf(label string) string {
        blkid_cmd := exec.Command("blkid", "-o", "device", "-t", "LABEL="+label)
        var out bytes.Buffer
        blkid_cmd.Stdout = &out
        err := blkid_cmd.Run();
        if err != nil {
                log.Fatal(err)
        }
        devices := strings.Split(out.String(), "\n")
        var d, m string
        for _, device := range devices {
                if device == "" {
                        continue
                }
                d, m = deviceAndMountPointOf(device)
                if d != device {
                        continue
                }
        }
        return m
}

func (d ocfs2driver) Create(r volume.Request) volume.Response {

        d.m.Lock()

        defer d.m.Unlock()

        globalJsonPath := filepath.Join(mountPointOf(d.defaultDir.fsLabel), "/", pluginDataID, globalDataID)    // add err from mountpoint because at this point we don't know if the labeled device exists

        globalJsonData, err := ioutil.ReadFile(globalJsonPath)
        volumes := make(map[string][]vol)
        json.Unmarshal(globalJsonData, &volumes)
        for _, vStruc := range volumes["Volumes"]       {
                if vStruc.Name == r.Name {
                        return volume.Response{Err: "Volume of the given name already exist!"}
                }
        }

        var (
                volumePath string
                fsLabel string
                fsLocalPath     string
                localJsonPath   string
		localPluginData string
        )

        if len(r.Options) == 0  {

		mountpoint := mountPointOf(d.defaultDir.fsLabel)
                volumePath = filepath.Join(mountpoint, "/", d.defaultDir.fsLocalPath, r.Name)

                fsLabel = d.defaultDir.fsLabel
                fsLocalPath = d.defaultDir.fsLocalPath
                localJsonPath = filepath.Join(mountpoint, "/", pluginDataID, localDataID)

        } else if path := r.Options["path"]; path != "" {

                if _,err := os.Lstat(path); os.IsNotExist(err) {
                        return volume.Response{Err: "Given path does not exist!"}
                }

                device ,mountpoint := deviceAndMountPointOf(path)
                pathOnFS := strings.SplitAfterN(path, mountpoint, 2)
                if pathOnFS[1] == "" {                                                  // For handling mountpoint == pathOnFS condition i.e. pathOnFS is '/' on the filesystem.
                        pathOnFS[1] = "/"
                }

                fsLabel = labelOf(device)
                fsLocalPath = filepath.Join(pathOnFS[1], "/")

                localJsonPath = filepath.Join(mountpoint, "/", pluginDataID, localDataID)

                volumePath = filepath.Join(mountpoint, "/", fsLocalPath, r.Name)
		
		localPluginData = filepath.Join(mountpoint, "/", pluginDataID)	
		 _, err = os.Lstat(localPluginData)
                if os.IsNotExist(err) {
                        if err := os.MkdirAll(localPluginData, 0750); err != nil {
                        fmt.Fprintf(os.Stderr, "%s\n", err)
                        os.Exit(1)
                        }
		}

        }

        _, err = os.Lstat(volumePath)
        if os.IsExist(err) {
                return volume.Response{Err: "Volume directory already exist!"}
        }

        if err := os.MkdirAll(volumePath, 0750); err != nil {
                log.Println(err)
                return volume.Response{Err: "Unable to create volume directory!"}
        }

/* make entry to .global.json */
        newVolume := vol {
                Name:           r.Name,
                FsLabel:        fsLabel,
                FsLocalPath:    fsLocalPath,
                Status:         "",
        }

        volumes["Volumes"] = append(volumes["Volumes"], newVolume)

        json_volumes, _ := json.MarshalIndent(volumes, "", "    ")
        _ = ioutil.WriteFile(globalJsonPath, json_volumes, 0750)

/* Write to .local.json file for records and during recovery */

        localJsonData, err := ioutil.ReadFile(localJsonPath)
        local_volumes := make(map[string][]vol)

        json.Unmarshal(localJsonData, &local_volumes)
        local_volumes["Volumes"] = append(local_volumes["Volumes"], newVolume)
        local_json, _ := json.MarshalIndent(local_volumes, "", "        ")
        _ = ioutil.WriteFile(localJsonPath, local_json, 0750)

return volume.Response{}
}

func (d ocfs2driver) List(r volume.Request) volume.Response {

        d.m.Lock()

        defer d.m.Unlock()

/* Get the path of global.json, read it, reformat into daemon's acceptable response format */

        globalJsonPath := filepath.Join(mountPointOf(d.defaultDir.fsLabel), "/", pluginDataID, globalDataID)    // add err from mountpoint because at this point we don't know if the labeled device exists

        globalJsonData, _ := ioutil.ReadFile(globalJsonPath)            // err unhandled for file does not exist case
        volumes := make(map[string][]vol)
        json.Unmarshal(globalJsonData, &volumes)
        var volumesArray []*volume.Volume
        for _, vStruc := range volumes["Volumes"] {
                v := &volume.Volume {
                        Name:   vStruc.Name,
                        Mountpoint:     filepath.Join(mountPointOf(vStruc.FsLabel), "/", vStruc.FsLocalPath),
                }
                volumesArray = append(volumesArray, v)
        }

/* return response to daemon json.Marshal-ed but as string*/

return volume.Response{Volumes: volumesArray}
}

func (d ocfs2driver) Get(r volume.Request) volume.Response {

        d.m.Lock()

        defer d.m.Unlock()

        var v volume.Volume

        volumes := make(map[string][]vol)

/* Get the path of global.json, read it, reformat into daemon's acceptable response format */

        globalJsonPath := filepath.Join(mountPointOf(d.defaultDir.fsLabel), "/", pluginDataID, globalDataID)    // add err from mountpoint because at this point we don't know if the labeled device exists

        _, err := os.Lstat(globalJsonPath)
        if err != nil {
                return volume.Response{Err: "Global volumes data unavailable!"}
        }

        globalJsonData, _ := ioutil.ReadFile(globalJsonPath)            // err unhandled for file does not exist case
        json.Unmarshal(globalJsonData, &volumes)
        volumesArray := volumes["Volumes"]
        if l:=len(volumesArray); l == 0 {
                return volume.Response{Err: "No volumes present!"}
        } else if l != 0 {
                for _, Volume := range volumesArray {
                        if Volume.Name == r.Name {
                                v = volume.Volume {
                                        Name:           Volume.Name,
                                        Mountpoint:     filepath.Join(mountPointOf(Volume.FsLabel), "/", Volume.FsLocalPath, Volume.Name),
                                }
                        }
                }
        }
if v.Name != "" {
	return volume.Response{Volume: &v}
}

return volume.Response{}

}

func (d ocfs2driver) Remove(r volume.Request) volume.Response {

        d.m.Lock()

        defer d.m.Unlock()

        var pathLabel string
        volumes := make(map[string][]vol)
        var (
                volumesArray2 []vol
                localJsonPath string
        )
        volumes2 := make(map[string][]vol)

        mountpoint := mountPointOf(d.defaultDir.fsLabel)
        globalJsonPath := filepath.Join(mountpoint, "/", pluginDataID, globalDataID)

        _, err := os.Lstat(globalJsonPath)
        if os.IsNotExist(err) {
                /* If file does not exist, it means that the requested volume does not exist. */
                return volume.Response{}
        }

        globalJsonData, _ := ioutil.ReadFile(globalJsonPath)
        json.Unmarshal(globalJsonData, &volumes)
        volumesArray := volumes["Volumes"]
        if l:=len(volumesArray); l == 0 {
                /* If no volumes present, it means that the requested volume does not exist. */
                return volume.Response{}
        } else if l != 0 {
                for _, Volume := range volumesArray {
                        if Volume.Name == r.Name {

                                pathLabel = Volume.FsLabel
                                path := filepath.Join(mountPointOf(pathLabel), "/", Volume.FsLocalPath, Volume.Name)

                                if err := os.RemoveAll(path); err != nil {
                                        return volume.Response{Err: fmt.Sprintf("Unable to remove the directory %s!\n", path)}
                                }
                                continue
                        }
                        volumes2["Volumes"] = append(volumesArray2, Volume)
                }
        }

/* make entry to .global.json */


        json_volumes, _ := json.MarshalIndent(volumes2, "", "    ")
        _ = ioutil.WriteFile(globalJsonPath, json_volumes, 0750)

/* Write to .local.json file for records and during recovery */

        mountpoint = mountPointOf(pathLabel)
        localJsonPath = filepath.Join(mountpoint, "/", pluginDataID, localDataID)

        localJsonData, err := ioutil.ReadFile(localJsonPath)
        local_volumes := make(map[string][]vol)
        local_volumes2 := make(map[string][]vol)

        json.Unmarshal(localJsonData, &local_volumes)

        var localVolumesArray2 []vol

        volumesArray = local_volumes["Volumes"]
        if l:=len(volumesArray); l == 0 {
                /* If no volumes present, it means that the requested volume does not exist. */
                return volume.Response{}
        } else if l != 0 {
                for _, Volume := range volumesArray {
                        if Volume.Name == r.Name {
                                continue
                                }

                      local_volumes2["Volumes"] = append(localVolumesArray2, Volume)
                }
        }


        local_json, _ := json.MarshalIndent(local_volumes2, "", "        ")
        _ = ioutil.WriteFile(localJsonPath, local_json, 0750)

return volume.Response{}
}


func (d ocfs2driver) Path(r volume.Request) volume.Response {

        res := d.Mount(volume.MountRequest{Name: r.Name})

return volume.Response{Mountpoint: res.Mountpoint, Err: res.Err}
}

func (d ocfs2driver) Mount(r volume.MountRequest) volume.Response {

        d.m.Lock()

        defer d.m.Unlock()

        var m string

        volumes := make(map[string][]vol)

        mountpoint := mountPointOf(d.defaultDir.fsLabel)
        globalJsonPath := filepath.Join(mountpoint, "/", pluginDataID, globalDataID)

        _, err := os.Lstat(globalJsonPath)
        if err != nil {
                return volume.Response{Err: "Global volumes data unavailable!"}
        }

        globalJsonData, _ := ioutil.ReadFile(globalJsonPath)
        json.Unmarshal(globalJsonData, &volumes)
        volumesArray := volumes["Volumes"]
        if l:=len(volumesArray); l == 0 {
                return volume.Response{Err: "No volumes present!"}
        } else if l != 0 {
                for _, Volume := range volumesArray {
                        if Volume.Name == r.Name {
                                m = filepath.Join(mountPointOf(Volume.FsLabel), "/", Volume.FsLocalPath, Volume.Name)
                        }
                }
        }

        _, err = os.Lstat(m)
        if os.IsNotExist(err) {
                return volume.Response{Err: fmt.Sprintf("Volume path %s does not exist on the system!", m)}
        }
return volume.Response{Mountpoint: m}
}

func (d ocfs2driver) Unmount(r volume.UnmountRequest) volume.Response {
	/* Nothing to do */
return volume.Response{}
}

func (d ocfs2driver) Capabilities(r volume.Request) volume.Response {

return volume.Response{Capabilities: volume.Capability{Scope: "global"}}

}
