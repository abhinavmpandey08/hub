package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"html/template"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/insomniacslk/dhcp/dhcpv4"
	"github.com/insomniacslk/dhcp/dhcpv4/nclient4"
	log "github.com/sirupsen/logrus"
	"github.com/vishvananda/netns"
)

const (
	mountAction          = "/mountAction"
	bootConfigAction     = "/usr/bin/bootconfig"
	hegelUserDataVersion = "2009-04-04"
)

type Info struct {
	HWAddr      net.HardwareAddr
	IPAddr      net.IPNet
	Gateway     net.IP
	Nameservers []net.IP
}

func main() {
	fmt.Printf("WriteFile - Write file to disk\n------------------------\n")

	blockDevice := os.Getenv("DEST_DISK")
	filesystemType := os.Getenv("FS_TYPE")
	filePath := os.Getenv("DEST_PATH")

	contents := os.Getenv("CONTENTS")
	bootconfig := os.Getenv("BOOTCONFIG_CONTENTS")
	hegelUrls := os.Getenv("HEGEL_URLS")
	uid := os.Getenv("UID")
	gid := os.Getenv("GID")
	mode := os.Getenv("MODE")
	dirMode := os.Getenv("DIRMODE")

	if os.Getenv("STATIC_NETPLAN") == "true" {
		ifname := determineNetIF()
		if f := os.Getenv("IFNAME"); f != "" {
			ifname = f
		}
		var err error
		timeout := 2 * time.Minute
		if t := os.Getenv("DHCP_TIMEOUT"); t != "" {
			timeout, err = time.ParseDuration(t)
			if err != nil {
				log.Errorf("Invalid DHCP_TIMEOUT: %s, using default: %v", t, timeout)
			}
		}
		contents, err = dhcpAndWriteNetplan(ifname, timeout)
		if err != nil {
			log.Fatal(err)
		}
	}

	// Validate inputs
	if blockDevice == "" {
		log.Fatalf("No Block Device speified with Environment Variable [DEST_DISK]")
	}

	if !filepath.IsAbs(filePath) {
		log.Fatal("Provide path must be an absolute path")
	}

	modePrime, err := strconv.ParseUint(mode, 8, 32)
	if err != nil {
		log.Fatalf("Could not parse mode: %v", err)
	}

	// Only set one of contents, bootconfig or hegelUrls
	validationCount := 0
	for _, envVar := range []string{contents, bootconfig, hegelUrls} {
		if envVar != "" {
			validationCount++
		}
	}
	if validationCount != 1 {
		log.Fatal("Only one environment vars of CONTENTS, BOOTCONFIG_CONTENTS, HEGEL_URLS can be set")
	}

	fileMode := os.FileMode(modePrime)

	dirModePrime, err := strconv.ParseUint(dirMode, 8, 32)
	if err != nil {
		log.Fatalf("Could not parse dirmode: %v", err)
	}

	newDirMode := os.FileMode(dirModePrime)

	fileUID, err := strconv.Atoi(uid)
	if err != nil {
		log.Fatalf("Could not parse uid: %v", err)
	}

	fileGID, err := strconv.Atoi(gid)
	if err != nil {
		log.Fatalf("Could not parse gid: %v", err)
	}

	dirPath, fileName := filepath.Split(filePath)
	if len(fileName) == 0 {
		log.Fatal("Provide path must include a file component")
	}

	// Create the /mountAction mountpoint (no folders exist previously in scratch container)
	if err := os.Mkdir(mountAction, os.ModeDir); err != nil {
		log.Fatalf("Error creating the action Mountpoint [%s]", mountAction)
	}

	// Mount the block device to the /mountAction point
	if err := syscall.Mount(blockDevice, mountAction, filesystemType, 0, ""); err != nil {
		log.Fatalf("Mounting [%s] -> [%s] error [%v]", blockDevice, mountAction, err)
	}

	log.Infof("Mounted [%s] -> [%s]", blockDevice, mountAction)

	if err := recursiveEnsureDir(mountAction, dirPath, newDirMode, fileUID, fileGID); err != nil {
		log.Fatalf("Failed to ensure directory exists: %v", err)
	}

	if hegelUrls != "" {
		success := false
		// go through all urls in hegelUrls, and attempt to retrieve userdata from them
		// upon a successful userdata retrieval, this loop will exit and populate the userdata contents in contents
		for _, hegelUrl := range strings.Split(hegelUrls, ",") {
			userDataServiceUrl, err := url.ParseRequestURI(hegelUrl)
			if err != nil {
				log.Warnf("Error parsing hegel url: %v", err)
				continue
			}
			userDataServiceUrl.Path = path.Join(userDataServiceUrl.Path, hegelUserDataVersion, "user-data")
			client := http.Client{
				Timeout: time.Second * 10,
			}
			resp, err := client.Get(userDataServiceUrl.String())
			if err != nil {
				log.Warnf("Error with HTTP GET call: %v", err)
				continue
			}
			defer resp.Body.Close()

			respBody, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Warnf("Error reading HTTP GET response body: %v", err)
				continue
			}

			// Set contents to be the user-data
			contents = string(respBody)
			success = true
			break
		}

		if !success {
			log.Fatalf("Failed to read user-data, exhausted all the urls defined in HEGEL_URLS: {%v}", hegelUrls)
		}
	}

	// If bootconfig is set, contents will be empty and will serve as output initrd file provided
	// to bootconfig tool
	fqFilePath := filepath.Join(mountAction, filePath)
	// Write the file to disk
	if err := ioutil.WriteFile(fqFilePath, []byte(contents), fileMode); err != nil {
		log.Fatalf("Could not write file %s: %v", filePath, err)
	}

	if bootconfig != "" {
		// Write the input bootconfig to file to serve as input to the tool
		inputFilePath := "/userInputBootConfig"
		err := ioutil.WriteFile(inputFilePath, []byte(bootconfig), fileMode)
		if err != nil {
			log.Fatalf("Could not write file %s: %v", filePath, err)
		}
		defer os.Remove(inputFilePath)

		// Parse through bootconfig if enabled
		cmd := exec.Command(bootConfigAction, "-a", inputFilePath, fqFilePath)
		output, err := cmd.Output()
		if err != nil {
			log.Fatalf("Error running Bootconfig tool. Err: %v, Output: %s", err, string(output))
		}
	}

	if err := os.Chown(fqFilePath, fileUID, fileGID); err != nil {
		log.Fatalf("Could not modify ownership of file %s: %v", filePath, err)
	}

	log.Infof("Successfully wrote file [%s] to device [%s]", filePath, blockDevice)
}

func determineNetIF() string {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// Change to PID 1 network namespace for working with the host's interfaces.
	ns1, err := netns.GetFromPid(1)
	if err != nil {
		return ""
	}
	defer ns1.Close()
	err = netns.Set(ns1)
	if err != nil {
		return ""
	}

	ifs, err := net.Interfaces()
	if err != nil {
		return ""
	}

	for _, ifi := range ifs {
		addrs, err := ifi.Addrs()
		if err != nil {
			break
		}
		for _, addr := range addrs {
			ip, ok := addr.(*net.IPNet)
			if !ok {
				continue
			}
			v4 := ip.IP.To4()
			if v4 == nil || !v4.IsGlobalUnicast() {
				continue
			}

			return ifi.Name
		}
	}

	return ""
}

func dirExists(mountPath, path string) (bool, error) {
	fqPath := filepath.Join(mountPath, path)
	info, err := os.Stat(fqPath)

	switch {
	// Any error that does not indicate the directory doesn't exist
	case err != nil && !os.IsNotExist(err):
		return false, fmt.Errorf("failed to stat path %s: %w", path, err)
	// The directory already exists
	case err == nil:
		if !info.IsDir() {
			return false, fmt.Errorf("expected %s to be a path, but it is a file", path)
		}
	}

	return !os.IsNotExist(err), nil
}

func recursiveEnsureDir(mountPath, path string, mode os.FileMode, uid, gid int) error {
	// Does the directory already exist? If so we can return early
	exists, err := dirExists(mountPath, path)
	if err != nil {
		return err
	}

	if exists {
		return nil
	}

	pathParts := strings.Split(path, string(os.PathSeparator))
	if len(pathParts) == 1 && pathParts[0] == path {
		return errors.New("bad path")
	}

	basePath := string(os.PathSeparator)
	for _, part := range pathParts {
		basePath = filepath.Join(basePath, part)
		if err := ensureDir(mountPath, basePath, mode, uid, gid); err != nil {
			return err
		}
	}

	return nil
}

func ensureDir(mountPath, path string, mode os.FileMode, uid, gid int) error {
	exists, err := dirExists(mountPath, path)
	if err != nil {
		return err
	}

	if exists {
		return nil
	}

	// The directory doesn't exist, let's create it.
	fqPath := filepath.Join(mountPath, path)

	if err := os.Mkdir(fqPath, mode); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", path, err)
	}

	log.Infof("Successfully created directory: %s", path)

	if err := os.Chown(fqPath, uid, gid); err != nil {
		return fmt.Errorf("failed to set ownership of directory %s to %d:%d: %w", path, uid, gid, err)
	}

	log.Infof("Successfully set ownernership of directory %s to %d:%d", path, uid, gid)

	return nil
}

func dhcpAndWriteNetplan(ifname string, dhcpTimeout time.Duration) (string, error) {
	// After locking a goroutine to its current OS thread with runtime.LockOSThread()
	// and changing its network namespace, any new subsequent goroutine won't be scheduled
	// on that thread while it's locked. Therefore, the new goroutine will run in a
	// different namespace leading to unexpected results.
	// See these links for more details:
	// https://www.weave.works/blog/linux-namespaces-golang-followup
	// https://github.com/vishvananda/netns
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// Change to PID 1 network namespace so we can do a DHCP using the host's interface.
	ns1, err := netns.GetFromPid(1)
	if err != nil {
		return "", err
	}
	defer ns1.Close()
	err = netns.Set(ns1)
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), dhcpTimeout)
	defer cancel()
	d, err := dhcp(ctx, ifname)
	if err != nil {
		return "", err
	}

	netplanTemplate := `network:
    version: 2
    renderer: networkd
    ethernets:
        id0:
            match:
                macaddress: {{ .HWAddr }}
            addresses:
                - {{ ToString .IPAddr }}
            nameservers:
                addresses: [{{ ToStringSlice .Nameservers ", " }}]
            {{- if .Gateway }}
            routes:
                - to: default
                  via: {{ ToString .Gateway }}
            {{- end }}
`

	return createNetplan(netplanTemplate, translate(d))
}

func netIPToString(ip []net.IP, sep string) string {
	var strs []string
	for _, i := range ip {
		strs = append(strs, i.String())
	}
	return strings.Join(strs, sep)
}

func netToString(v interface{}) string {
	switch n := v.(type) {
	case net.IP:
		return n.String()
	case net.HardwareAddr:
		return n.String()
	case net.IPNet:
		return n.String()
	}

	return fmt.Sprintf("%v", v)
}

func createNetplan(tmpl string, i Info) (string, error) {
	tp, err := template.New("netplan").Funcs(template.FuncMap{"ToStringSlice": netIPToString}).Funcs(template.FuncMap{"ToString": netToString}).Parse(tmpl)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	err = tp.Execute(&buf, i)
	if err != nil {
		return "", err
	}

	return buf.String(), nil
}

func dhcp(ctx context.Context, ifname string) (*dhcpv4.DHCPv4, error) {
	c, err := nclient4.New(ifname)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	return c.DiscoverOffer(ctx)
}

func translate(d *dhcpv4.DHCPv4) Info {
	if d == nil {
		return Info{}
	}
	var info Info
	info.HWAddr = d.ClientHWAddr
	info.IPAddr = net.IPNet{IP: d.YourIPAddr, Mask: d.SubnetMask()}
	info.Gateway = d.GetOneOption(dhcpv4.OptionRouter)
	info.Nameservers = d.DNS()

	return info
}
