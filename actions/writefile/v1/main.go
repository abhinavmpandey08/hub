package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	log "github.com/sirupsen/logrus"
)

const (
	mountAction = "/mountAction"
	bootConfigAction = "/usr/bin/bootconfig"
	hegelUserDataVersion = "2009-04-04"
)

func main() {
	fmt.Printf("WriteFile - Write file to disk\n------------------------\n")

	blockDevice := os.Getenv("DEST_DISK")
	filesystemType := os.Getenv("FS_TYPE")
	filePath := os.Getenv("DEST_PATH")

	contents := os.Getenv("CONTENTS")
	bootconfig := os.Getenv("BOOTCONFIG_CONTENTS")
	hegelUrl := os.Getenv("HEGEL_URL")
	uid := os.Getenv("UID")
	gid := os.Getenv("GID")
	mode := os.Getenv("MODE")
	dirMode := os.Getenv("DIRMODE")

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

	// Only set one of contents, bootconfig or hegelUrl
	validationCount := 0
	for _, envVar := range []string{contents, bootconfig, hegelUrl} {
		if envVar != "" {
			validationCount++
		}
	}
	if validationCount != 1 {
		log.Fatal("Only one environment vars of CONTENTS, BOOTCONFIG_CONTENTS, HEGEL_URL can be set")
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

	// If hegelUrl is set, get the user-data from hegel and set it to contents
	if hegelUrl != "" {
		userDataServiceUrl, err := url.Parse(hegelUrl)
		if err != nil {
			log.Fatalf("Error parsing hegel url: %v", err)
		}
		userDataServiceUrl.Path = path.Join(userDataServiceUrl.Path, hegelUserDataVersion, "user-data")
		resp, err := http.Get(userDataServiceUrl.String())
		if err != nil {
			log.Fatalf("Error with HTTP GET call: %v", err)
		}
		defer resp.Body.Close()

		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Fatalf("Error reading HTTP GET response body: %v", err)
		}

		// Set contents to be the user-data
		contents = string(respBody)
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
