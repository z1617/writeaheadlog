package writeaheadlog

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"gitlab.com/NebulousLabs/fastrand"
)

// TestCommon tests the methods of common.go.
func TestCommon(t *testing.T) {
	// Create testing environment.
	testDir := tempDir(t.Name())
	if err := os.MkdirAll(testDir, 0777); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(testDir, "test.file")
	txns, wal, err := New(filepath.Join(testDir, "test.wal"))
	if err != nil {
		t.Fatal(err)
	}
	if len(txns) != 0 {
		t.Fatal("wal wasn't empty")
	}
	// Create a file using an update.
	data := fastrand.Bytes(100)
	update := WriteAtUpdate(path, 0, data)
	err = wal.CreateAndApplyTransaction(ApplyUpdates, update)
	if err != nil {
		t.Fatal(err)
	}
	// Make sure the file was created.
	readData, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, readData) {
		t.Fatal("Data on disk doesn't match written data")
	}
	// Truncate the file to half of its size.
	update = TruncateUpdate(path, int64(len(data)/2))
	err = wal.CreateAndApplyTransaction(ApplyUpdates, update)
	if err != nil {
		t.Fatal(err)
	}
	// Make sure the file has the right contents.
	readData, err = ioutil.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data[:len(data)/2], readData) {
		t.Fatal("Data on disk doesn't match written data")
	}
	// Delete the file.
	update = DeleteUpdate(path)
	err = wal.CreateAndApplyTransaction(ApplyUpdates, update)
	if err != nil {
		t.Fatal(err)
	}
	// Make sure the file is gone.
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("file should've been deleted")
	}
}
