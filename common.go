package writeaheadlog

import (
	"fmt"
	"os"

	"gitlab.com/NebulousLabs/Sia/encoding"
	"gitlab.com/NebulousLabs/errors"
)

var (
	// NameDeleteUpdate is the name of an idempotent update that deletes a file
	// from a given path on disk.
	NameDeleteUpdate = "DELETE"
	// NameTruncateUpdate is the name of an idempotent update that truncates a file
	// to have a certain size.
	NameTruncateUpdate = "TRUNCATE"
	// NameWriteAtUpdate is the name of an idempotent update that writes data to a
	// file at the specified offset. If the file doesn't exist it is created.
	NameWriteAtUpdate = "WRITEAT"
)

// ApplyDeleteUpdate parses and applies a delete update.
func ApplyDeleteUpdate(u Update) error {
	if u.Name != NameDeleteUpdate {
		return fmt.Errorf("applyDeleteUpdate called on update of type %v", u.Name)
	}
	// Remove file if it hasn't been removed already.
	if err := os.Remove(string(u.Instructions)); !os.IsNotExist(err) {
		return err
	}
	return nil
}

// ApplyTruncateUpdate parses and applies a truncate update.
func ApplyTruncateUpdate(u Update) error {
	if u.Name != NameTruncateUpdate {
		return fmt.Errorf("applyTruncateUpdate called on update of type %v", u.Name)
	}
	// Decode update.
	var path string
	var size int64
	err := encoding.UnmarshalAll(u.Instructions, &path, &size)
	if err != nil {
		return err
	}
	// Truncate file.
	return os.Truncate(path, size)
}

// ApplyWriteAtUpdate parses and applies a writeat update.
func ApplyWriteAtUpdate(u Update) error {
	if u.Name != NameWriteAtUpdate {
		return fmt.Errorf("applyWriteAtUpdate called on update of type %v", u.Name)
	}
	// Decode update.
	var path string
	var index int64
	var data []byte
	err := encoding.UnmarshalAll(u.Instructions, &path, &index, &data)
	if err != nil {
		return err
	}
	// Open file.
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	// Write data to file.
	_, err = f.WriteAt(data, index)
	return err
}

// DeleteUpdate creates an update that deletes the file at the specified path.
func DeleteUpdate(path string) Update {
	return Update{
		Name:         NameDeleteUpdate,
		Instructions: []byte(path),
	}
}

// TruncateUpdate is a helper function which creates a writeaheadlog update for
// truncating the specified file.
func TruncateUpdate(path string, size int64) Update {
	// Create update
	return Update{
		Name:         NameTruncateUpdate,
		Instructions: encoding.MarshalAll(path, size),
	}
}

// ApplyUpdates can be used to apply the common update types provided by the
// writeaheadlog. Since it potentially applies updates to many different files
// it's not optimized and opens and closes a file for each update. For optimal
// performance write a custom applyUpdates function.
func ApplyUpdates(updates ...Update) error {
	for _, update := range updates {
		var err error
		switch update.Name {
		case NameDeleteUpdate:
			err = ApplyDeleteUpdate(update)
		case NameTruncateUpdate:
			err = ApplyTruncateUpdate(update)
		case NameWriteAtUpdate:
			err = ApplyWriteAtUpdate(update)
		default:
			err = fmt.Errorf("unknown update type: %v", update.Name)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// WriteAtUpdate is a helper function which creates a writeaheadlog update for
// writing the specified data to the provided index of a file.
func WriteAtUpdate(path string, index int64, data []byte) Update {
	// Create update
	return Update{
		Name:         NameWriteAtUpdate,
		Instructions: encoding.MarshalAll(path, index, data),
	}
}

// CreateAndApplyTransaction is a helper method which creates a transaction from
// a given set of updates and uses the supplied updateFunc to apply it.
func (w *WAL) CreateAndApplyTransaction(applyFunc func(...Update) error, updates ...Update) error {
	// Create the transaction.
	txn, err := w.NewTransaction(updates)
	if err != nil {
		return errors.AddContext(err, "failed to create wal txn")
	}
	// No extra setup is required. Signal that it is done.
	if err := <-txn.SignalSetupComplete(); err != nil {
		return errors.AddContext(err, "failed to signal setup completion")
	}
	// Apply the updates.
	if err := applyFunc(updates...); err != nil {
		return errors.AddContext(err, "failed to apply updates")
	}
	// Updates are applied. Let the writeaheadlog know.
	if err := txn.SignalUpdatesApplied(); err != nil {
		return errors.AddContext(err, "failed to signal that updates are applied")
	}
	return nil

}
