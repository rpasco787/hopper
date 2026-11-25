package accounting

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// An evidence directory holds one scenario's run (docs/design/conformance.md,
// "Expect and Evidence"):
//
//	expect.json              the Expect, indented
//	rows.csv                 id,state,attempts,lease_id,last_error
//	<container>.stdout       each container's stdout, the JSON log
//	<container>.stderr       each container's stderr
//	exits.json               container -> exit code
const (
	expectFile = "expect.json"
	rowsFile   = "rows.csv"
	exitsFile  = "exits.json"
)

var rowsHeader = []string{"id", "state", "attempts", "lease_id", "last_error"}

// WriteEvidence saves e and ev under dir, which it creates.
func WriteEvidence(dir string, e Expect, ev Evidence) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(e, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, expectFile), append(raw, '\n'), 0o644); err != nil {
		return err
	}

	f, err := os.Create(filepath.Join(dir, rowsFile))
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	werr := w.Write(rowsHeader)
	for _, r := range ev.Rows {
		lease := ""
		if r.LeaseID != uuid.Nil {
			lease = r.LeaseID.String()
		}
		if werr == nil {
			werr = w.Write([]string{r.ID.String(), r.State, strconv.Itoa(r.Attempts), lease, r.LastError})
		}
	}
	w.Flush()
	if err := errors.Join(werr, w.Error(), f.Close()); err != nil {
		return err
	}

	for c, b := range ev.Stdout {
		if err := os.WriteFile(filepath.Join(dir, c+".stdout"), b, 0o644); err != nil {
			return err
		}
	}
	for c, b := range ev.Stderr {
		if err := os.WriteFile(filepath.Join(dir, c+".stderr"), b, 0o644); err != nil {
			return err
		}
	}
	exits := ev.Exits
	if exits == nil {
		exits = map[string]int{}
	}
	raw, err = json.MarshalIndent(exits, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, exitsFile), append(raw, '\n'), 0o644)
}

// ReadEvidence loads what WriteEvidence saved.
func ReadEvidence(dir string) (Expect, Evidence, error) {
	var e Expect
	ev := Evidence{Stdout: map[string][]byte{}, Stderr: map[string][]byte{}}
	raw, err := os.ReadFile(filepath.Join(dir, expectFile))
	if err != nil {
		return e, ev, err
	}
	if err := json.Unmarshal(raw, &e); err != nil {
		return e, ev, fmt.Errorf("%s: %w", expectFile, err)
	}

	f, err := os.Open(filepath.Join(dir, rowsFile))
	if err != nil {
		return e, ev, err
	}
	defer f.Close()
	recs, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return e, ev, fmt.Errorf("%s: %w", rowsFile, err)
	}
	if len(recs) == 0 || !slices.Equal(recs[0], rowsHeader) {
		return e, ev, fmt.Errorf("%s: header is not %s", rowsFile, strings.Join(rowsHeader, ","))
	}
	for _, rec := range recs[1:] {
		id, err := uuid.Parse(rec[0])
		if err != nil {
			return e, ev, fmt.Errorf("%s: %w", rowsFile, err)
		}
		attempts, err := strconv.Atoi(rec[2])
		if err != nil {
			return e, ev, fmt.Errorf("%s: %w", rowsFile, err)
		}
		r := Row{ID: id, State: rec[1], Attempts: attempts, LastError: rec[4]}
		if rec[3] != "" {
			if r.LeaseID, err = uuid.Parse(rec[3]); err != nil {
				return e, ev, fmt.Errorf("%s: %w", rowsFile, err)
			}
		}
		ev.Rows = append(ev.Rows, r)
	}

	for ext, into := range map[string]map[string][]byte{".stdout": ev.Stdout, ".stderr": ev.Stderr} {
		paths, err := filepath.Glob(filepath.Join(dir, "*"+ext))
		if err != nil {
			return e, ev, err
		}
		for _, path := range paths {
			if into[strings.TrimSuffix(filepath.Base(path), ext)], err = os.ReadFile(path); err != nil {
				return e, ev, err
			}
		}
	}

	if raw, err = os.ReadFile(filepath.Join(dir, exitsFile)); err != nil {
		return e, ev, err
	}
	if err := json.Unmarshal(raw, &ev.Exits); err != nil {
		return e, ev, fmt.Errorf("%s: %w", exitsFile, err)
	}
	if len(ev.Exits) == 0 {
		ev.Exits = nil
	}
	return e, ev, nil
}
