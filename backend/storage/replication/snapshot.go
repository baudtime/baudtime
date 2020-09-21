package replication

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	backendmsg "github.com/baudtime/baudtime/msg/backend"
	"github.com/baudtime/baudtime/util/os/fileutil"
	"github.com/baudtime/baudtime/vars"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb"
)

type currentFile struct {
	file *os.File
	ulid string
	path string
}

type Snapshot struct {
	epoch   int64
	db      *tsdb.DB
	snapDir string
	blocks  []*tsdb.BlockMeta

	current currentFile
	buf     []byte

	snapMtx sync.RWMutex

	progress    backendmsg.BlockSyncOffset
	progressMtx sync.RWMutex
}

func newSnapshot(db *tsdb.DB) *Snapshot {
	return &Snapshot{
		db:  db,
		buf: make([]byte, 1024*1024),
	}
}

func (s *Snapshot) Init(epoch int64) error {
	s.snapMtx.Lock()
	defer s.snapMtx.Unlock()

	if s.epoch != epoch {
		s.reset()

		snapDir := filepath.Join(s.db.Dir(), fmt.Sprintf("rpl_%d", epoch))
		if fileutil.Exist(snapDir) {
			fileutil.ClearPath(snapDir)
		}

		err := s.db.Snapshot(snapDir, true)
		if err != nil {
			return errors.Wrap(err, "snapshotting failed")
		}

		blockMetas, err := s.snapshotMeta(snapDir)
		if err != nil {
			os.RemoveAll(snapDir)
			return err
		}

		s.blocks = blockMetas
		s.snapDir = snapDir
		s.epoch = epoch
	}
	return nil
}

func (s *Snapshot) Reset() {
	s.snapMtx.Lock()
	s.reset()
	s.snapMtx.Unlock()

	s.progressMtx.Lock()
	s.progress = backendmsg.BlockSyncOffset{}
	s.progressMtx.Unlock()
}

func (s *Snapshot) reset() {
	if s.current.file != nil {
		s.current.file.Close()
		s.current.file = nil
	}
	if len(s.snapDir) > 0 && fileutil.Exist(s.snapDir) {
		os.RemoveAll(s.snapDir)
		s.snapDir = ""
	}
}

func (s *Snapshot) FetchData(offset *backendmsg.BlockSyncOffset) ([]byte, error) {
	s.progressMtx.Lock()
	s.progress = *offset
	s.progressMtx.Unlock()

	s.snapMtx.Lock()
	defer s.snapMtx.Unlock()

	if len(offset.Ulid) == 0 || len(offset.Path) == 0 {
		return nil, nil
	}

	if s.current.ulid != offset.Ulid || s.current.path != offset.Path {
		if s.current.file != nil {
			s.current.file.Close()
			s.current.file = nil
		}

		f, err := os.Open(filepath.Join(s.snapDir, offset.Ulid, offset.Path))
		if os.IsNotExist(err) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}

		s.current.file = f
		s.current.ulid = offset.Ulid
		s.current.path = offset.Path
	}

	if s.current.file == nil {
		return nil, nil
	}

	s.current.file.Seek(offset.Offset, 0)

	n, err := s.current.file.Read(s.buf)
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}

	return s.buf[:n], nil
}

func (s *Snapshot) CutOffset(syncOffset *backendmsg.BlockSyncOffset) (*backendmsg.BlockSyncOffset, error) {
	s.snapMtx.RLock()
	defer s.snapMtx.RUnlock()

	if syncOffset.Ulid == "" {
		block := s.nextBlock(syncOffset.MinT)
		if block == nil {
			return nil, nil
		}
		syncOffset.Ulid = block.ULID.String()
		syncOffset.MinT = block.MinTime
		syncOffset.MaxT = block.MaxTime
		syncOffset.Path = metaFileName
		syncOffset.Offset = 0
	} else {
		path, err := s.nextPath(syncOffset.Ulid, syncOffset.Path)
		if err != nil {
			return nil, err
		}

		if path == "" {
			block := s.nextBlock(syncOffset.MinT)
			if block == nil {
				return nil, nil
			}

			syncOffset.Ulid = block.ULID.String()
			syncOffset.MinT = block.MinTime
			syncOffset.MaxT = block.MaxTime
			syncOffset.Path = metaFileName
			syncOffset.Offset = 0
		} else {
			syncOffset.Path = path
			syncOffset.Offset = 0
		}
	}

	return syncOffset, nil
}

func (s *Snapshot) Progress() (progre backendmsg.BlockSyncOffset) {
	s.progressMtx.RLock()
	progre = s.progress
	s.progressMtx.RUnlock()
	return
}

func (s *Snapshot) nextBlock(mint int64) *tsdb.BlockMeta {
	if len(s.blocks) == 0 {
		return nil
	}

	blk := s.blocks[0]

	if len(s.blocks) > 1 {
		s.blocks = s.blocks[1:]
	} else {
		s.blocks = s.blocks[:0]
	}

	if blk.MaxTime >= mint {
		return blk
	} else {
		return s.nextBlock(mint)
	}
}

func (s *Snapshot) nextPath(ulid string, currentPath string) (string, error) {
	path := ""
	blockDir := filepath.Join(s.snapDir, ulid)

	if currentPath == metaFileName {
		path = indexFileName
	} else if currentPath == indexFileName {
		path = tombstonesFileName
	} else if currentPath == tombstonesFileName {
		path = filepath.Join("chunks", fmt.Sprintf("%0.6d", 1))
	} else {
		i, err := strconv.ParseUint(strings.TrimPrefix(currentPath, "chunks"+string(os.PathSeparator)), 10, 64)
		if err != nil {
			return "", err
		}

		path = filepath.Join("chunks", fmt.Sprintf("%0.6d", i+1))
	}

	_, err := os.Stat(filepath.Join(blockDir, path))
	if os.IsNotExist(err) {
		return "", nil
	}

	if err != nil {
		return s.nextPath(ulid, path)
	}

	return path, nil
}

func (s *Snapshot) snapshotMeta(dir string) (blockMetas []*tsdb.BlockMeta, err error) {
	bDirs, err := blockDirs(dir)
	if err != nil {
		return nil, errors.Wrap(err, "find blocks")
	}

	for _, bDir := range bDirs {
		meta, _, err := readMetaFile(bDir)
		if err != nil {
			level.Error(vars.Logger).Log("msg", "not a block dir", "dir", bDir)
			continue
		}

		blockMetas = append(blockMetas, meta)
	}
	sort.Slice(blockMetas, func(i, j int) bool {
		return blockMetas[i].MinTime < blockMetas[j].MinTime
	})
	return blockMetas, nil
}

func readMetaFile(dir string) (*tsdb.BlockMeta, int64, error) {
	b, err := ioutil.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		return nil, 0, err
	}
	var m tsdb.BlockMeta

	if err := json.Unmarshal(b, &m); err != nil {
		return nil, 0, err
	}
	if m.Version != 1 {
		return nil, 0, errors.Errorf("unexpected meta file version %d", m.Version)
	}

	return &m, int64(len(b)), nil
}

func isBlockDir(fi os.FileInfo) bool {
	if !fi.IsDir() {
		return false
	}
	_, err := ulid.ParseStrict(fi.Name())
	return err == nil
}

func blockDirs(dir string) ([]string, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var dirs []string

	for _, fi := range files {
		if isBlockDir(fi) {
			dirs = append(dirs, filepath.Join(dir, fi.Name()))
		}
	}
	return dirs, nil
}
