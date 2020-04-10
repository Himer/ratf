package raft

//       SnapshotStore用来存储快照信息，对于stcache来说，就是存储当前的所有的kv数据，hashicorp内部提供3中快照存储方式，分别是：
//
//DiscardSnapshotStore：  不存储，忽略快照，相当于/dev/null，一般用于测试
//FileSnapshotStore：        文件持久化存储
//InmemSnapshotStore：   内存存储，不持久化，重启程序会丢失
//
//​       这里我们使用文件持久化存储。snapshotStore只是提供了一个快照存储的介质，还需要应用程序提供快照生成的方式，后面我们再具体说。
import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	hclog "github.com/hashicorp/go-hclog"
)


//----snapshots
//-------------1-222-3333333.tmp
//--------------------------meta.json
//--------------------------state.bin
const (
	testPath      = "permTest"  //用于测试镜像群路径可写的文件名字
	snapPath      = "snapshots" //镜像群的路径
	metaFilePath  = "meta.json" //一个镜像的meta信息
	stateFilePath = "state.bin" //一个镜像的数据
	tmpSuffix     = ".tmp"  //临时镜像的目录名字
)

// FileSnapshotStore implements the SnapshotStore interface and allows
// snapshots to be made on the local disk.
//FileSnapshotStore代表一坨 文件列表表示的镜像群
// FileSnapshotSink表示一个个镜像每个任期有一个
type FileSnapshotStore struct {
	path   string    /*目录*/
	retain int     /*保留个数*/
	logger hclog.Logger   /*日志输出*/

	// noSync, if true, skips crash-safe file fsync api calls.
	// It's a private field, only used in testing
	noSync bool
}

//snapMetaSlice用户对meta信息进行排序
// 先按照任期配置
// 如果任期一样则按照日志index排序
// 如果日志index也一样 按照文件的meta文件的名字排序(其实就是文件名字里边的毫秒数)
type snapMetaSlice []*fileSnapshotMeta

// FileSnapshotSink implements SnapshotSink with a file.
type FileSnapshotSink struct {
	store     *FileSnapshotStore //镜像群
	logger    hclog.Logger  //日志
	dir       string  //这个镜像的路径
	parentDir string //镜像群的路径
	meta      fileSnapshotMeta

	noSync bool

	stateFile *os.File
	stateHash hash.Hash64
	buffered  *bufio.Writer  /*是个multiwrite  一个write落地状态state文件(就是stateFile属性)  一个write来计算crc64(就是stateHash属性)
	buffered = bufio.NewWriter(io.MultiWriter(sink.stateFile, sink.stateHash))*/

	closed bool
}

// fileSnapshotMeta is stored on disk. We also put a CRC
// on disk so that we can verify the snapshot.
type fileSnapshotMeta struct {
	SnapshotMeta
	CRC []byte
}

// bufferedFile is returned when we open a snapshot. This way
// reads are buffered and the file still gets closed.
type bufferedFile struct {
	bh *bufio.Reader
	fh *os.File
}

func (b *bufferedFile) Read(p []byte) (n int, err error) {
	return b.bh.Read(p)
}

func (b *bufferedFile) Close() error {
	return b.fh.Close()
}

// NewFileSnapshotStoreWithLogger creates a new FileSnapshotStore based
// on a base directory. The `retain` parameter controls how many
// snapshots are retained. Must be at least 1.
//创建一个文件快照存储 还带有程序log功能 retain是确定保存多少个
func NewFileSnapshotStoreWithLogger(base string, retain int, logger hclog.Logger) (*FileSnapshotStore, error) {
	if retain < 1 {
		return nil, fmt.Errorf("must retain at least one snapshot")
	}
	if logger == nil {
		logger = hclog.New(&hclog.LoggerOptions{
			Name:   "snapshot",
			Output: hclog.DefaultOutput,
			Level:  hclog.DefaultLevel,
		})
	}

	// Ensure our path exists
	//"xxxxxxxxxxxxxxxxx/snapshots"目录
	path := filepath.Join(base, snapPath)

	//创建目录
	if err := os.MkdirAll(path, 0755); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("snapshot path not accessible: %v", err)
	}

	// Setup the store
	store := &FileSnapshotStore{
		path:   path,
		retain: retain,
		logger: logger,
	}

	// Do a permissions test
	//测试目录权限
	if err := store.testPermissions(); err != nil {
		return nil, fmt.Errorf("permissions test failed: %v", err)
	}
	return store, nil
}

// NewFileSnapshotStore creates a new FileSnapshotStore based
// on a base directory. The `retain` parameter controls how many
// snapshots are retained. Must be at least 1.
//创建一个文件快照存储 还带有程序log功能  程序log打印到stderr retain是确定保存多少个
func NewFileSnapshotStore(base string, retain int, logOutput io.Writer) (*FileSnapshotStore, error) {
	if logOutput == nil {
		logOutput = os.Stderr
	}
	return NewFileSnapshotStoreWithLogger(base, retain, hclog.New(&hclog.LoggerOptions{
		Name:   "snapshot",
		Output: logOutput,
		Level:  hclog.DefaultLevel,
	}))
}

// testPermissions tries to touch a file in our path to see if it works.
//在目录创建个文件xxxxxxx/permTest 后删除  来测试目录可创建+可删除
func (f *FileSnapshotStore) testPermissions() error {
	path := filepath.Join(f.path, testPath)
	fh, err := os.Create(path)
	if err != nil {
		return err
	}

	if err = fh.Close(); err != nil {
		return err
	}

	if err = os.Remove(path); err != nil {
		return err
	}
	return nil
}

// snapshotName generates a name for the snapshot.
//返回个快照文件的名字   任期-日志编号-微秒数
func snapshotName(term, index uint64) string {
	now := time.Now()
	msec := now.UnixNano() / int64(time.Millisecond)
	return fmt.Sprintf("%d-%d-%d", term, index, msec)
}

// Create is used to start a new snapshot
func (f *FileSnapshotStore) Create(version SnapshotVersion, index, term uint64,
	configuration Configuration, configurationIndex uint64, trans Transport) (SnapshotSink, error) {
	// We only support version 1 snapshots at this time.
	if version != 1 {
		return nil, fmt.Errorf("unsupported snapshot version %d", version)
	}

	// Create a new path
	name := snapshotName(term, index)
	path := filepath.Join(f.path, name+tmpSuffix)

	//path == xxxxxxxxxxxxxxxxx/snapshots/任期-日志编号-微秒数.tmp(这竟然是个目录)
	f.logger.Info("creating new snapshot", "path", path)

	// Make the directory
	if err := os.MkdirAll(path, 0755); err != nil {
		f.logger.Error("failed to make snapshot directly", "error", err)
		return nil, err
	}

	// Create the sink sink==落地的意思?
	sink := &FileSnapshotSink{
		store:     f,
		logger:    f.logger,
		dir:       path,
		parentDir: f.path,
		noSync:    f.noSync,
		meta: fileSnapshotMeta{
			SnapshotMeta: SnapshotMeta{
				Version:            version,
				ID:                 name,
				Index:              index,
				Term:               term,
				Peers:              encodePeers(configuration, trans),
				Configuration:      configuration,
				ConfigurationIndex: configurationIndex,
			},
			CRC: nil,
		},
	}

	// Write out the meta data
	if err := sink.writeMeta(); err != nil {
		f.logger.Error("failed to write metadata", "error", err)
		return nil, err
	}

	// Open the state file
	//xxxxxxxxxxxxxxxxx/snapshots/任期-日志编号-微秒数.tmp/state.bin
	statePath := filepath.Join(path, stateFilePath)
	fh, err := os.Create(statePath)
	if err != nil {
		f.logger.Error("failed to create state file", "error", err)
		return nil, err
	}
	sink.stateFile = fh

	// Create a CRC64 hash
	sink.stateHash = crc64.New(crc64.MakeTable(crc64.ECMA))

	// Wrap both the hash and file in a MultiWriter with buffering
	//2个writer合并在一起组成一个write (MultiWriter)  对MultiWriter写数据会并发写入2个writer
	//这2个writer 一个是上面的stateFile文件句柄
	//一个是crc64的write用来计算crc64
	multi := io.MultiWriter(sink.stateFile, sink.stateHash)
	sink.buffered = bufio.NewWriter(multi)

	// Done
	return sink, nil
}

// List returns available snapshots in the store.
func (f *FileSnapshotStore) List() ([]*SnapshotMeta, error) {
	// Get the eligible snapshots
	snapshots, err := f.getSnapshots()
	if err != nil {
		f.logger.Error("failed to get snapshots", "error", err)
		return nil, err
	}

	var snapMeta []*SnapshotMeta
	for _, meta := range snapshots {
		snapMeta = append(snapMeta, &meta.SnapshotMeta)
		if len(snapMeta) == f.retain {
			break
		}
	}
	return snapMeta, nil
}

// getSnapshots returns all the known snapshots.
//得到所有镜像的meta信息组成的数据
//meta信息通过读取所有的 xxxxxxxxxxxxxxxxx/snapshots/任期-日志编号-微秒数/meta.json 得到
func (f *FileSnapshotStore) getSnapshots() ([]*fileSnapshotMeta, error) {
	// Get the eligible snapshots
	//得到所有的xxxxxxxxxxxxxxxxx/snapshots/任期-日志编号-微秒数
	snapshots, err := ioutil.ReadDir(f.path)
	if err != nil {
		f.logger.Error("failed to scan snapshot directory", "error", err)
		return nil, err
	}

	// Populate the metadata
	var snapMeta []*fileSnapshotMeta
	for _, snap := range snapshots {
		// Ignore any files
		if !snap.IsDir() {
			continue
		}

		// Ignore any temporary snapshots
		dirName := snap.Name()
		if strings.HasSuffix(dirName, tmpSuffix) {
			f.logger.Warn("found temporary snapshot", "name", dirName)
			continue
		}

		// Try to read the meta data
		meta, err := f.readMeta(dirName)
		if err != nil {
			f.logger.Warn("failed to read metadata", "name", dirName, "error", err)
			continue
		}

		// Make sure we can understand this version.
		//现在仅支持 0 1 版本
		if meta.Version < SnapshotVersionMin || meta.Version > SnapshotVersionMax {
			f.logger.Warn("snapshot version not supported", "name", dirName, "version", meta.Version)
			continue
		}

		// Append, but only return up to the retain count
		snapMeta = append(snapMeta, meta)
	}

	// Sort the snapshot, reverse so we get new -> old
	//排序后还没有改变原来的数据类型slice
	//类型转换后排序新类型  新类型和老类型内存一样   老的类型内存也变动

	sort.Sort(sort.Reverse(snapMetaSlice(snapMeta)))

	return snapMeta, nil
}

// readMeta is used to read the meta data for a given named backup
//从xxxxxxxxxxxxxxxxx/snapshots/任期-日志编号-微秒数/meta.json中读取镜像的meta信息
//传的参数 name 就是任期-日志编号-微秒数
func (f *FileSnapshotStore) readMeta(name string) (*fileSnapshotMeta, error) {
	// Open the meta file
	metaPath := filepath.Join(f.path, name, metaFilePath)
	fh, err := os.Open(metaPath)
	if err != nil {
		return nil, err
	}
	defer fh.Close()

	// Buffer the file IO
	buffered := bufio.NewReader(fh)

	// Read in the JSON
	//直接从文件json反序列化成fileSnapshotMeta结构体
	meta := &fileSnapshotMeta{}
	dec := json.NewDecoder(buffered)
	if err := dec.Decode(meta); err != nil {
		return nil, err
	}
	return meta, nil
}

// Open takes a snapshot ID and returns a ReadCloser for that snapshot.
//打开一个镜像,返回镜像meta信息和  read+close接口对象(fd)
func (f *FileSnapshotStore) Open(id string) (*SnapshotMeta, io.ReadCloser, error) {
	// Get the metadata
	meta, err := f.readMeta(id)
	if err != nil {
		f.logger.Error("failed to get meta data to open snapshot", "error", err)
		return nil, nil, err
	}

	// Open the state file
	statePath := filepath.Join(f.path, id, stateFilePath)
	fh, err := os.Open(statePath)
	if err != nil {
		f.logger.Error("failed to open state file", "error", err)
		return nil, nil, err
	}

	// Create a CRC64 hash
	//计算state文件的crc64
	stateHash := crc64.New(crc64.MakeTable(crc64.ECMA))

	// Compute the hash
	_, err = io.Copy(stateHash, fh)
	if err != nil {
		f.logger.Error("failed to read state file", "error", err)
		fh.Close()
		return nil, nil, err
	}

	// Verify the hash
	//比较state文件的crc64和meta记录的crc64是否一致
	computed := stateHash.Sum(nil)
	if bytes.Compare(meta.CRC, computed) != 0 {
		f.logger.Error("CRC checksum failed", "stored", meta.CRC, "computed", computed)
		fh.Close()
		return nil, nil, fmt.Errorf("CRC mismatch")
	}

	// Seek to the start
	//移动到文件开始
	if _, err := fh.Seek(0, 0); err != nil {
		f.logger.Error("state file seek failed", "error", err)
		fh.Close()
		return nil, nil, err
	}

	// Return a buffered file
	//返回了一个实现  read close的对象
	//read还是bufio 结合比较巧妙
	// 系统自带的bufio是不支持close操作的  如果在系统里边传回就没法close了
	//而fd的读缓存比较弱
	buffered := &bufferedFile{
		bh: bufio.NewReader(fh),
		fh: fh,
	}

	return &meta.SnapshotMeta, buffered, nil
}

// ReapSnapshots reaps any snapshots beyond the retain count.
func (f *FileSnapshotStore) ReapSnapshots() error {
	snapshots, err := f.getSnapshots()
	if err != nil {
		f.logger.Error("failed to get snapshots", "error", err)
		return err
	}

	for i := f.retain; i < len(snapshots); i++ {
		path := filepath.Join(f.path, snapshots[i].ID)
		f.logger.Info("reaping snapshot", "path", path)
		if err := os.RemoveAll(path); err != nil {
			f.logger.Error("failed to reap snapshot", "path", path, "error", err)
			return err
		}
	}
	return nil
}

// ID returns the ID of the snapshot, can be used with Open()
// after the snapshot is finalized.
func (s *FileSnapshotSink) ID() string {
	return s.meta.ID
}

// Write is used to append to the state file. We write to the
// buffered IO object to reduce the amount of context switches.
func (s *FileSnapshotSink) Write(b []byte) (int, error) {
	return s.buffered.Write(b)
}

// Close is used to indicate a successful end.
func (s *FileSnapshotSink) Close() error {
	// Make sure close is idempotent
	if s.closed {
		return nil
	}
	s.closed = true

	// Close the open handles
	if err := s.finalize(); err != nil {
		s.logger.Error("failed to finalize snapshot", "error", err)
		if delErr := os.RemoveAll(s.dir); delErr != nil {
			s.logger.Error("failed to delete temporary snapshot directory", "path", s.dir, "error", delErr)
			return delErr
		}
		return err
	}

	// Write out the meta data
	if err := s.writeMeta(); err != nil {
		s.logger.Error("failed to write metadata", "error", err)
		return err
	}

	// Move the directory into place
	newPath := strings.TrimSuffix(s.dir, tmpSuffix)
	if err := os.Rename(s.dir, newPath); err != nil {
		s.logger.Error("failed to move snapshot into place", "error", err)
		return err
	}

	if !s.noSync && runtime.GOOS != "windows" { // skipping fsync for directory entry edits on Windows, only needed for *nix style file systems
		parentFH, err := os.Open(s.parentDir)
		defer parentFH.Close()
		if err != nil {
			s.logger.Error("failed to open snapshot parent directory", "path", s.parentDir, "error", err)
			return err
		}

		if err = parentFH.Sync(); err != nil {
			s.logger.Error("failed syncing parent directory", "path", s.parentDir, "error", err)
			return err
		}
	}

	// Reap any old snapshots
	if err := s.store.ReapSnapshots(); err != nil {
		return err
	}

	return nil
}

// Cancel is used to indicate an unsuccessful end.
func (s *FileSnapshotSink) Cancel() error {
	// Make sure close is idempotent
	if s.closed {
		return nil
	}
	s.closed = true

	// Close the open handles
	if err := s.finalize(); err != nil {
		s.logger.Error("failed to finalize snapshot", "error", err)
		return err
	}

	// Attempt to remove all artifacts
	return os.RemoveAll(s.dir)
}

// finalize is used to close all of our resources.
func (s *FileSnapshotSink) finalize() error {
	// Flush any remaining data
	if err := s.buffered.Flush(); err != nil {
		return err
	}

	// Sync to force fsync to disk
	if !s.noSync {
		if err := s.stateFile.Sync(); err != nil {
			return err
		}
	}

	// Get the file size
	stat, statErr := s.stateFile.Stat()

	// Close the file
	if err := s.stateFile.Close(); err != nil {
		return err
	}

	// Set the file size, check after we close
	if statErr != nil {
		return statErr
	}
	s.meta.Size = stat.Size()

	// Set the CRC
	s.meta.CRC = s.stateHash.Sum(nil)
	return nil
}

// writeMeta is used to write out the metadata we have.
func (s *FileSnapshotSink) writeMeta() error {
	var err error
	// Open the meta file
	metaPath := filepath.Join(s.dir, metaFilePath)
	var fh *os.File
	fh, err = os.Create(metaPath)
	if err != nil {
		return err
	}
	defer fh.Close()

	// Buffer the file IO
	buffered := bufio.NewWriter(fh)

	// Write out as JSON
	enc := json.NewEncoder(buffered)
	if err = enc.Encode(&s.meta); err != nil {
		return err
	}

	if err = buffered.Flush(); err != nil {
		return err
	}

	if !s.noSync {
		if err = fh.Sync(); err != nil {
			return err
		}
	}

	return nil
}

// Implement the sort interface for []*fileSnapshotMeta.
func (s snapMetaSlice) Len() int {
	return len(s)
}

func (s snapMetaSlice) Less(i, j int) bool {
	if s[i].Term != s[j].Term {
		return s[i].Term < s[j].Term
	}
	if s[i].Index != s[j].Index {
		return s[i].Index < s[j].Index
	}
	return s[i].ID < s[j].ID
}

func (s snapMetaSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
