//go:build linux

package mount2

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	fusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	_ "github.com/rclone/rclone/backend/s3"
	"github.com/rclone/rclone/cmd/mountlib"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fstest"
	"github.com/rclone/rclone/fstest/testserver"
	"github.com/rclone/rclone/fstest/testy"
	"github.com/rclone/rclone/vfs"
	"github.com/stretchr/testify/require"
)

// TestMain selects logging before any VFS background goroutines start.
func TestMain(m *testing.M) {
	if os.Getenv("RCLONE_TEST_CALLBACK_DEBUG") != "" {
		fs.GetConfig(context.Background()).LogLevel = fs.LogLevelDebug
	}
	os.Exit(m.Run())
}

// TestLookupCallbackCancellation exercises the userspace callbacks without a FUSE mount.
func TestLookupCallbackCancellation(t *testing.T) {
	for _, stale := range []bool{false, true} {
		name := "CachedDirectory"
		if stale {
			name = "StaleDirectory"
		}
		t.Run(name, func(t *testing.T) { testLookupCallbackCancellation(t, stale) })
	}
}

func testLookupCallbackCancellation(t *testing.T, stale bool) {
	// A separately managed fixture lets an unprivileged Linux runner use MinIO
	// without receiving access to the Docker socket.
	if os.Getenv("RCLONE_TEST_CALLBACK_EXTERNAL_S3") == "" {
		testy.SkipUnlessDocker(t)
		stop, err := testserver.Start("TestS3Minio:")
		require.NoError(t, err)
		t.Cleanup(stop)
	}
	ctx, ci := fs.AddConfig(context.Background())
	ci.UseServerModTime = false
	remoteName, _, err := fstest.RandomRemoteName("TestS3Minio:")
	require.NoError(t, err)
	remote, err := fs.NewFs(ctx, remoteName)
	require.NoError(t, err)
	require.NoError(t, remote.Mkdir(ctx, ""))
	t.Cleanup(func() { require.NoError(t, operations.Purge(ctx, remote, "")) })
	const contents = "callback cancellation preserves real object contents"
	originalTime := time.Date(2001, 2, 3, 4, 5, 6, 0, time.UTC)
	_, err = remote.Put(ctx, strings.NewReader(contents), object.NewStaticObjectInfo("file.txt", originalTime, int64(len(contents)), true, nil, remote))
	require.NoError(t, err)
	endpoint, err := url.Parse(os.Getenv("RCLONE_CONFIG_TESTS3MINIO_ENDPOINT"))
	require.NoError(t, err)
	copyStarted, resumeCopy := make(chan struct{}), make(chan struct{})
	var startedOnce, resumeOnce sync.Once
	resume := func() { resumeOnce.Do(func() { close(resumeCopy) }) }
	proxy := httputil.NewSingleHostReverseProxy(endpoint)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && r.Header.Get("X-Amz-Copy-Source") != "" {
			startedOnce.Do(func() { close(copyStarted) })
			<-resumeCopy
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(func() { resume(); server.Close() })
	_, bucket, found := strings.Cut(remoteName, ":")
	require.True(t, found)
	proxied, err := fs.NewFs(ctx, "TestS3Minio,endpoint='"+server.URL+"':"+bucket)
	require.NoError(t, err)
	VFS := vfs.New(ctx, proxied, nil)
	t.Cleanup(func() { require.NoError(t, VFS.CleanUp()); VFS.Shutdown() })
	// Release the backend before any VFS or server cleanup on a failing assertion.
	t.Cleanup(resume)
	root, err := VFS.Root()
	require.NoError(t, err)
	child, err := root.StatContext(ctx, "file.txt")
	require.NoError(t, err)
	file := child.(*vfs.File)
	initial, err := file.ModTimeContext(ctx)
	require.NoError(t, err)
	require.True(t, originalTime.Equal(initial))
	fsys := NewFS(VFS, &mountlib.Opt)
	dir := newNode(fsys, root)
	// Initialize the real go-fuse inode bridge without opening /dev/fuse.
	_ = fusefs.NewNodeFS(dir, &fusefs.Options{})
	callback := func(ctx context.Context) error {
		_, errno := dir.Lookup(ctx, "file.txt", &fuse.EntryOut{})
		if errno != 0 {
			return errno
		}
		return nil
	}
	if stale {
		VFS.Opt.DirCacheTime = 0
	}
	wantTime := originalTime.Add(time.Hour)
	owner := make(chan error, 1)
	go func() { owner <- file.SetModTime(wantTime) }()
	select {
	case <-copyStarted:
	case err := <-owner:
		t.Fatalf("SetModTime did not send CopyObject: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("SetModTime did not reach CopyObject")
	}
	request, cancel := context.WithCancel(ctx)
	defer cancel()
	finished := make(chan error, 1)
	go func() { finished <- callback(request) }()
	select {
	case err := <-finished:
		t.Fatalf("callback did not wait for SetModTime: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-finished:
		require.Equal(t, syscall.EINTR, err)
	case <-time.After(2 * time.Second):
		resume()
		select {
		case <-owner:
		case <-time.After(10 * time.Second):
			t.Fatal("SetModTime did not complete after releasing failed callback")
		}
		select {
		case <-finished:
		case <-time.After(10 * time.Second):
			t.Fatal("callback did not complete after releasing CopyObject")
		}
		t.Fatal("canceled callback waited for unrelated SetModTime")
	}
	select {
	case err := <-owner:
		t.Fatalf("canceling lookup interrupted SetModTime: %v", err)
	default:
	}
	resume()
	select {
	case err := <-owner:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("SetModTime did not complete after CopyObject resumed")
	}
	require.NoError(t, callback(ctx))
	// Verify persisted metadata and bytes independently of the VFS cache.
	obj, err := remote.NewObject(ctx, "file.txt")
	require.NoError(t, err)
	require.True(t, wantTime.Equal(obj.ModTime(ctx)))
	reader, err := obj.Open(ctx)
	require.NoError(t, err)
	got, err := io.ReadAll(reader)
	require.NoError(t, reader.Close())
	require.NoError(t, err)
	require.Equal(t, contents, string(got))
}
