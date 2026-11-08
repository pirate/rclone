//go:build !windows

package vfs

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	_ "github.com/rclone/rclone/backend/s3"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fstest"
	"github.com/rclone/rclone/fstest/testserver"
	"github.com/rclone/rclone/fstest/testy"
	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/stretchr/testify/require"
)

// TestFileModTimeTCPForwarder forwards real MinIO traffic in a process which
// can be suspended without affecting other tests using the shared server.
func TestFileModTimeTCPForwarder(t *testing.T) {
	upstream := os.Getenv("RCLONE_TEST_MODTIME_UPSTREAM")
	if upstream == "" {
		return
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	fmt.Println(listener.Addr().String())
	for {
		client, err := listener.Accept()
		require.NoError(t, err)
		go func() {
			defer client.Close()
			server, err := net.Dial("tcp", upstream)
			if err != nil {
				return
			}
			defer server.Close()
			go func() { _, _ = io.Copy(server, client) }()
			_, _ = io.Copy(client, server)
		}()
	}
}

func fileModTimeForwarder(t *testing.T, upstream string) (string, *os.Process) {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestFileModTimeTCPForwarder$")
	command.Env = append(os.Environ(), "RCLONE_TEST_MODTIME_UPSTREAM="+upstream)
	stdout, err := command.StdoutPipe()
	require.NoError(t, err)
	command.Stderr = os.Stderr
	require.NoError(t, command.Start())
	t.Cleanup(func() {
		_ = command.Process.Signal(syscall.SIGCONT)
		_ = command.Process.Signal(syscall.SIGTERM)
		_ = command.Wait()
	})
	address, err := bufio.NewReader(stdout).ReadString('\n')
	require.NoError(t, err)
	return "http://" + strings.TrimSpace(address), command.Process
}

const fileContextContents = "preserved metadata and contents"

func fileContextS3(t *testing.T) (*File, *os.Process, time.Time) {
	t.Helper()
	return fileContextS3Proxy(t, nil)
}

func fileContextS3Proxy(t *testing.T, proxy func(*url.URL) string) (*File, *os.Process, time.Time) {
	t.Helper()
	testy.SkipUnlessDocker(t)
	stop, err := testserver.Start("TestS3Minio:")
	require.NoError(t, err)
	t.Cleanup(stop)
	ctx, ci := fs.AddConfig(context.Background())
	ci.UseServerModTime = false
	remoteName, _, err := fstest.RandomRemoteName("TestS3Minio:")
	require.NoError(t, err)
	remote, err := fs.NewFs(ctx, remoteName)
	require.NoError(t, err)
	require.NoError(t, remote.Mkdir(ctx, ""))
	t.Cleanup(func() { require.NoError(t, operations.Purge(ctx, remote, "")) })
	wantTime := time.Date(2001, 2, 3, 4, 5, 6, 0, time.UTC)
	_, err = remote.Put(ctx, strings.NewReader(fileContextContents), object.NewStaticObjectInfo("file.txt", wantTime, int64(len(fileContextContents)), true, nil, remote))
	require.NoError(t, err)
	endpoint, err := url.Parse(os.Getenv("RCLONE_CONFIG_TESTS3MINIO_ENDPOINT"))
	require.NoError(t, err)
	var address string
	var forwarder *os.Process
	if proxy == nil {
		address, forwarder = fileModTimeForwarder(t, endpoint.Host)
	} else {
		address = proxy(endpoint)
	}
	_, bucket, found := strings.Cut(remoteName, ":")
	require.True(t, found)
	proxied, err := fs.NewFs(ctx, "TestS3Minio,endpoint='"+address+"':"+bucket)
	require.NoError(t, err)
	vfs := New(ctx, proxied, nil)
	t.Cleanup(func() {
		if forwarder != nil {
			_ = forwarder.Signal(syscall.SIGCONT)
		}
		require.NoError(t, vfs.CleanUp())
		vfs.Shutdown()
	})
	root, err := vfs.Root()
	require.NoError(t, err)
	node, err := root.StatContext(context.Background(), "file.txt")
	require.NoError(t, err)
	return node.(*File), forwarder, wantTime
}

func TestFileModTimeCancelsPendingMetadata(t *testing.T) {
	file, forwarder, wantTime := fileContextS3(t)

	// Listing S3 objects leaves their user metadata to a subsequent HEAD.
	// Suspend only this test's byte-forwarding process, not shared MinIO.
	require.NoError(t, forwarder.Signal(syscall.SIGSTOP))
	request, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		_, err := file.ModTimeContext(request)
		finished <- err
	}()
	select {
	case err := <-finished:
		t.Fatalf("metadata did not wait for its suspended connection: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-finished:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		require.NoError(t, forwarder.Signal(syscall.SIGCONT))
		<-finished
		t.Fatal("canceled metadata request waited for backend I/O")
	}
	require.NoError(t, forwarder.Signal(syscall.SIGCONT))
	modTime, err := file.ModTimeContext(context.Background())
	require.NoError(t, err)
	require.True(t, wantTime.Equal(modTime))
	require.True(t, wantTime.Equal(file.ModTime()))
	_, err = file.ModTimeContext(request)
	require.ErrorIs(t, err, context.Canceled)
	handle, err := file.Open(os.O_RDONLY)
	require.NoError(t, err)
	got, err := io.ReadAll(handle)
	require.NoError(t, handle.Close())
	require.NoError(t, err)
	require.Equal(t, fileContextContents, string(got))
}

func TestFileModTimeCancelsWhileSettingModTime(t *testing.T) {
	file, forwarder, wantTime := fileContextS3(t)
	modTime, err := file.ModTimeContext(context.Background())
	require.NoError(t, err)
	require.True(t, wantTime.Equal(modTime))
	wantTime = wantTime.Add(time.Hour)
	require.NoError(t, forwarder.Signal(syscall.SIGSTOP))
	owner := make(chan error, 1)
	go func() { owner <- file.SetModTime(wantTime) }()
	select {
	case err := <-owner:
		t.Fatalf("SetModTime did not wait for its suspended connection: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	request, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 2)
	go func() {
		_, err := file.ModTimeContext(request)
		finished <- err
	}()
	go func() {
		_, err := file.AttrContext(request)
		finished <- err
	}()
	select {
	case err := <-finished:
		t.Fatalf("metadata did not wait for SetModTime: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	cancel()
	for range 2 {
		select {
		case err := <-finished:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(2 * time.Second):
			require.NoError(t, forwarder.Signal(syscall.SIGCONT))
			require.NoError(t, <-owner)
			<-finished
			t.Fatal("canceled metadata lookup waited for unrelated SetModTime")
		}
	}
	select {
	case err := <-owner:
		t.Fatalf("canceling the metadata lookup interrupted SetModTime: %v", err)
	default:
	}
	require.NoError(t, forwarder.Signal(syscall.SIGCONT))
	require.NoError(t, <-owner)
	modTime, err = file.ModTimeContext(context.Background())
	require.NoError(t, err)
	require.True(t, wantTime.Equal(modTime))
	attr, err := file.AttrContext(context.Background())
	require.NoError(t, err)
	require.Equal(t, file.Size(), attr.Size)
	require.Equal(t, file.Mode(), attr.Mode)
	require.True(t, wantTime.Equal(attr.ModTime))
	handle, err := file.Open(os.O_RDONLY)
	require.NoError(t, err)
	contents, err := io.ReadAll(handle)
	require.NoError(t, handle.Close())
	require.NoError(t, err)
	require.Equal(t, fileContextContents, string(contents))
}

func TestFileAttrCancelsPendingCacheFingerprint(t *testing.T) {
	file, forwarder, wantTime := fileContextS3(t)
	vfs := file.VFS()
	vfs.Opt.FastFingerprint = false
	vfs.Opt.HandleCaching = 0
	vfs.SetCacheMode(vfscommon.CacheModeFull)
	handle, err := file.Open(os.O_RDONLY)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = forwarder.Signal(syscall.SIGCONT)
		require.NoError(t, handle.Close())
	})
	require.NoError(t, forwarder.Signal(syscall.SIGSTOP))
	type readResult struct {
		contents []byte
		err      error
	}
	owner := make(chan readResult, 1)
	go func() {
		contents, err := io.ReadAll(handle)
		owner <- readResult{contents, err}
	}()
	select {
	case got := <-owner:
		t.Fatalf("cached read did not wait for its suspended connection: %v", got.err)
	case <-time.After(100 * time.Millisecond):
	}
	request, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		_, err := file.AttrContext(request)
		finished <- err
	}()
	select {
	case err := <-finished:
		t.Fatalf("attributes did not wait for cache fingerprint: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-finished:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		require.NoError(t, forwarder.Signal(syscall.SIGCONT))
		got := <-owner
		require.NoError(t, got.err)
		require.Equal(t, fileContextContents, string(got.contents))
		<-finished
		t.Fatal("canceled attributes waited for unrelated cache fingerprint")
	}
	select {
	case got := <-owner:
		t.Fatalf("canceling attributes interrupted cached read: %v", got.err)
	default:
	}
	require.NoError(t, forwarder.Signal(syscall.SIGCONT))
	got := <-owner
	require.NoError(t, got.err)
	require.Equal(t, fileContextContents, string(got.contents))
	attr, err := file.AttrContext(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(len(fileContextContents)), attr.Size)
	require.True(t, wantTime.Equal(attr.ModTime))
}

func TestDirStatCancelsWhileUpdatingFile(t *testing.T) {
	t.Run("Listed", func(t *testing.T) { testDirStatCancelsWhileUpdatingFile(t, false, false) })
	t.Run("Virtual", func(t *testing.T) { testDirStatCancelsWhileUpdatingFile(t, true, false) })
	t.Run("Metadata", func(t *testing.T) { testDirStatCancelsWhileUpdatingFile(t, false, true) })
}

func testDirStatCancelsWhileUpdatingFile(t *testing.T, virtual, metadata bool) {
	copyStarted := make(chan struct{})
	resumeCopy := make(chan struct{})
	var resumeOnce, startedOnce sync.Once
	resume := func() { resumeOnce.Do(func() { close(resumeCopy) }) }
	file, _, wantTime := fileContextS3Proxy(t, func(endpoint *url.URL) string {
		proxy := httputil.NewSingleHostReverseProxy(endpoint)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Hold the real CopyObject request while forwarding real listings.
			if r.Method == http.MethodPut && r.Header.Get("X-Amz-Copy-Source") != "" {
				startedOnce.Do(func() { close(copyStarted) })
				<-resumeCopy
			}
			proxy.ServeHTTP(w, r)
		}))
		t.Cleanup(func() {
			resume()
			server.Close()
		})
		return server.URL
	})
	lookupName := "file.txt"
	if metadata {
		file.VFS().Opt.MetadataExtension = ".meta"
		lookupName += ".meta"
	}
	// Resume before the VFS and remote cleanup, even if an assertion fails.
	t.Cleanup(resume)
	if virtual {
		file.VFS().Opt.HandleCaching = 0
		file.VFS().SetCacheMode(vfscommon.CacheModeFull)
		handle, err := file.Open(os.O_RDONLY)
		require.NoError(t, err)
		contents, err := io.ReadAll(handle)
		require.NoError(t, handle.Close())
		require.NoError(t, err)
		require.Equal(t, fileContextContents, string(contents))
		root, err := file.VFS().Root()
		require.NoError(t, err)
		root.mu.RLock()
		virtualState := root.virtual["file.txt"]
		root.mu.RUnlock()
		require.Equal(t, vAddFile, virtualState)
	}
	modTime, err := file.ModTimeContext(context.Background())
	require.NoError(t, err)
	require.True(t, wantTime.Equal(modTime))
	wantTime = wantTime.Add(time.Hour)
	owner := make(chan error, 1)
	go func() { owner <- file.SetModTime(wantTime) }()
	select {
	case <-copyStarted:
	case err := <-owner:
		t.Fatalf("SetModTime did not send CopyObject: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("SetModTime did not reach CopyObject")
	}
	root, err := file.d.vfs.Root()
	require.NoError(t, err)
	if !metadata {
		root.mu.Lock()
		root.read = time.Time{}
		root.mu.Unlock()
	}
	request, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		_, err := root.StatContext(request, lookupName)
		finished <- err
	}()
	select {
	case err := <-finished:
		t.Fatalf("directory refresh did not wait for SetModTime: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-finished:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		resume()
		require.NoError(t, <-owner)
		<-finished
		t.Fatal("canceled directory refresh waited for unrelated SetModTime")
	}
	select {
	case err := <-owner:
		t.Fatalf("canceling the directory refresh interrupted SetModTime: %v", err)
	default:
	}
	root.mu.RLock()
	read := root.read
	root.mu.RUnlock()
	if !metadata {
		require.True(t, read.IsZero(), "canceled refresh must remain stale")
	}
	resume()
	require.NoError(t, <-owner)
	node, err := root.StatContext(context.Background(), "file.txt")
	require.NoError(t, err)
	attr, err := node.(*File).AttrContext(context.Background())
	require.NoError(t, err)
	require.True(t, wantTime.Equal(attr.ModTime))
	handle, err := node.Open(os.O_RDONLY)
	require.NoError(t, err)
	contents, err := io.ReadAll(handle)
	require.NoError(t, handle.Close())
	require.NoError(t, err)
	require.Equal(t, fileContextContents, string(contents))
}

func TestFileAttrSizeAfterMetadata(t *testing.T) {
	file, _, wantTime := fileContextS3(t)
	ctx := context.Background()
	remote := file.VFS().Fs()
	contents := fileContextContents + " changed after the directory listing"
	wantTime = wantTime.Add(time.Hour)
	_, err := remote.Put(ctx, strings.NewReader(contents), object.NewStaticObjectInfo("file.txt", wantTime, int64(len(contents)), true, nil, remote))
	require.NoError(t, err)
	// The cached listing has the old size; the real metadata HEAD refreshes it.
	require.Equal(t, int64(len(fileContextContents)), file.Size())
	attr, err := file.AttrContext(ctx)
	require.NoError(t, err)
	require.True(t, wantTime.Equal(attr.ModTime))
	require.Equal(t, int64(len(contents)), file.Size())
	require.Equal(t, int64(len(contents)), attr.Size)
	handle, err := file.Open(os.O_RDONLY)
	require.NoError(t, err)
	got, err := io.ReadAll(handle)
	require.NoError(t, handle.Close())
	require.NoError(t, err)
	require.Equal(t, contents, string(got))
}
