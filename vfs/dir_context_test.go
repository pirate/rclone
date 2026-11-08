//go:build !windows

package vfs

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	_ "github.com/rclone/rclone/backend/http"
	"github.com/rclone/rclone/fs"
	"github.com/stretchr/testify/require"
)

// TestDirStatHTTPServer runs a real file server in a separate process so the
// cancellation test can suspend the backend without replacing its handlers.
func TestDirStatHTTPServer(t *testing.T) {
	root := os.Getenv("RCLONE_TEST_STAT_HTTP_ROOT")
	if root == "" {
		return
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	fmt.Println("http://" + listener.Addr().String() + "/")
	files := http.FileServer(http.Dir(root))
	var pause sync.Once
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if os.Getenv("RCLONE_TEST_STAT_HTTP_PAUSE_HEAD") == "1" && r.Method == http.MethodHead && r.URL.Path == "/file.txt" {
			pause.Do(func() {
				if err := syscall.Kill(os.Getpid(), syscall.SIGSTOP); err != nil {
					panic(err)
				}
			})
		}
		files.ServeHTTP(w, r)
	})
	require.NoError(t, http.Serve(listener, handler))
}

func dirStatHTTPBackend(t *testing.T, pauseHead bool) (*Dir, *os.Process) {
	t.Helper()
	source := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(source, "file.txt"), []byte("preserved"), 0600))
	server := exec.Command(os.Args[0], "-test.run=^TestDirStatHTTPServer$")
	server.Env = append(os.Environ(), "RCLONE_TEST_STAT_HTTP_ROOT="+source)
	if pauseHead {
		server.Env = append(server.Env, "RCLONE_TEST_STAT_HTTP_PAUSE_HEAD=1")
	}
	stdout, err := server.StdoutPipe()
	require.NoError(t, err)
	server.Stderr = os.Stderr
	require.NoError(t, server.Start())
	t.Cleanup(func() {
		_ = server.Process.Signal(syscall.SIGCONT)
		_ = server.Process.Signal(syscall.SIGTERM)
		_ = server.Wait()
	})
	reader := bufio.NewReader(stdout)
	address, err := reader.ReadString('\n')
	require.NoError(t, err)
	ctx := context.Background()
	remote, err := fs.NewFs(ctx, ":http,url='"+strings.TrimSpace(address)+"':")
	require.NoError(t, err)
	vfs := New(ctx, remote, nil)
	t.Cleanup(func() {
		_ = server.Process.Signal(syscall.SIGCONT)
		vfs.Shutdown()
	})
	root, err := vfs.Root()
	require.NoError(t, err)

	return root, server.Process
}

func TestDirStatCancelsPendingListing(t *testing.T) {
	root, server := dirStatHTTPBackend(t, false)
	ctx := context.Background()

	// The TCP listener remains open, but the real server cannot answer the
	// directory listing until resumed. No timeout or fake backend releases it.
	require.NoError(t, server.Signal(syscall.SIGSTOP))
	request, cancel := context.WithCancel(ctx)
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		_, err := root.StatContext(request, "file.txt")
		finished <- err
	}()
	select {
	case err := <-finished:
		t.Fatalf("listing did not wait for its suspended backend: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	canceledAt := time.Now()
	cancel()
	select {
	case err := <-finished:
		t.Logf("pending backend lookup canceled in %s", time.Since(canceledAt))
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		// Resume before failing so the original regression cannot strand a
		// test goroutine or VFS cleanup behind the directory mutex.
		require.NoError(t, server.Signal(syscall.SIGCONT))
		<-finished
		t.Fatal("canceled lookup waited for backend I/O")
	}
	require.NoError(t, server.Signal(syscall.SIGCONT))
	node, err := root.Stat("file.txt")
	require.NoError(t, err)
	require.Equal(t, int64(len("preserved")), node.Size())
	file, err := node.Open(os.O_RDONLY)
	require.NoError(t, err)
	contents, err := io.ReadAll(file)
	require.NoError(t, file.Close())
	require.NoError(t, err)
	require.Equal(t, "preserved", string(contents))
	_, err = root.StatContext(request, "file.txt")
	require.ErrorIs(t, err, context.Canceled)
	_, err = root.StatContext(context.Background(), "file.txt")
	require.NoError(t, err)
}

func TestDirStatCancelsWhileWaitingForDirectory(t *testing.T) {
	root, server := dirStatHTTPBackend(t, false)
	require.NoError(t, server.Signal(syscall.SIGSTOP))
	listing := make(chan error, 1)
	go func() {
		_, err := root.ReadDirAll()
		listing <- err
	}()
	select {
	case err := <-listing:
		t.Fatalf("listing did not wait for its suspended backend: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	request, cancel := context.WithCancel(context.Background())
	defer cancel()
	lookup := make(chan error, 1)
	go func() {
		_, err := root.StatContext(request, "file.txt")
		lookup <- err
	}()
	select {
	case err := <-lookup:
		t.Fatalf("lookup did not wait for the busy directory: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	canceledAt := time.Now()
	cancel()
	select {
	case err := <-lookup:
		t.Logf("queued directory lookup canceled in %s", time.Since(canceledAt))
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		require.NoError(t, server.Signal(syscall.SIGCONT))
		<-listing
		<-lookup
		t.Fatal("canceled lookup waited for another directory operation")
	}
	// Canceling one waiter must not cancel the unrelated listing or release
	// the lock it owns. Resuming the real backend must complete that listing.
	select {
	case err := <-listing:
		t.Fatalf("canceling the lookup interrupted the directory owner: %v", err)
	default:
	}
	require.NoError(t, server.Signal(syscall.SIGCONT))
	require.NoError(t, <-listing)
	node, err := root.Stat("file.txt")
	require.NoError(t, err)
	require.Equal(t, int64(len("preserved")), node.Size())
	file, err := node.Open(os.O_RDONLY)
	require.NoError(t, err)
	contents, err := io.ReadAll(file)
	require.NoError(t, file.Close())
	require.NoError(t, err)
	require.Equal(t, "preserved", string(contents))
}

func TestDirStatCanceledListingDoesNotRefreshCache(t *testing.T) {
	root, server := dirStatHTTPBackend(t, true)
	request, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		_, err := root.StatContext(request, "file.txt")
		finished <- err
	}()
	// The real directory response has arrived. Its per-file HEAD is now
	// suspended, where the HTTP backend can swallow a cancellation error.
	var waitErr error
	require.Eventually(t, func() bool {
		var status syscall.WaitStatus
		pid, err := syscall.Wait4(server.Pid, &status, syscall.WNOHANG|syscall.WUNTRACED, nil)
		if err != nil {
			waitErr = err
			return true
		}
		// WCONTINUED is not requested, so a live-child event is a stop.
		// Darwin's WaitStatus helpers classify SIGSTOP as Continued.
		return pid == server.Pid && !status.Exited() && !status.Signaled()
	}, 2*time.Second, 10*time.Millisecond)
	require.NoError(t, waitErr)
	cancel()
	select {
	case err := <-finished:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		require.NoError(t, server.Signal(syscall.SIGCONT))
		<-finished
		t.Fatal("canceled HEAD lookup did not finish")
	}
	root.mu.RLock()
	read := root.read
	root.mu.RUnlock()
	require.True(t, read.IsZero(), "canceled listing must not mark an empty cache fresh")
	require.NoError(t, server.Signal(syscall.SIGCONT))
	node, err := root.Stat("file.txt")
	require.NoError(t, err)
	file, err := node.Open(os.O_RDONLY)
	require.NoError(t, err)
	contents, err := io.ReadAll(file)
	require.NoError(t, file.Close())
	require.NoError(t, err)
	require.Equal(t, "preserved", string(contents))
}
