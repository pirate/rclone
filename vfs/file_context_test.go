//go:build !windows

package vfs

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
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

func TestFileModTimeCancelsPendingMetadata(t *testing.T) {
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
	const contents = "preserved metadata and contents"
	wantTime := time.Date(2001, 2, 3, 4, 5, 6, 0, time.UTC)
	_, err = remote.Put(ctx, strings.NewReader(contents), object.NewStaticObjectInfo("file.txt", wantTime, int64(len(contents)), true, nil, remote))
	require.NoError(t, err)
	endpoint, err := url.Parse(os.Getenv("RCLONE_CONFIG_TESTS3MINIO_ENDPOINT"))
	require.NoError(t, err)
	address, forwarder := fileModTimeForwarder(t, endpoint.Host)
	_, bucket, found := strings.Cut(remoteName, ":")
	require.True(t, found)
	proxied, err := fs.NewFs(ctx, "TestS3Minio,endpoint='"+address+"':"+bucket)
	require.NoError(t, err)
	vfs := New(ctx, proxied, nil)
	t.Cleanup(func() {
		_ = forwarder.Signal(syscall.SIGCONT)
		vfs.Shutdown()
	})
	root, err := vfs.Root()
	require.NoError(t, err)
	node, err := root.StatContext(context.Background(), "file.txt")
	require.NoError(t, err)
	file := node.(*File)

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
	require.Equal(t, contents, string(got))
}
