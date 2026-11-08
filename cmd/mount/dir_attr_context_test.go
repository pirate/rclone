//go:build linux

package mount

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"bazil.org/fuse"
	httpbackend "github.com/rclone/rclone/backend/http"
	"github.com/rclone/rclone/cmd/mountlib"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/vfs"
	"github.com/stretchr/testify/require"
)

// TestDirAttrDoesNotWaitForListing exercises attributes during a real HTTP listing.
func TestDirAttrDoesNotWaitForListing(t *testing.T) {
	directory := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(directory, "file.txt"), []byte("contents"), 0600))
	started, release := make(chan struct{}), make(chan struct{})
	var startOnce, releaseOnce sync.Once
	resume := func() { releaseOnce.Do(func() { close(release) }) }
	handler := http.FileServer(http.Dir(directory))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/" {
			startOnce.Do(func() { close(started) })
			<-release
		}
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(func() { resume(); server.Close() })
	ctx := context.Background()
	remote, err := httpbackend.NewFs(ctx, "callback-http", "", configmap.Simple{"url": server.URL})
	require.NoError(t, err)
	VFS := vfs.New(ctx, remote, nil)
	t.Cleanup(func() { VFS.Shutdown() })
	t.Cleanup(resume)
	root, err := VFS.Root()
	require.NoError(t, err)
	dir := &Dir{Dir: root, fsys: NewFS(VFS, &mountlib.Opt)}
	listing := make(chan error, 1)
	go func() { _, err := root.ReadDirAll(); listing <- err }()
	select {
	case <-started:
	case err := <-listing:
		t.Fatalf("listing did not reach HTTP server: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("listing did not reach HTTP server")
	}
	request, cancel := context.WithCancel(ctx)
	cancel()
	finished := make(chan error, 1)
	go func() { finished <- dir.Attr(request, &fuse.Attr{}) }()
	select {
	case err := <-finished:
		// Directory attributes are already available without the listing.
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		resume()
		require.NoError(t, <-listing)
		<-finished
		t.Fatal("canceled directory Attr waited for unrelated HTTP listing")
	}
	select {
	case err := <-listing:
		t.Fatalf("Attr interrupted unrelated HTTP listing: %v", err)
	default:
	}
	resume()
	require.NoError(t, <-listing)
}
