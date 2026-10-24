package core

import (
	"errors"
	"fmt"
	"io/fs"
	"mime"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
)

func newRavelFallback(rawURL string) (http.Handler, error) {
	origin, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	if origin.Opaque != "" || origin.Fragment != "" || origin.User != nil {
		return nil, errors.New("opaque data, fragment, and userinfo are not allowed")
	}
	switch origin.Scheme {
	case "file":
		root, err := ravelFileURLPath(origin)
		if err != nil {
			return nil, err
		}
		return newRavelSafeFileHandler(root)
	case "http", "https":
		if origin.Host == "" {
			return nil, errors.New("http(s) masquerade URL requires an origin host")
		}
		proxy := httputil.NewSingleHostReverseProxy(origin)
		director := proxy.Director
		proxy.Director = func(request *http.Request) {
			director(request)
			request.Host = origin.Host
			request.Header.Del("Forwarded")
			request.Header.Del("X-Forwarded-Host")
			request.Header.Del("X-Forwarded-Proto")
			request.Header["X-Forwarded-For"] = nil
		}
		proxy.ErrorHandler = func(writer http.ResponseWriter, _ *http.Request, _ error) {
			http.Error(writer, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
		}
		return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			_ = http.NewResponseController(writer).EnableFullDuplex()
			proxy.ServeHTTP(writer, request)
		}), nil
	default:
		return nil, errors.New("masquerade URL scheme must be file, http, or https")
	}
}

func ravelFileURLPath(origin *url.URL) (string, error) {
	if origin.Host != "" && origin.Host != "localhost" {
		return "", errors.New("file masquerade URL host must be empty or localhost")
	}
	if origin.RawQuery != "" {
		return "", errors.New("file masquerade URL must not contain a query")
	}
	decoded, err := url.PathUnescape(origin.EscapedPath())
	if err != nil {
		return "", fmt.Errorf("invalid file URL path: %w", err)
	}
	if runtime.GOOS == "windows" &&
		len(decoded) >= 3 &&
		decoded[0] == '/' &&
		decoded[2] == ':' {
		decoded = decoded[1:]
	}
	if decoded == "" {
		return "", errors.New("file masquerade URL requires an absolute directory")
	}
	root := filepath.FromSlash(decoded)
	if !filepath.IsAbs(root) {
		return "", errors.New("file masquerade URL requires an absolute directory")
	}
	return root, nil
}

type ravelSafeFileHandler struct {
	root string
}

func newRavelSafeFileHandler(root string) (http.Handler, error) {
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return nil, fmt.Errorf("resolve file masquerade root: %w", err)
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, errors.New("file masquerade root must be a directory")
	}
	return &ravelSafeFileHandler{root: filepath.Clean(resolved)}, nil
}

func (handler *ravelSafeFileHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet && request.Method != http.MethodHead {
		writer.Header().Set("Allow", "GET, HEAD")
		http.Error(writer, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	cleanURLPath := path.Clean("/" + request.URL.Path)
	relative := strings.TrimPrefix(cleanURLPath, "/")
	if relative == "" {
		relative = "."
	}
	file, info, err := handler.openFile(relative)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			http.NotFound(writer, request)
			return
		}
		http.Error(writer, http.StatusText(http.StatusForbidden), http.StatusForbidden)
		return
	}
	if info.IsDir() {
		file.Close()
		relative = path.Join(relative, "index.html")
		file, info, err = handler.openFile(relative)
		if err != nil {
			http.NotFound(writer, request)
			return
		}
	}
	defer file.Close()
	if !info.Mode().IsRegular() {
		http.NotFound(writer, request)
		return
	}
	if contentType := mime.TypeByExtension(path.Ext(relative)); contentType != "" {
		writer.Header().Set("Content-Type", contentType)
	}
	http.ServeContent(writer, request, info.Name(), info.ModTime(), file)
}

func (handler *ravelSafeFileHandler) openFile(relative string) (*os.File, fs.FileInfo, error) {
	local, err := filepath.Localize(relative)
	if err != nil {
		return nil, nil, fs.ErrPermission
	}
	root, err := os.OpenRoot(handler.root)
	if err != nil {
		return nil, nil, err
	}
	defer root.Close()
	current := "."
	for _, component := range strings.Split(local, string(filepath.Separator)) {
		if component == "" || component == "." {
			continue
		}
		current = filepath.Join(current, component)
		info, err := root.Lstat(current)
		if err != nil {
			return nil, nil, err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, nil, fs.ErrPermission
		}
	}
	// Root.Open also confines resolution if a component changes after Lstat.
	file, err := root.Open(local)
	if err != nil {
		return nil, nil, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, nil, err
	}
	return file, info, nil
}
