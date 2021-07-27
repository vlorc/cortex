package qiniu

import (
	"bytes"
	"context"
	"fmt"
	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/pkg/errors"
	"github.com/qiniu/go-sdk/v7/auth"
	"github.com/qiniu/go-sdk/v7/storage"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type qiniuClient struct {
	mac      *auth.Credentials
	manager  *storage.BucketManager
	uploader *storage.FormUploader
	bucket   string
	base     string
	token    string
	last     int64
	client   *http.Client
	mkUrl    func(*auth.Credentials, string, string) string
}

type listFilesRet struct {
	Marker         string             `json:"marker"`
	Items          []storage.ListItem `json:"items"`
	CommonPrefixes []string           `json:"commonPrefixes"`
}

var _ chunk.ObjectClient = &qiniuClient{}

func NewQiniuObjectClient(cfg Config) (chunk.ObjectClient, error) {
	q := &qiniuClient{
		mac:    auth.New(cfg.Access, cfg.Secret),
		bucket: cfg.Bucket,
		base:   cfg.Url,
		client: &http.Client{Transport: http.DefaultTransport},
		mkUrl: func(_ *auth.Credentials, domain string, key string) string {
			return storage.MakePublicURLv2(domain, key)
		},
	}

	config := &storage.Config{
		Zone:          &storage.ZoneHuadong,
		UseHTTPS:      false,
		UseCdnDomains: false,
	}
	if "" != cfg.Region {
		if zone, ok := storage.GetRegionByID(storage.RegionID(cfg.Region)); ok {
			config.Zone = &zone
		}
	}
	if strings.Index(cfg.Flag, "https") >= 0 {
		config.UseHTTPS = true
	}
	if strings.Index(cfg.Flag, "cdn") >= 0 {
		config.UseCdnDomains = true
	}
	if strings.Index(cfg.Flag, "private") >= 0 {
		q.mkUrl = func(mac *auth.Credentials, domain string, key string) string {
			return storage.MakePrivateURLv2(mac, domain, key, time.Now().Add(time.Hour).Unix())
		}
	}

	q.uploader = storage.NewFormUploaderEx(config, &storage.Client{Client: q.client})
	q.manager = storage.NewBucketManager(q.mac, config)

	return q, nil
}

func (q qiniuClient) PutObject(ctx context.Context, objectKey string, object io.ReadSeeker) error {
	if now := time.Now().Unix(); "" == q.token || (now-q.last) > 3600 {
		p := &storage.PutPolicy{Scope: q.bucket, Expires: 4000}
		q.token = p.UploadToken(q.mac)
		q.last = now
	}

	rd := object
	size := int64(0)

	switch r := object.(type) {
	case *bytes.Reader:
		size = r.Size()
	case *strings.Reader:
		size = r.Size()
	default:
		end, _ := object.Seek(0, io.SeekEnd)
		begin, _ := object.Seek(0, io.SeekStart)

		size = end - begin
	}

	return q.uploader.Put(ctx, nil, q.token, objectKey, rd, size, nil)
}

func (q qiniuClient) GetObject(ctx context.Context, objectKey string) (io.ReadCloser, error) {
	rawurl := q.mkUrl(q.mac, q.base, objectKey)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawurl, nil)
	if nil != err {
		return nil, err
	}
	req.Header.Set("User-Agent", "storage")

	resp, err := q.client.Do(req)
	if nil != err {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		err = resp.Body.Close()
		err = errors.Errorf("http status %d", resp.StatusCode)
		return nil, err
	}

	return resp.Body, nil
}

func (q qiniuClient) List(ctx context.Context, prefix string, delimiter string) (
	objects []chunk.StorageObject,
	prefixes []chunk.StorageCommonPrefix,
	err error) {
	host, err := q.manager.RsfReqHost(q.bucket)
	if err != nil {
		return
	}

	for marker, base := "", uriListFiles(host, q.bucket, prefix, delimiter); ; {
		var ret listFilesRet

		rawurl := base + url.QueryEscape(marker)
		if err = q.manager.Client.CredentialedCall(ctx, q.mac, auth.TokenQiniu, &ret, "POST", rawurl, nil); nil != err {
			break
		}
		for i := range ret.Items {
			objects = append(objects, chunk.StorageObject{
				Key:        ret.Items[i].Key,
				ModifiedAt: time.Unix(0, ret.Items[i].PutTime*100),
			})
		}
		for _, p := range ret.CommonPrefixes {
			prefixes = append(prefixes, chunk.StorageCommonPrefix(p))
		}
		if "" == ret.Marker {
			break
		}
		marker = ret.Marker
	}
	return
}

func (q qiniuClient) DeleteObject(ctx context.Context, objectKey string) error {
	host, err := q.manager.RsReqHost(q.bucket)
	if err != nil {
		return err
	}

	rawurl := strings.Join([]string{host, storage.URIDelete(q.bucket, objectKey)}, "")

	return q.manager.Client.CredentialedCall(ctx, q.mac, auth.TokenQiniu, nil, "POST", rawurl, nil)
}

func (q qiniuClient) Stop() {

}

func uriListFiles(host, bucket, prefix, delimiter string) string {
	query := make(url.Values)
	query.Add("bucket", bucket)
	if prefix != "" {
		query.Add("prefix", prefix)
	}
	if delimiter != "" {
		query.Add("delimiter", delimiter)
	}
	query.Add("limit", "1000")
	return fmt.Sprintf("%s/list?%s&marker=", host, query.Encode())
}
