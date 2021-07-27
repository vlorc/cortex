package qiniu

// Config for a StorageClient
type Config struct {
	Url    string `yaml:"url,omitempty"`
	Access string `yaml:"access,omitempty"`
	Secret string `yaml:"secret,omitempty"`
	Bucket string `flag:"bucket,omitempty"`
	Region string `flag:"region,omitempty"`
	Flag   string `flag:"flag,omitempty"`
}
