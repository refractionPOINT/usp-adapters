package usp_sqs_files

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/sqs"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	defaultWriteTimeout = 60 * 10
)

type SQSFilesAdapter struct {
	conf      SQSFilesConfig
	uspClient *uspclient.Client

	chFiles chan fileInfo

	// SQS
	awsConfig  *aws.Config
	awsSession *session.Session
	sqsClient  *sqs.SQS

	// S3
	isS3Inited    bool
	awsS3Config   *aws.Config
	awsS3Session  *session.Session
	awsS3         *s3.S3
	awsDownloader *s3manager.Downloader

	ctx    context.Context
	isStop bool
	wg     sync.WaitGroup
}

type SQSFilesConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`

	// SQS specific
	AccessKey string `json:"access_key" yaml:"access_key"`
	SecretKey string `json:"secret_key,omitempty" yaml:"secret_key,omitempty"`
	QueueURL  string `json:"queue_url" yaml:"queue_url"`
	Region    string `json:"region" yaml:"region"`

	// S3 specific
	ParallelFetch     int    `json:"parallel_fetch" yaml:"parallel_fetch"`
	BucketPath        string `json:"bucket_path,omitempty" yaml:"bucket_path,omitempty"`
	FilePath          string `json:"file_path,omitempty" yaml:"file_path,omitempty"`
	IsDecodeObjectKey bool   `json:"is_decode_object_key,omitempty" yaml:"is_decode_object_key,omitempty"`
	// Optional: alternative to BucketPath
	Bucket string `json:"bucket,omitempty" yaml:"bucket,omitempty"`

	// CEF Parsing (e.g., Imperva WAF logs)
	IsCEFFormat bool `json:"is_cef_format,omitempty" yaml:"is_cef_format,omitempty"`
}

type fileInfo struct {
	bucket string
	path   string
}

func (c *SQSFilesConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.AccessKey == "" {
		return errors.New("missing access_key")
	}
	if c.SecretKey == "" {
		return errors.New("missing secret_key")
	}
	if c.Region == "" {
		return errors.New("missing region")
	}
	if c.QueueURL == "" {
		return errors.New("missing queue_url")
	}
	return nil
}

func NewSQSFilesAdapter(ctx context.Context, conf SQSFilesConfig) (*SQSFilesAdapter, chan struct{}, error) {
	if conf.ParallelFetch <= 0 {
		conf.ParallelFetch = 1
	}
	if conf.BucketPath == "" {
		conf.BucketPath = "bucket"
	}
	if conf.FilePath == "" {
		conf.FilePath = "files/path"
	}

	a := &SQSFilesAdapter{
		conf: conf,
		ctx:  context.Background(),
	}

	var err error

	// SQS
	a.awsConfig = &aws.Config{
		Region:      aws.String(conf.Region),
		Credentials: credentials.NewStaticCredentials(conf.AccessKey, conf.SecretKey, ""),
	}

	if a.awsSession, err = session.NewSession(a.awsConfig); err != nil {
		return nil, nil, err
	}

	a.sqsClient = sqs.New(a.awsSession)

	// The S3 SDK will be initialized at run-time
	// once we get the first file from an SQS event.
	a.isS3Inited = false

	a.chFiles = make(chan fileInfo)

	a.uspClient, err = uspclient.NewClient(ctx, conf.ClientOptions)
	if err != nil {
		return nil, nil, err
	}

	// Start the processors.
	for i := 0; i < a.conf.ParallelFetch; i++ {
		a.wg.Add(1)
		go func() {
			defer a.wg.Done()
			a.processFiles()
		}()
	}

	var subErr error
	chStopped := make(chan struct{})
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		defer close(chStopped)
		subErr = a.receiveEvents()
	}()
	// Give it a second to start the subscriber to check it's
	// working without any errors.
	time.Sleep(2 * time.Second)
	if subErr != nil {
		a.uspClient.Close()
		return nil, nil, subErr
	}

	return a, chStopped, nil
}

func (a *SQSFilesAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing")
	a.isStop = true
	a.wg.Wait()
	if _, err := a.uspClient.Close(); err != nil {
		return err
	}
	return nil
}

func (a *SQSFilesAdapter) initS3SDKs(bucket string) error {
	if a.isS3Inited {
		return nil
	}
	region, err := a.getBucketRegion(bucket)
	if err != nil {
		return fmt.Errorf("s3.Region: %v", err)
	}
	a.awsS3Config = &aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials(a.conf.AccessKey, a.conf.SecretKey, ""),
	}

	if a.awsS3Session, err = session.NewSession(a.awsS3Config); err != nil {
		return fmt.Errorf("s3.NewSession(): %v", err)
	}

	a.awsS3 = s3.New(a.awsSession)
	a.awsDownloader = s3manager.NewDownloader(a.awsS3Session)
	return nil
}

func (a *SQSFilesAdapter) getBucketRegion(bucket string) (string, error) {
	return s3manager.GetBucketRegion(a.ctx, session.Must(session.NewSession(&aws.Config{})), bucket, "us-east-1")
}

func (a *SQSFilesAdapter) receiveEvents() error {
	defer close(a.chFiles)

	for !a.isStop {
		result, err := a.sqsClient.ReceiveMessage(&sqs.ReceiveMessageInput{
			AttributeNames:        []*string{},
			MessageAttributeNames: []*string{},
			QueueUrl:              &a.conf.QueueURL,
			MaxNumberOfMessages:   aws.Int64(10),
			VisibilityTimeout:     aws.Int64(60), // 60 seconds
			WaitTimeSeconds:       aws.Int64(5),
		})
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("sqsClient.ReceiveMessage: %v", err))
			return err
		}
		delRequest := &sqs.DeleteMessageBatchInput{
			Entries:  make([]*sqs.DeleteMessageBatchRequestEntry, 0, len(result.Messages)),
			QueueUrl: &a.conf.QueueURL,
		}
		if len(result.Messages) == 0 {
			continue
		}
		for _, msg := range result.Messages {
			delRequest.Entries = append(delRequest.Entries, &sqs.DeleteMessageBatchRequestEntry{
				Id:            msg.MessageId,
				ReceiptHandle: msg.ReceiptHandle,
			})

			d := utils.Dict{}
			if err := json.Unmarshal([]byte(*msg.Body), &d); err != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("sqsClient.Message.Json: %v", err))
				continue
			}

			bucket := a.conf.Bucket
			if bucket == "" {
				bucket = d.ExpandableFindOneString(a.conf.BucketPath)
			}
			filePaths := d.ExpandableFindString(a.conf.FilePath)

			if bucket == "" {
				a.conf.ClientOptions.OnError(errors.New("sqsClient.Message: missing bucket"))
				continue
			}
			if len(filePaths) == 0 {
				continue
			}
			if err := a.initS3SDKs(bucket); err != nil {
				a.conf.ClientOptions.OnError(err)
				return err
			}

			for _, p := range filePaths {
				a.chFiles <- fileInfo{
					bucket: bucket,
					path:   p,
				}
			}

		}
		delRes, err := a.sqsClient.DeleteMessageBatch(delRequest)
		if err != nil {
			return err
		}
		if len(delRes.Failed) != 0 {
			return errors.New("sqsClient.DeleteMessageBatch: failed to delete some messages")
		}
	}
	return nil
}

func (a *SQSFilesAdapter) processFiles() error {
	for f := range a.chFiles {
		path := f.path
		if a.conf.IsDecodeObjectKey {
			// URL Decode the path
			var err error
			path, err = url.QueryUnescape(path)
			if err != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("url.QueryUnescape(): %v", err))
				continue
			}
		}
		startTime := time.Now().UTC()
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("downloading file %s", path))

		writerAt := aws.NewWriteAtBuffer([]byte{})

		if _, err := a.awsDownloader.Download(writerAt, &s3.GetObjectInput{
			Bucket: aws.String(f.bucket),
			Key:    aws.String(path),
		}); err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("s3.Download(): %v", err))
			return err
		}
		isCompressed := false

		if strings.HasSuffix(path, ".gz") {
			isCompressed = true
		}

		a.conf.ClientOptions.DebugLog(fmt.Sprintf("file %s downloaded in %v", path, time.Since(startTime)))

		a.processEvent(writerAt.Bytes(), isCompressed)
	}
	return nil
}

func (a *SQSFilesAdapter) processEvent(data []byte, isCompressed bool) bool {
	// If CEF format is enabled, parse lines individually
	if a.conf.IsCEFFormat {
		return a.processCEFEvent(data, isCompressed)
	}

	// Since we're dealing with files, we use the
	// bundle payloads to avoid having to go through
	// the whole unmarshal+marshal roundtrip.
	var msg *protocol.DataMessage
	if isCompressed {
		msg = &protocol.DataMessage{
			CompressedBundlePayload: data,
			TimestampMs:             uint64(time.Now().UnixNano() / int64(time.Millisecond)),
		}
	} else {
		msg = &protocol.DataMessage{
			BundlePayload: data,
			TimestampMs:   uint64(time.Now().UnixNano() / int64(time.Millisecond)),
		}
	}

	if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
		if err == uspclient.ErrorBufferFull {
			a.conf.ClientOptions.OnWarning("stream falling behind")
			err = a.uspClient.Ship(msg, 1*time.Hour)
		}
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
			return false
		}
	}
	return true
}

func (a *SQSFilesAdapter) processCEFEvent(data []byte, isCompressed bool) bool {
	var reader io.Reader = bytes.NewReader(data)
	if isCompressed {
		gzReader, err := gzip.NewReader(reader)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("gzip.NewReader(): %v", err))
			return false
		}
		defer gzReader.Close()
		reader = gzReader
	}

	scanner := bufio.NewScanner(reader)
	// Increase buffer size for potentially long CEF lines
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		parsed, err := parseCEF(line)
		if err != nil {
			a.conf.ClientOptions.OnWarning(fmt.Sprintf("CEF parse error: %v", err))
			// Ship as raw text if parsing fails
			msg := &protocol.DataMessage{
				TextPayload: line,
				TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
			}
			if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
				if err == uspclient.ErrorBufferFull {
					a.conf.ClientOptions.OnWarning("stream falling behind")
					err = a.uspClient.Ship(msg, 1*time.Hour)
				}
				if err != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
					return false
				}
			}
			continue
		}

		msg := &protocol.DataMessage{
			JsonPayload: parsed,
			TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
		}
		if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
			if err == uspclient.ErrorBufferFull {
				a.conf.ClientOptions.OnWarning("stream falling behind")
				err = a.uspClient.Ship(msg, 1*time.Hour)
			}
			if err != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
				return false
			}
		}
	}

	if err := scanner.Err(); err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("scanner error: %v", err))
		return false
	}

	return true
}

// parseCEF parses a CEF (Common Event Format) log line into a map.
// CEF format: CEF:Version|Device Vendor|Device Product|Device Version|Device Event Class ID|Name|Severity|Extension
func parseCEF(line string) (utils.Dict, error) {
	// Check for CEF prefix
	if !strings.HasPrefix(line, "CEF:") {
		return nil, fmt.Errorf("not a CEF log line")
	}

	result := utils.Dict{}

	// Remove CEF: prefix
	line = line[4:]

	// Split header fields (first 7 pipe-separated fields)
	// We need to handle escaped pipes (\|) in the header
	headerFields := splitCEFHeader(line)
	if len(headerFields) < 7 {
		return nil, fmt.Errorf("invalid CEF format: expected at least 7 header fields, got %d", len(headerFields))
	}

	// Parse header fields
	result["cef_version"] = headerFields[0]
	result["device_vendor"] = unescapeCEFHeader(headerFields[1])
	result["device_product"] = unescapeCEFHeader(headerFields[2])
	result["device_version"] = unescapeCEFHeader(headerFields[3])
	result["device_event_class_id"] = unescapeCEFHeader(headerFields[4])
	result["name"] = unescapeCEFHeader(headerFields[5])
	result["severity"] = headerFields[6]

	// Parse extension (key=value pairs after the 7th pipe)
	if len(headerFields) > 7 {
		extension := headerFields[7]
		extFields := parseCEFExtension(extension)
		for k, v := range extFields {
			result[k] = v
		}
	}

	return result, nil
}

// splitCEFHeader splits the CEF line by pipe, respecting escaped pipes
func splitCEFHeader(line string) []string {
	var fields []string
	var current strings.Builder
	escaped := false
	fieldCount := 0

	for i := 0; i < len(line); i++ {
		ch := line[i]

		if escaped {
			current.WriteByte(ch)
			escaped = false
			continue
		}

		if ch == '\\' {
			// Check if next char is a pipe or backslash
			if i+1 < len(line) && (line[i+1] == '|' || line[i+1] == '\\') {
				escaped = true
				current.WriteByte(ch)
				continue
			}
			current.WriteByte(ch)
			continue
		}

		if ch == '|' {
			fields = append(fields, current.String())
			current.Reset()
			fieldCount++
			// After 7 fields (header), the rest is extension
			if fieldCount >= 7 {
				if i+1 < len(line) {
					fields = append(fields, line[i+1:])
				}
				break
			}
			continue
		}

		current.WriteByte(ch)
	}

	// Add last field if we didn't reach 7 pipes
	if fieldCount < 7 {
		fields = append(fields, current.String())
	}

	return fields
}

// unescapeCEFHeader unescapes CEF header field values
func unescapeCEFHeader(s string) string {
	s = strings.ReplaceAll(s, "\\|", "|")
	s = strings.ReplaceAll(s, "\\\\", "\\")
	return s
}

// parseCEFExtension parses the extension portion of a CEF log (key=value pairs)
func parseCEFExtension(extension string) map[string]string {
	result := make(map[string]string)
	if extension == "" {
		return result
	}

	// CEF extension format: key1=value1 key2=value2
	// Values can contain spaces, so we need to find the next key=
	// Keys are alphanumeric (and may contain underscores per some implementations)

	var currentKey string
	var currentValue strings.Builder
	inValue := false
	i := 0

	for i < len(extension) {
		// Try to find a key
		if !inValue {
			// Skip leading spaces
			for i < len(extension) && extension[i] == ' ' {
				i++
			}
			if i >= len(extension) {
				break
			}

			// Find the key (up to =)
			keyStart := i
			for i < len(extension) && extension[i] != '=' && extension[i] != ' ' {
				i++
			}
			if i >= len(extension) || extension[i] != '=' {
				// No valid key found, skip
				break
			}
			currentKey = extension[keyStart:i]
			i++ // skip '='
			inValue = true
			currentValue.Reset()
			continue
		}

		// We're in a value - find where it ends
		// Value ends when we find " key=" pattern (space followed by valid key and equals)
		// Or at end of string
		valueStart := i
		for i < len(extension) {
			// Check if this might be the start of a new key
			if extension[i] == ' ' {
				// Look ahead for potential key=
				j := i + 1
				// Skip spaces
				for j < len(extension) && extension[j] == ' ' {
					j++
				}
				// Check if next token looks like a key (alphanumeric/underscore followed by =)
				keyEnd := j
				for keyEnd < len(extension) && isValidCEFKeyChar(extension[keyEnd]) {
					keyEnd++
				}
				if keyEnd > j && keyEnd < len(extension) && extension[keyEnd] == '=' {
					// Found next key, current value ends here
					currentValue.WriteString(extension[valueStart:i])
					break
				}
			}
			i++
		}

		// If we reached end of string
		if i >= len(extension) {
			currentValue.WriteString(extension[valueStart:])
		}

		// Save the key-value pair
		value := unescapeCEFExtension(currentValue.String())
		result[currentKey] = value
		inValue = false
	}

	return result
}

// isValidCEFKeyChar returns true if the character is valid in a CEF extension key
func isValidCEFKeyChar(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') ||
		(ch >= 'A' && ch <= 'Z') ||
		(ch >= '0' && ch <= '9') ||
		ch == '_'
}

// unescapeCEFExtension unescapes CEF extension field values
func unescapeCEFExtension(s string) string {
	s = strings.ReplaceAll(s, "\\=", "=")
	s = strings.ReplaceAll(s, "\\n", "\n")
	s = strings.ReplaceAll(s, "\\r", "\r")
	s = strings.ReplaceAll(s, "\\\\", "\\")
	return s
}
