package bosutil

import (
	"errors"
	"hash"
	"hash/crc32"
	"io"

	"github.com/baidubce/bce-sdk-go/bce"
	"github.com/baidubce/bce-sdk-go/services/bos"
	"github.com/baidubce/bce-sdk-go/services/bos/api"
	"github.com/baidubce/bce-sdk-go/util/log"
)

/*
type ProgressReader struct {
	reader   io.Reader
	total    int64
	readed   int64
	progress int64
	callback func(readed, total, progress int64)
}

func CreateProgressReader(reader io.Reader, size int64, callback func(readed, total, progress int64)) (*ProgressReader, error) {

	return &ProgressReader{reader: reader, total: size, readed: 0, callback: callback}, nil
}

func (p *ProgressReader) Read(b []byte) (n int, err error) {
	n, err = p.reader.Read(b)
	p.readed += int64(n)
	if p.total > 0 {
		progress := p.readed * 100 / p.total
		if progress > p.progress {
			p.progress = progress
			if p.callback != nil {
				p.callback(p.readed, p.total, p.progress)
			}
		}
	}
	// fmt.Printf("\r%d%%", p.readed*100/p.total)
	return n, err
}
func (p *ProgressReader) FileSize() int64 {
	return p.total
}
*/

var (
	EncryptFunction func(data []byte) []byte = nil
)

const (
	// DEFAULT_SERVICE_DOMAIN = bce.DEFAULT_REGION + ".bcebos.com"
	DEFAULT_MAX_PARALLEL   = 10
	MULTIPART_ALIGN        = 1 << 20         // 1MB
	MIN_MULTIPART_SIZE     = 100 * (1 << 10) // 100 KB
	DEFAULT_MULTIPART_SIZE = 12 * (1 << 20)  // 12MB

	MAX_PART_NUMBER        = 10000
	MAX_SINGLE_PART_SIZE   = 5 * (1 << 30)    // 5GB
	MAX_SINGLE_OBJECT_SIZE = 48.8 * (1 << 40) // 48.8TB
)

type SimpleUploader struct {
	client   *bos.Client
	fileSize int64
	// md5Hasher hash.Hash
	crc32Hasher hash.Hash32
	//FileName  string
	// MD5Bytes []byte
	reader io.Reader
}

func CreateSimpleUploader(c *bos.Client, reader io.Reader, fileSize int64) *SimpleUploader {
	return &SimpleUploader{
		client:      c,
		fileSize:    fileSize,
		crc32Hasher: crc32.NewIEEE(),
		reader:      reader,
	}
}

// ParallelUpload - auto multipart upload object
//
// PARAMS:
//   - bucket: the bucket name
//   - object: the object name
//   - filename: the filename
//   - contentType: the content type default(application/octet-stream)
//   - args: the bucket name nil using default
//
// RETURNS:
//   - *api.CompleteMultipartUploadResult: multipart upload result
//   - error: nil if success otherwise the specific error
func (uploader *SimpleUploader) SimplelUpload(bucket string, object string, contentType string, args *api.InitiateMultipartUploadArgs) (*api.CompleteMultipartUploadResult, error) {

	c := uploader.client
	initiateMultipartUploadResult, err := api.InitiateMultipartUpload(c, bucket, object, contentType, args)
	if err != nil {
		return nil, err
	}

	partEtags, err := uploader.simplePartUpload(bucket, object, initiateMultipartUploadResult.UploadId)
	if err != nil {
		c.AbortMultipartUpload(bucket, object, initiateMultipartUploadResult.UploadId)
		return nil, err
	}

	completeMultipartUploadResult, err := c.CompleteMultipartUploadFromStruct(bucket, object, initiateMultipartUploadResult.UploadId, &api.CompleteMultipartUploadArgs{Parts: partEtags})
	if err != nil {
		c.AbortMultipartUpload(bucket, object, initiateMultipartUploadResult.UploadId)
		return nil, err
	}
	return completeMultipartUploadResult, nil
}

// simplePartUpload - single part upload
//
// PARAMS:
//   - bucket: the bucket name
//   - object: the object name
//   - filename: the uploadId
//   - uploadId: the uploadId
//
// RETURNS:
//   - []api.UploadInfoType: multipart upload result
//   - error: nil if success otherwise the specific error
func (uploader *SimpleUploader) simplePartUpload(bucket string, object string, uploadId string) ([]api.UploadInfoType, error) {

	// 分块大小按MULTIPART_ALIGN=1MB对齐
	c := uploader.client
	partSize := (c.MultipartSize +
		MULTIPART_ALIGN - 1) / MULTIPART_ALIGN * MULTIPART_ALIGN

	// 获取文件大小，并计算分块数目，最大分块数MAX_PART_NUMBER=10000
	fileSize := uploader.fileSize
	uploader.fileSize = fileSize
	partNum := (fileSize + partSize - 1) / partSize
	if partNum > MAX_PART_NUMBER { // 超过最大分块数，需调整分块大小
		partSize = (fileSize + MAX_PART_NUMBER + 1) / MAX_PART_NUMBER
		partSize = (partSize + MULTIPART_ALIGN - 1) / MULTIPART_ALIGN * MULTIPART_ALIGN
		partNum = (fileSize + partSize - 1) / partSize
	}

	// 逐个分块上传
	partEtags := make([]api.UploadInfoType, partNum)
	for i := int64(1); i <= partNum; i++ {
		// 计算偏移offset和本次上传的大小uploadSize
		uploadSize := partSize
		offset := partSize * (i - 1)
		left := fileSize - offset
		if left < partSize {
			uploadSize = left
		}

		// 创建指定偏移、指定大小的文件流

		buffer := make([]byte, uploadSize)
		read, err := uploader.reader.Read(buffer)
		if err != nil {
			return nil, err
		}
		if read != int(uploadSize) {
			buffer = buffer[:read]
		}
		if EncryptFunction != nil {
			buffer = EncryptFunction(buffer)
		}
		partBody, _ := bce.NewBodyFromBytes(buffer)
		uploader.crc32Hasher.Write(buffer)
		result, err := uploader.simpleSinglePartUpload(bucket, object, uploadId, int(i), partBody)
		if err != nil {
			return nil, err
		}
		if err == nil && result == nil {
			return nil, errors.New("upload panic")
		}
		partEtags[result.PartNumber-1].PartNumber = result.PartNumber
		partEtags[result.PartNumber-1].ETag = result.ETag
	}

	return partEtags, nil
}

// singlePartUpload - single part upload
//
// PARAMS:
//   - pararelChan: the pararelChan
//   - errChan: the error chan
//   - result: the upload result chan
//   - bucket: the bucket name
//   - object: the object name
//   - uploadId: the uploadId
//   - partNumber: the part number of the object
//   - content: the content of current part
func (uploader *SimpleUploader) simpleSinglePartUpload(bucket string, object string, uploadId string,
	partNumber int, content *bce.Body) (*api.UploadInfoType, error) {

	defer func() {
		if r := recover(); r != nil {
			log.Fatal("parallelPartUpload recovered in f:", r)
		}
	}()

	var args api.UploadPartArgs
	args.ContentMD5 = content.ContentMD5()

	etag, err := api.UploadPart(uploader.client, bucket, object, uploadId, partNumber, content, &args)
	if err != nil {
		log.Error("upload part fail,err:%v", err)
		return nil, err
	}
	return &api.UploadInfoType{PartNumber: partNumber, ETag: etag}, nil
}

func (uploader *SimpleUploader) SumCrc32() uint32 {
	return uploader.crc32Hasher.Sum32()
}

func (uploader *SimpleUploader) FileSize() int64 {
	return uploader.fileSize
}

func (uploader *SimpleUploader) ParallelUpload(bucket string, object string, contentType string, args *api.InitiateMultipartUploadArgs) (*api.CompleteMultipartUploadResult, error) {
	c := uploader.client
	size := uploader.fileSize
	if size < MIN_MULTIPART_SIZE || c.MultipartSize < MIN_MULTIPART_SIZE {
		// do simple upload
		buffer, err := io.ReadAll(uploader.reader)
		if err != nil {
			return nil, err
		}
		if EncryptFunction != nil {
			buffer = EncryptFunction(buffer)
		}
		body, _ := bce.NewBodyFromBytes(buffer)
		uploader.crc32Hasher.Write(buffer)
		hash, err := c.PutObject(bucket, object, body, nil)
		if err != nil {
			return nil, err
		}
		return &api.CompleteMultipartUploadResult{ETag: hash, Bucket: bucket, Key: object}, nil
		// return bce.NewBceClientError("multipart size should not be less than 1MB")
	}

	initiateMultipartUploadResult, err := api.InitiateMultipartUpload(c, bucket, object, contentType, args)
	if err != nil {
		return nil, err
	}

	partEtags, err := uploader.parallelPartUpload(bucket, object, initiateMultipartUploadResult.UploadId)
	if err != nil {
		c.AbortMultipartUpload(bucket, object, initiateMultipartUploadResult.UploadId)
		return nil, err
	}

	completeMultipartUploadResult, err := c.CompleteMultipartUploadFromStruct(bucket, object, initiateMultipartUploadResult.UploadId, &api.CompleteMultipartUploadArgs{Parts: partEtags})
	if err != nil {
		c.AbortMultipartUpload(bucket, object, initiateMultipartUploadResult.UploadId)
		return nil, err
	}
	return completeMultipartUploadResult, nil
}

// parallelPartUpload - single part upload
//
// PARAMS:
//   - bucket: the bucket name
//   - object: the object name
//   - filename: the uploadId
//   - uploadId: the uploadId
//
// RETURNS:
//   - []api.UploadInfoType: multipart upload result
//   - error: nil if success otherwise the specific error
func (uploader *SimpleUploader) parallelPartUpload(bucket string, object string, uploadId string) ([]api.UploadInfoType, error) {
	c := uploader.client

	// 分块大小按MULTIPART_ALIGN=1MB对齐
	partSize := (c.MultipartSize +
		MULTIPART_ALIGN - 1) / MULTIPART_ALIGN * MULTIPART_ALIGN

	// 获取文件大小，并计算分块数目，最大分块数MAX_PART_NUMBER=10000
	fileSize := uploader.fileSize
	partNum := (fileSize + partSize - 1) / partSize
	if partNum > MAX_PART_NUMBER { // 超过最大分块数，需调整分块大小
		partSize = (fileSize + MAX_PART_NUMBER + 1) / MAX_PART_NUMBER
		partSize = (partSize + MULTIPART_ALIGN - 1) / MULTIPART_ALIGN * MULTIPART_ALIGN
		partNum = (fileSize + partSize - 1) / partSize
	}

	parallelChan := make(chan int, c.MaxParallel)

	errChan := make(chan error, c.MaxParallel)

	resultChan := make(chan api.UploadInfoType, partNum)

	// 逐个分块上传
	for i := int64(1); i <= partNum; i++ {
		// 计算偏移offset和本次上传的大小uploadSize
		uploadSize := partSize
		offset := partSize * (i - 1)
		left := fileSize - offset
		if left < partSize {
			uploadSize = left
		}

		// 创建指定偏移、指定大小的文件流
		buffer := make([]byte, uploadSize)
		read, err := uploader.reader.Read(buffer)
		if err != nil {
			return nil, err
		}
		if read != int(uploadSize) {
			buffer = buffer[:read]
		}
		if EncryptFunction != nil {
			buffer = EncryptFunction(buffer)
		}
		partBody, _ := bce.NewBodyFromBytes(buffer)
		uploader.crc32Hasher.Write(buffer)

		select {
		case err = <-errChan:
			return nil, err
		default:
			select {
			case err = <-errChan:
				return nil, err
			case parallelChan <- 1:
				go uploader.singlePartUpload(bucket, object, uploadId, int(i), partBody, parallelChan, errChan, resultChan)
			}

		}
	}

	partEtags := make([]api.UploadInfoType, partNum)
	for i := int64(0); i < partNum; i++ {
		select {
		case err := <-errChan:
			return nil, err
		case result := <-resultChan:
			partEtags[result.PartNumber-1].PartNumber = result.PartNumber
			partEtags[result.PartNumber-1].ETag = result.ETag
		}
	}
	return partEtags, nil
}

// singlePartUpload - single part upload
//
// PARAMS:
//   - pararelChan: the pararelChan
//   - errChan: the error chan
//   - result: the upload result chan
//   - bucket: the bucket name
//   - object: the object name
//   - uploadId: the uploadId
//   - partNumber: the part number of the object
//   - content: the content of current part
func (uploader *SimpleUploader) singlePartUpload(
	bucket string, object string, uploadId string,
	partNumber int, content *bce.Body,
	parallelChan chan int, errChan chan error, result chan api.UploadInfoType) {

	defer func() {
		if r := recover(); r != nil {
			log.Fatal("parallelPartUpload recovered in f:", r)
			errChan <- errors.New("parallelPartUpload panic")
		}
		<-parallelChan
	}()

	var args api.UploadPartArgs
	args.ContentMD5 = content.ContentMD5()

	etag, err := api.UploadPart(uploader.client, bucket, object, uploadId, partNumber, content, &args)
	if err != nil {
		errChan <- err
		log.Error("upload part fail,err:%v", err)
		return
	}
	result <- api.UploadInfoType{PartNumber: partNumber, ETag: etag}
}
