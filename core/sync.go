package core

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/pkg/errors"

	"github.com/containers/image/v5/copy"
	"github.com/containers/image/v5/docker"
	"github.com/containers/image/v5/signature"
	"github.com/containers/image/v5/types"
	"github.com/panjf2000/ants/v2"
	log "github.com/sirupsen/logrus"
)

func Run(opt *SyncOption) {
	// 设置默认值
	opt = opt.setDefault()
	// 关闭键值对数据库文件
	defer func() {
		_ = opt.Closer()
	}()

	// 系统进程信号管道
	Sigs := make(chan os.Signal)

	// 关闭上下文的管道
	var cancel context.CancelFunc
	opt.Ctx, cancel = context.WithCancel(context.Background())
	if opt.CmdTimeout > 0 {
		// 超时管道
		opt.Ctx, cancel = context.WithTimeout(opt.Ctx, opt.CmdTimeout)
	}

	var cancelOnce sync.Once
	defer cancel()

	// 创建 goroutines 接收系统信号,并在接收到系统信号后仅完成一次动作
	go func() {
		for range Sigs {
			// 仅执行一次的动作
			cancelOnce.Do(func() {
				log.Info("Receiving a termination signal, gracefully shutdown!")
				cancel()
			})
			log.Info("The goroutines pool has stopped, please wait for the remaining tasks to complete.")
		}
	}()
	// 仅接收 SIGINT,SIGTERM 系统信号
	signal.Notify(Sigs, syscall.SIGINT, syscall.SIGTERM)

	// 创建 "k8s.gcr.io" 键值对集合
	if err := opt.CheckSumer.CreatBucket("k8s.gcr.io"); err != nil {
		log.Error(err)
	}

	if opt.LiveInterval > 0 {
		if opt.LiveInterval >= 10*time.Minute { //travis-ci 10分钟没任何输出就会被强制关闭
			opt.LiveInterval = 9 * time.Minute
		}
		go func() {
		    // for-select 实现超时机制,10 分钟没任何输出就会被强制关闭
			for {
				select {
				case <-opt.Ctx.Done():
					return
				case <-time.After(opt.LiveInterval):
					log.Info("Live output for live in ci runner")
				}
			}
		}()
	}
	//
	//for _, ns := range namespace {
	//	g.Sync(ns)
	//}
	// 开始同步
	if err := Sync(opt); err != nil {
		log.Fatal(err)
	}
}

func Sync(opt *SyncOption) error {
    // k8s.gcr.io 仓库下所有的镜像名称 ${imgName}:${tag} 的列表
	allImages, err := ImageNames(opt)
	if err != nil {
		return err
	}
    // 同步
	imgs := SyncImages(allImages, opt)
	log.Info("sync done")
	// 同步完成后,对镜像列表进行汇总报告
	report(imgs)
	return nil
}

// 同步镜像
func SyncImages(imgs Images, opt *SyncOption) Images {

	processWg := new(sync.WaitGroup)
	processWg.Add(len(imgs))

	if opt.Limit == 0 {
		opt.Limit = DefaultLimit
	}

	// 创建协程池
	pool, err := ants.NewPool(opt.Limit, ants.WithPreAlloc(true), ants.WithPanicHandler(func(i interface{}) {
		log.Error(i)
	}))

	if err != nil {
		log.Fatalf("failed to create goroutines pool: %s", err)
	}
	// Images 实现了"sort.Interface" 接口,可对内部元素进行排序
	sort.Sort(imgs)
	for i := 0; i < len(imgs); i++ {
		k := i
		err = pool.Submit(func() {
			defer processWg.Done()

			select {
			case <-opt.Ctx.Done():
			default:
				log.Debug("process image: ", imgs[k].String())
				// 判断是否需要同步,并返回未同步镜像的 CheckSum
				newSum, needSync := checkSync(imgs[k], opt)
				if !needSync {
					return
				}

				rerr := retry(opt.Retry, opt.RetryInterval, func() error {
					// 同步镜像到指定的 DockerHub
					return sync2DockerHub(imgs[k], opt)
				})
				if rerr != nil {
					imgs[k].Err = rerr
					log.Errorf("failed to process image %s, error: %s", imgs[k].String(), rerr)
					return
				}
				imgs[k].Success = true

				// 将同步镜像的 CheckSum 写入键值对数据库
				if sErr := opt.CheckSumer.Save(imgs[k].Key(), newSum); sErr != nil {
					log.Errorf("failed to save image [%s] checksum: %s", imgs[k].String(), sErr)
				}
				log.Debugf("save image [%s] checksum: %d", imgs[k].String(), newSum)
			}
		})
		if err != nil {
			log.Fatalf("failed to submit task: %s", err)
		}
	}
	processWg.Wait()
	pool.Release()
	return imgs
}

// 将镜像同步到指定的 DockerHub
func sync2DockerHub(image *Image, opt *SyncOption) error {
	// 源镜像名称
	srcImg := image.String()
	// 同步的镜像名称,
	destImg := fmt.Sprintf("%s/%s/%s:%s", opt.PushRepo, opt.PushNS, image.Name, image.Tag)

	log.Infof("syncing %s => %s", srcImg, destImg)

	ctx, cancel := context.WithTimeout(opt.Ctx, opt.SingleTimeout)
	defer cancel()

	policyContext, err := signature.NewPolicyContext(
		&signature.Policy{
			Default: []signature.PolicyRequirement{signature.NewPRInsecureAcceptAnything()},
		},
	)

	if err != nil {
		return err
	}
	defer func() { _ = policyContext.Destroy() }()

	srcRef, err := docker.ParseReference("//" + srcImg)
	if err != nil {
		return err
	}
	destRef, err := docker.ParseReference("//" + destImg)
	if err != nil {
		return err
	}

	sourceCtx := &types.SystemContext{DockerAuthConfig: &types.DockerAuthConfig{}}
	destinationCtx := &types.SystemContext{DockerAuthConfig: &types.DockerAuthConfig{
		Username: opt.Auth.User,
		Password: opt.Auth.Pass,
	}}

	log.Debugf("copy %s to %s ...", image.String(), opt.PushRepo)
	// 进行同步,
	_, err = copy.Image(ctx, policyContext, destRef, srcRef, &copy.Options{
		SourceCtx:          sourceCtx,
		DestinationCtx:     destinationCtx,
		ImageListSelection: copy.CopyAllImages,
	})
	log.Debugf("%s copy done, error is %v.", srcImg, err)
	return err
}

// 判断是否已经同步,并返回未同步镜像的 CheckSum
func checkSync(image *Image, opt *SyncOption) (uint32, bool) {
	var (
		bodySum uint32
		diff    bool
	)
	imgFullName := image.String()
	err := retry(opt.Retry, opt.RetryInterval, func() error {
		var mErr error
		// 获取指定镜像的 CheckSum
		bodySum, mErr = GetManifestBodyCheckSum(imgFullName)
		if mErr != nil {
			return mErr
		}
		if bodySum == 0 {
			return errors.New("checkSum is 0, maybe resp body is nil")
		}
		return nil
	})

	if err != nil {
		image.Err = err
		log.Errorf("failed to get image [%s] manifest, error: %s", imgFullName, err)
		return 0, false
	}

	// db 查询校验值是否相等.如果不相等,返回 true
	// 因为只同步一个ns下的镜像，所以 bucket 的 key 只用 baseName:tag
	diff, err = opt.CheckSumer.Diff(image.Key(), bodySum)
	if err != nil {
		image.Err = err
		log.Errorf("failed to get image [%s] checkSum, error: %s", imgFullName, err)
		return 0, false
	}

	log.Debugf("%s diff:%v", imgFullName, diff)

	// 如果相同,则 image 结构体的 Success,CacheHit 为 true.用于统计同步了多少镜像
	if !diff {
		image.Success = true
		image.CacheHit = true
		log.Debugf("image [%s] not changed, skip sync...", imgFullName)
		return 0, false
	}
	// 否则,返回远程镜像的 CheckSum
	return bodySum, true
}

// 汇总统计,成功,缓存命中,失败的镜像个数,并进行输出
func report(images Images) {

	var successCount, failedCount, cacheHitCount int
	var report string

	for _, img := range images {
		if img.Success {
			successCount++
			if img.CacheHit {
				cacheHitCount++
			}
		} else {
			failedCount++
		}
	}
	report = fmt.Sprintf(`========================================
>> Sync Repo: k8s.gcr.io
>> Sync Total: %d
>> Sync Failed: %d
>> Sync Success: %d
>> CacheHit: %d`, len(images), failedCount, successCount, cacheHitCount)
	fmt.Println(report)
}
